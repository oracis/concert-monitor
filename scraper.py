"""
江苏演唱会·集 - 数据抓取模块
数据源：票牛网（piaoniu.com）+ 有票网（ypiao.com）
策略：两个网站都是 SSR 渲染，直接用 requests 解析 HTML
"""

import json
import os
import time
import re
import logging
from datetime import datetime
from typing import List, Dict

import requests

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# 江苏城市列表
JIANGSU_CITIES = ["南京", "苏州", "无锡", "常州", "南通", "徐州", "扬州", "盐城", "泰州", "镇江", "淮安", "连云港", "宿迁"]
JIANGSU_NAMES = set(JIANGSU_CITIES)

# 演唱会关键词
CONCERT_KEYWORDS = ["演唱会", "巡演", "音乐节", "TOUR", "Live", "CONCERT"]

# 通用请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}

# 票牛网城市 URL 映射
PIAONIU_CITIES = {
    "南京": "https://www.piaoniu.com/nanjing",
    "苏州": "https://www.piaoniu.com/suzhou",
    "无锡": "https://www.piaoniu.com/wuxi",
    "常州": "https://www.piaoniu.com/changzhou",
    "南通": "https://www.piaoniu.com/nantong",
    "徐州": "https://www.piaoniu.com/xuzhou",
    "扬州": "https://www.piaoniu.com/yangzhou",
    "盐城": "https://www.piaoniu.com/yancheng",
    "泰州": "https://www.piaoniu.com/taizhou",
    "镇江": "https://www.piaoniu.com/zhenjiang",
    "淮安": "https://www.piaoniu.com/huaian",
    "连云港": "https://www.piaoniu.com/lianyungang",
    "宿迁": "https://www.piaoniu.com/suqian",
}

# 有票网 URL（全国演唱会页面，从中筛选江苏城市数据）
YPIAO_URL = "https://www.ypiao.com/yanchanghui/"


def fetch_html(url: str) -> str:
    """获取页面 HTML"""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def detect_status_piaoniu(item_html: str, show_time: str) -> str:
    """票牛网：综合判断演唱会状态"""
    if "售空" in item_html or "售罄" in item_html:
        return "已售罄"
    if "预售" in item_html or "即将开抢" in item_html:
        return "预售"
    if "热卖" in item_html or "抢票" in item_html:
        return "在售"

    if show_time:
        try:
            date_str = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", show_time)
            if date_str:
                show_date = datetime(int(date_str.group(1)), int(date_str.group(2)), int(date_str.group(3)))
                days_until = (datetime.now() - show_date).days
                if days_until > 0:
                    return "已结束"
                elif days_until <= -3:
                    return "在售"
                else:
                    return "预售"
        except (ValueError, IndexError):
            pass
    return "在售"


def detect_status_ypiao(show_time: str) -> str:
    """有票网：基于时间推断状态"""
    if not show_time or show_time == "等待官宣" or "待定" in show_time:
        return "待定"
    try:
        date_str = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", show_time)
        if date_str:
            show_date = datetime(int(date_str.group(1)), int(date_str.group(2)), int(date_str.group(3)))
            days_until = (datetime.now() - show_date).days
            if days_until > 0:
                return "已结束"
            elif days_until >= -7:
                return "在售"
            else:
                return "预售"
    except (ValueError, IndexError):
        pass
    return "预售"


def scrape_piaoniu() -> List[Dict]:
    """抓取票牛网所有江苏城市演唱会"""
    all_concerts = []

    for city_name, url in PIAONIU_CITIES.items():
        try:
            logger.info(f"[票牛网] 正在抓取 {city_name}...")
            html = fetch_html(url)

            item_pattern = re.compile(
                r'<li[^>]*class="item"[^>]*>(.*?)</li>',
                re.DOTALL | re.IGNORECASE
            )
            seen_ids = set()
            for match in item_pattern.finditer(html):
                item_html = match.group(1)

                id_match = re.search(r'/activity/(\d+)', item_html)
                if not id_match:
                    continue
                act_id = id_match.group(1)
                if act_id in seen_ids:
                    continue
                seen_ids.add(act_id)

                name = ""
                title_match = re.search(r'<div[^>]*class="title"[^>]*>(.*?)</div>', item_html, re.DOTALL)
                if title_match:
                    name = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()
                if not name:
                    alt_match = re.search(r'<img[^>]+alt="([^"]+)"', item_html)
                    if alt_match:
                        name = alt_match.group(1)
                if len(name) < 5:
                    continue

                if not any(kw in name for kw in CONCERT_KEYWORDS):
                    continue

                city_tags = re.findall(r"\[([^\]]{2,4})\]", name)
                cities = [c for c in city_tags if c in JIANGSU_NAMES]
                item_city = cities[0] if cities else ""
                if not item_city:
                    continue

                name = re.sub(r"\[([^\]]+)\]\s*", "", name).strip()
                name = re.sub(r"\[\w]+\]", "", name, count=1).strip()
                name = re.sub(r"\d+\.\d+折\s*", "", name).strip()

                show_time = ""
                time_match = re.search(r'<div[^>]*class="time"[^>]*>(.*?)</div>', item_html, re.DOTALL)
                if time_match:
                    show_time = re.sub(r"<[^>]+>", "", time_match.group(1)).strip()

                venue = ""
                venue_match = re.search(r'<div[^>]*class="venue"[^>]*>(.*?)</div>', item_html, re.DOTALL)
                if venue_match:
                    venue = re.sub(r"<[^>]+>", "", venue_match.group(1)).strip()

                price = ""
                amount_match = re.search(r'<span[^>]*class="amount"[^>]*>(\d+)</span>', item_html)
                if amount_match:
                    price = f"\u00a5{amount_match.group(1)}\u8d77"

                status = detect_status_piaoniu(item_html, show_time)

                img = ""
                img_match = re.search(r'<img[^>]+src="(https?://[^"]+poster[^"]*)"', item_html)
                if img_match:
                    img = img_match.group(1)

                all_concerts.append({
                    "id": f"pn_{act_id}",
                    "name": name,
                    "city": item_city,
                    "time": show_time,
                    "venue": venue,
                    "price": price,
                    "status": status,
                    "img": img,
                    "artist": "",
                    "category": "演唱会",
                    "source": "票牛网",
                    "url": f"https://www.piaoniu.com/activity/{act_id}",
                })

            logger.info(f"[票牛网][{city_name}] 提取到 {len(seen_ids)} 场演唱会")
            time.sleep(1)

        except Exception as e:
            logger.error(f"[票牛网][{city_name}] 抓取失败: {e}")

    logger.info(f"票牛网共抓取 {len(all_concerts)} 场演唱会")
    return all_concerts


def scrape_ypiao() -> List[Dict]:
    """抓取有票网全国演唱会列表，筛选江苏城市数据"""
    concerts = []
    try:
        logger.info("[有票网] 开始抓取...")
        html = fetch_html(YPIAO_URL)

        item_starts = [(m.start(), m.end()) for m in re.finditer(r'<div class="jieguo-xm">', html)]
        seen_ids = set()

        for start_pos, after_div in item_starts:
            chunk = html[after_div:after_div + 1500]

            id_match = re.search(r'href="/t_(\d+)/"', chunk)
            if not id_match:
                continue
            act_id = id_match.group(1)
            if act_id in seen_ids:
                continue
            seen_ids.add(act_id)

            name = ""
            all_titles = re.findall(r'class="cc-title"[^>]*>\s*(.*?)\s*</a>', chunk, re.DOTALL)
            if all_titles:
                for title in all_titles:
                    clean_title = re.sub(r"<[^>]+>", "", title).strip()
                    if len(clean_title) >= 5:
                        name = clean_title
                        break
            if not name:
                continue

            if not any(kw in name for kw in CONCERT_KEYWORDS):
                continue

            show_time = ""
            time_match = re.search(r'class="blc cc-time"[^>]*>\s*(.*?)\s*</span>', chunk, re.DOTALL)
            if time_match:
                show_time = re.sub(r"<[^>]+>", "", time_match.group(1)).strip()

            venue = ""
            venue_city = ""
            venue_name = ""
            venue_match = re.search(r'class="blc cc-changguan"[^>]*>\s*(.*?)\s*</span>', chunk, re.DOTALL)
            if venue_match:
                venue = re.sub(r"<[^>]+>", "", venue_match.group(1)).strip()
                city_match = re.match(r'\[([^\]]+)\](.*)', venue)
                if city_match:
                    venue_city = city_match.group(1)
                    venue_name = city_match.group(2).strip()
                else:
                    venue_name = venue

            if venue_city not in JIANGSU_NAMES:
                continue

            price = ""
            price_match = re.search(r'class="cc-price"[^>]*>([^<]+)', chunk)
            if price_match:
                price = price_match.group(1).strip() + "起"

            img = ""
            img_match = re.search(r'class="l cc-haibao"[^>]+data-original="([^"]+)"', chunk)
            if not img_match:
                img_match = re.search(r'class="l cc-haibao"[^>]+src="(https?://[^"]+)"', chunk)
            if img_match:
                img = img_match.group(1)

            status = detect_status_ypiao(show_time)
            if "补票中" in chunk:
                status = "补票中"

            status_text_match = re.search(r'class="cc-price1[^"]*"[^>]*>([^<]+)', chunk)
            if status_text_match:
                st = status_text_match.group(1).strip()
                if st and st not in ("", "¥0起", "0起"):
                    status = st

            concerts.append({
                "id": f"yp_{act_id}",
                "name": name,
                "city": venue_city,
                "time": show_time,
                "venue": venue_name,
                "price": price,
                "status": status,
                "img": img,
                "artist": "",
                "category": "演唱会",
                "source": "有票网",
                "url": f"https://www.ypiao.com/t_{act_id}/",
            })

    except Exception as e:
        logger.error(f"[有票网] 抓取失败: {e}")

    logger.info(f"[有票网] 获取到 {len(concerts)} 场演唱会")
    return concerts


def scrape_all() -> List[Dict]:
    """执行全部数据抓取"""
    logger.info("=" * 50)
    logger.info(f"开始抓取演唱会数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

    all_data = []

    # 数据源1: 票牛网
    try:
        live_data = scrape_piaoniu()
        if live_data:
            all_data.extend(live_data)
    except Exception as e:
        logger.error(f"[票牛网] 抓取异常: {e}")

    # 数据源2: 有票网
    try:
        yp_data = scrape_ypiao()
        if yp_data:
            all_data.extend(yp_data)
    except Exception as e:
        logger.error(f"[有票网] 抓取异常: {e}")

    # 去重（以名称+城市+时间为key）
    seen = set()
    unique = []
    for c in all_data:
        key = (c.get("name", ""), c.get("city", ""), c.get("time", ""))
        if key not in seen:
            seen.add(key)
            unique.append(c)

    # 按城市和时间排序
    unique.sort(key=lambda x: (x.get("city", ""), x.get("time", "")))

    # 保存到本地
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(DATA_DIR, f"concerts_{timestamp}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    latest_path = os.path.join(DATA_DIR, "latest.json")
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    # 同时输出到站点目录（供静态页面读取）
    site_data_path = os.path.join(os.path.dirname(__file__), "site", "data.json")
    os.makedirs(os.path.dirname(site_data_path), exist_ok=True)
    with open(site_data_path, "w", encoding="utf-8") as f:
        json.dump({
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total": len(unique),
            "concerts": unique
        }, f, ensure_ascii=False, indent=2)

    logger.info(f"数据抓取完成，共 {len(unique)} 场演唱会（去重后）")
    return unique


def load_latest() -> List[Dict]:
    """加载最新缓存数据"""
    latest_path = os.path.join(DATA_DIR, "latest.json")
    if os.path.exists(latest_path):
        with open(latest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    data = scrape_all()
    print(f"\n共获取 {len(data)} 场演唱会")
    for c in data[:15]:
        print(f"  [{c['city']}] {c['name']} | {c['time']} | {c['venue']} | {c['price']}")
