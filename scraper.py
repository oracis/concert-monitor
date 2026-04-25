"""
演唱会·集 - 数据抓取模块（全国版）
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

# 演唱会关键词
CONCERT_KEYWORDS = ["演唱会", "巡演", "音乐节", "TOUR", "Live", "CONCERT"]

# 通用请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}

# 票牛网城市 URL 映射（全国主要城市）
PIAONIU_CITIES = {
    # 北京
    "北京": "https://www.piaoniu.com/beijing",
    # 上海
    "上海": "https://www.piaoniu.com/shanghai",
    # 广东
    "广州": "https://www.piaoniu.com/guangzhou",
    "深圳": "https://www.piaoniu.com/shenzhen",
    "佛山": "https://www.piaoniu.com/foshan",
    "东莞": "https://www.piaoniu.com/dongguan",
    "珠海": "https://www.piaoniu.com/zhuhai",
    # 江苏
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
    # 浙江
    "杭州": "https://www.piaoniu.com/hangzhou",
    "宁波": "https://www.piaoniu.com/ningbo",
    "温州": "https://www.piaoniu.com/wenzhou",
    "嘉兴": "https://www.piaoniu.com/jiaxing",
    "绍兴": "https://www.piaoniu.com/shaoxing",
    "金华": "https://www.piaoniu.com/jinhua",
    # 四川
    "成都": "https://www.piaoniu.com/chengdu",
    "绵阳": "https://www.piaoniu.com/mianyang",
    # 湖北
    "武汉": "https://www.piaoniu.com/wuhan",
    "宜昌": "https://www.piaoniu.com/yichang",
    # 湖南
    "长沙": "https://www.piaoniu.com/changsha",
    "株洲": "https://www.piaoniu.com/zhuzhou",
    # 福建
    "福州": "https://www.piaoniu.com/fuzhou",
    "厦门": "https://www.piaoniu.com/xiamen",
    "泉州": "https://www.piaoniu.com/quanzhou",
    # 山东
    "济南": "https://www.piaoniu.com/jinan",
    "青岛": "https://www.piaoniu.com/qingdao",
    "烟台": "https://www.piaoniu.com/yantai",
    "淄博": "https://www.piaoniu.com/zibo",
    # 河南
    "郑州": "https://www.piaoniu.com/zhengzhou",
    "洛阳": "https://www.piaoniu.com/luoyang",
    # 天津
    "天津": "https://www.piaoniu.com/tianjin",
    # 重庆
    "重庆": "https://www.piaoniu.com/chongqing",
    # 陕西
    "西安": "https://www.piaoniu.com/xian",
    # 辽宁
    "沈阳": "https://www.piaoniu.com/shenyang",
    "大连": "https://www.piaoniu.com/dalian",
    # 黑龙江
    "哈尔滨": "https://www.piaoniu.com/haerbin",
    # 吉林
    "长春": "https://www.piaoniu.com/changchun",
    # 河北
    "石家庄": "https://www.piaoniu.com/shijiazhuang",
    "保定": "https://www.piaoniu.com/baoding",
    # 山西
    "太原": "https://www.piaoniu.com/taiyuan",
    # 安徽
    "合肥": "https://www.piaoniu.com/hefei",
    "芜湖": "https://www.piaoniu.com/wuhu",
    # 江西
    "南昌": "https://www.piaoniu.com/nanchang",
    "赣州": "https://www.piaoniu.com/ganzhou",
    # 广西
    "南宁": "https://www.piaoniu.com/nanning",
    "桂林": "https://www.piaoniu.com/guilin",
    # 云南
    "昆明": "https://www.piaoniu.com/kunming",
    # 贵州
    "贵阳": "https://www.piaoniu.com/guiyang",
    # 甘肃
    "兰州": "https://www.piaoniu.com/lanzhou",
    # 海南
    "海口": "https://www.piaoniu.com/haikou",
    "三亚": "https://www.piaoniu.com/sanya",
    # 内蒙古
    "呼和浩特": "https://www.piaoniu.com/huhehaote",
    "包头": "https://www.piaoniu.com/baotou",
    # 新疆
    "乌鲁木齐": "https://www.piaoniu.com/wulumuqi",
    # 宁夏
    "银川": "https://www.piaoniu.com/yinchuan",
    # 西藏
    "拉萨": "https://www.piaoniu.com/lasa",
    # 青海
    "西宁": "https://www.piaoniu.com/xining",
}

# 城市到省份的映射（覆盖所有可能在数据中出现的城市）
CITY_TO_PROVINCE = {
    # 直辖市
    "北京": "北京", "上海": "上海", "天津": "天津", "重庆": "重庆",
    # 广东
    "广州": "广东", "深圳": "广东", "佛山": "广东", "东莞": "广东", "珠海": "广东",
    "惠州": "广东", "中山": "广东", "汕头": "广东", "江门": "广东", "湛江": "广东",
    "肇庆": "广东", "梅州": "广东", "清远": "广东", "揭阳": "广东", "韶关": "广东",
    # 江苏
    "南京": "江苏", "苏州": "江苏", "无锡": "江苏", "常州": "江苏", "南通": "江苏",
    "徐州": "江苏", "扬州": "江苏", "盐城": "江苏", "泰州": "江苏", "镇江": "江苏",
    "淮安": "江苏", "连云港": "江苏", "宿迁": "江苏",
    # 浙江
    "杭州": "浙江", "宁波": "浙江", "温州": "浙江", "嘉兴": "浙江", "绍兴": "浙江",
    "金华": "浙江", "台州": "浙江", "湖州": "浙江", "丽水": "浙江", "衢州": "浙江",
    "舟山": "浙江",
    # 四川
    "成都": "四川", "绵阳": "四川", "德阳": "四川", "宜宾": "四川", "泸州": "四川",
    "南充": "四川", "乐山": "四川", "达州": "四川", "自贡": "四川", "内江": "四川",
    # 湖北
    "武汉": "湖北", "宜昌": "湖北", "襄阳": "湖北", "荆州": "湖北", "黄冈": "湖北",
    "十堰": "湖北", "孝感": "湖北", "荆门": "湖北", "咸宁": "湖北",
    # 湖南
    "长沙": "湖南", "株洲": "湖南", "湘潭": "湖南", "衡阳": "湖南", "岳阳": "湖南",
    "常德": "湖南", "郴州": "湖南", "邵阳": "湖南", "益阳": "湖南",
    # 福建
    "福州": "福建", "厦门": "福建", "泉州": "福建", "漳州": "福建", "莆田": "福建",
    "龙岩": "福建", "三明": "福建",
    # 山东
    "济南": "山东", "青岛": "山东", "烟台": "山东", "淄博": "山东", "潍坊": "山东",
    "临沂": "山东", "济宁": "山东", "泰安": "山东", "威海": "山东", "德州": "山东",
    "聊城": "山东", "菏泽": "山东", "枣庄": "山东",
    # 河南
    "郑州": "河南", "洛阳": "河南", "开封": "河南", "新乡": "河南", "安阳": "河南",
    "南阳": "河南", "许昌": "河南", "信阳": "河南", "驻马店": "河南", "周口": "河南",
    # 陕西
    "西安": "陕西", "咸阳": "陕西", "宝鸡": "陕西", "渭南": "陕西",
    # 辽宁
    "沈阳": "辽宁", "大连": "辽宁", "鞍山": "辽宁", "抚顺": "辽宁", "锦州": "辽宁",
    # 黑龙江
    "哈尔滨": "黑龙江", "大庆": "黑龙江", "齐齐哈尔": "黑龙江", "牡丹江": "黑龙江",
    # 吉林
    "长春": "吉林", "吉林": "吉林", "四平": "吉林",
    # 河北
    "石家庄": "河北", "保定": "河北", "唐山": "河北", "廊坊": "河北", "邯郸": "河北",
    "秦皇岛": "河北", "沧州": "河北", "邢台": "河北", "张家口": "河北",
    # 山西
    "太原": "山西", "大同": "山西", "临汾": "山西", "运城": "山西", "晋中": "山西",
    # 安徽
    "合肥": "安徽", "芜湖": "安徽", "蚌埠": "安徽", "阜阳": "安徽", "淮南": "安徽",
    "安庆": "安徽", "马鞍山": "安徽", "滁州": "安徽", "六安": "安徽", "宿州": "安徽",
    # 江西
    "南昌": "江西", "赣州": "江西", "九江": "江西", "上饶": "江西", "景德镇": "江西",
    "宜春": "江西", "吉安": "江西",
    # 广西
    "南宁": "广西", "桂林": "广西", "柳州": "广西", "北海": "广西", "玉林": "广西",
    "梧州": "广西", "钦州": "广西",
    # 云南
    "昆明": "云南", "大理": "云南", "丽江": "云南", "曲靖": "云南", "玉溪": "云南",
    # 贵州
    "贵阳": "贵州", "遵义": "贵州", "六盘水": "贵州",
    # 甘肃
    "兰州": "甘肃", "天水": "甘肃",
    # 海南
    "海口": "海南", "三亚": "海南", "儋州": "海南",
    # 内蒙古
    "呼和浩特": "内蒙古", "包头": "内蒙古", "鄂尔多斯": "内蒙古", "赤峰": "内蒙古",
    # 新疆
    "乌鲁木齐": "新疆", "伊犁": "新疆", "喀什": "新疆",
    # 宁夏
    "银川": "宁夏",
    # 西藏
    "拉萨": "西藏",
    # 青海
    "西宁": "青海",
    # 台湾
    "台北": "台湾", "高雄": "台湾",
    # 香港
    "香港": "香港",
    # 澳门
    "澳门": "澳门",
}

ALL_CITIES = set(CITY_TO_PROVINCE.keys())

# 有票网 URL（全国演唱会页面）
YPIAO_URL = "https://www.ypiao.com/yanchanghui/"


def get_province(city: str) -> str:
    """根据城市名获取省份"""
    return CITY_TO_PROVINCE.get(city, "")


def fetch_html(url: str) -> str:
    """获取页面 HTML（404 也返回页面内容，票牛网对无演出城市返回 404）"""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code == 404:
        logger.warning(f"页面返回 404（可能该城市暂无演出）: {url}")
        return resp.text
    resp.raise_for_status()
    return resp.text


def parse_show_time(show_time: str) -> tuple:
    """解析演出时间，返回 (开始日期, 结束日期) 的 datetime 元组。
    支持格式：2026.05.22-05.23 / 2026.05.22~05.23 / 2026.05.22 - 05.23 等。
    如果没有结束日期，结束日期 = 开始日期。
    """
    if not show_time:
        return (None, None)
    # 尝试匹配双日期格式：2026.05.22-05.23 或 2026.05.22 - 05.23
    dual = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})\s*[-~～至]\s*(\d{1,2})[.\-/](\d{1,2})", show_time)
    if dual:
        start = datetime(int(dual.group(1)), int(dual.group(2)), int(dual.group(3)))
        end = datetime(int(dual.group(1)), int(dual.group(4)), int(dual.group(5)))
        return (start, end)
    # 单日期
    single = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", show_time)
    if single:
        d = datetime(int(single.group(1)), int(single.group(2)), int(single.group(3)))
        return (d, d)
    return (None, None)


def detect_status_piaoniu(item_html: str, show_time: str) -> str:
    """票牛网：综合判断演唱会状态"""
    if "售空" in item_html or "售罄" in item_html:
        return "已售罄"
    if "预售" in item_html or "即将开抢" in item_html:
        return "预售"
    if "热卖" in item_html or "抢票" in item_html:
        return "在售"

    start, end = parse_show_time(show_time)
    if start:
        days_until = (datetime.now() - start).days
        days_until_end = (datetime.now() - end).days
        if days_until_end > 0:
            return "已结束"
        elif days_until <= -3:
            return "在售"
        else:
            return "预售"
    return "在售"


def detect_status_ypiao(show_time: str) -> str:
    """有票网：基于时间推断状态"""
    if not show_time or show_time == "等待官宣" or "待定" in show_time:
        return "待定"
    start, end = parse_show_time(show_time)
    if start:
        days_until = (datetime.now() - start).days
        days_until_end = (datetime.now() - end).days
        if days_until_end > 0:
            return "已结束"
        elif days_until >= -7:
            return "在售"
        else:
            return "预售"
    return "预售"


def scrape_piaoniu() -> List[Dict]:
    """抓取票牛网全国主要城市演唱会"""
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

                # 从标题中提取城市标签
                city_tags = re.findall(r"\[([^\]]{2,4})\]", name)
                cities = [c for c in city_tags if c in ALL_CITIES]
                item_city = cities[0] if cities else ""
                if not item_city:
                    # 没有城市标签，用当前抓取的城市
                    item_city = city_name

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
                # 票牛网图片有三种格式，按优先级依次尝试：
                # 1. img data-src（南京等城市常用）
                img_match = re.search(r'<img[^>]+data-src="(https?://[^"]+)"', item_html)
                # 2. div data-style（苏州等城市常用，background-image 方式）
                if not img_match:
                    img_match = re.search(r'data-style="background-image:url\((https?://[^)]+)\)"', item_html)
                # 3. img src（兜底）
                if not img_match:
                    img_match = re.search(r'<img[^>]+src="(https?://[^"]+poster[^"]*)"', item_html)
                if img_match:
                    img = img_match.group(1)
                    # 协议自适应：// 开头补 https
                    if img.startswith("//"):
                        img = "https:" + img

                province = get_province(item_city)
                all_concerts.append({
                    "id": f"pn_{act_id}",
                    "name": name,
                    "city": item_city,
                    "province": province,
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
            time.sleep(0.5)

        except Exception as e:
            logger.error(f"[票牛网][{city_name}] 抓取失败: {e}")

    logger.info(f"票牛网共抓取 {len(all_concerts)} 场演唱会")
    return all_concerts


def scrape_ypiao() -> List[Dict]:
    """抓取有票网全国演唱会列表"""
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

            if not venue_city:
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

            province = get_province(venue_city)

            concerts.append({
                "id": f"yp_{act_id}",
                "name": name,
                "city": venue_city,
                "province": province,
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
    logger.info(f"开始抓取演唱会数据（全国）- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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

    # 按省份、城市和时间排序
    unique.sort(key=lambda x: (x.get("province", ""), x.get("city", ""), x.get("time", "")))

    # 统计省份信息
    provinces = {}
    for c in unique:
        p = c.get("province", "其他")
        if not p:
            p = "其他"
            c["province"] = p
        provinces[p] = provinces.get(p, 0) + 1

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
            "provinces": provinces,
            "concerts": unique
        }, f, ensure_ascii=False, indent=2)

    logger.info(f"数据抓取完成，共 {len(unique)} 场演唱会（去重后），覆盖 {len(provinces)} 个省份")
    for p, count in sorted(provinces.items(), key=lambda x: -x[1]):
        logger.info(f"  {p}: {count} 场")
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
    for c in data[:20]:
        print(f"  [{c.get('province', '')}·{c['city']}] {c['name']} | {c['time']} | {c['price']}")
