"""
江苏演唱会监控 - Flask Web 应用
数据源：票牛网（piaoniu.com）+ DrissionPage 浏览器自动化
"""

from flask import Flask, render_template, jsonify, request
from scraper import scrape_all, load_latest
from datetime import datetime
import os

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


@app.route("/")
def index():
    data = load_latest()
    data = [c for c in data if c.get("source") != "示例数据"]
    update_time = "暂无数据"
    if data:
        latest_path = os.path.join(DATA_DIR, "latest.json")
        if os.path.exists(latest_path):
            mtime = os.path.getmtime(latest_path)
            update_time = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")

    # 统计数据
    cities = {}
    status_counts = {}

    for c in data:
        city = c.get("city", "未知")
        cities[city] = cities.get(city, 0) + 1

        status = c.get("status", "未知")
        status_counts[status] = status_counts.get(status, 0) + 1

    return render_template("index.html",
                           concerts=data,
                           update_time=update_time,
                           total_count=len(data),
                           cities=cities,
                           status_counts=status_counts)


@app.route("/api/concerts")
def api_concerts():
    """返回演唱会JSON数据"""
    data = [c for c in load_latest() if c.get("source") != "示例数据"]
    city = request.args.get("city", "")
    status = request.args.get("status", "")
    keyword = request.args.get("keyword", "")

    filtered = data
    if city:
        filtered = [c for c in filtered if city in c.get("city", "")]
    if status:
        filtered = [c for c in filtered if c.get("status") == status]
    if keyword:
        filtered = [c for c in filtered
                    if keyword.lower() in c.get("name", "").lower()
                    or keyword.lower() in c.get("artist", "").lower()]

    return jsonify({"total": len(filtered), "data": filtered})


@app.route("/api/refresh")
def api_refresh():
    """手动触发刷新数据"""
    try:
        data = scrape_all()
        return jsonify({"success": True, "total": len(data),
                        "message": f"成功刷新，获取 {len(data)} 场演唱会"})
    except Exception as e:
        return jsonify({"success": False, "message": f"刷新失败: {str(e)}"})


if __name__ == "__main__":
    # 确保有数据，没有则抓取
    data = load_latest()
    if not data:
        print("无缓存数据，正在抓取演唱会数据...")
        scrape_all()
    else:
        print(f"加载缓存数据，共 {len(data)} 场演唱会")
    print("启动Web服务...")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
