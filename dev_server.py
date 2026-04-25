"""
本地开发服务器，模拟 Cloudflare /cdn-cgi/trace 接口
用法: python dev_server.py [port]
默认端口 9090，访问 http://localhost:9090
"""
import http.server
import socketserver
import urllib.request
import json
import sys
import os

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9090
SITE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'site')

# 省份英文→CF loc 代码
REGION_TO_LOC = {
    "Beijing": "BJ", "Shanghai": "SH", "Tianjin": "TJ", "Chongqing": "CQ",
    "Hebei": "HE", "Shanxi": "SX", "Inner Mongolia": "NM", "Liaoning": "LN", "Jilin": "JL",
    "Heilongjiang": "HL", "Jiangsu": "JS", "Zhejiang": "ZJ", "Anhui": "AH", "Fujian": "FJ",
    "Jiangxi": "JX", "Shandong": "SD", "Henan": "HA", "Hubei": "HB", "Hunan": "HN",
    "Guangdong": "GD", "Guangxi": "GX", "Hainan": "HI", "Sichuan": "SC", "Guizhou": "GZ",
    "Yunnan": "YN", "Shaanxi": "SN", "Gansu": "GS", "Qinghai": "QH",
    "Ningxia": "NX", "Xinjiang": "XJ", "Tibet": "XZ", "Taiwan": "TW",
    "Macau": "MO", "Hong Kong": "HK",
}

# 缓存定位结果
_cached_loc = None


def get_location():
    global _cached_loc
    if _cached_loc:
        return _cached_loc
    try:
        req = urllib.request.Request('https://ipwho.is/')
        req.add_header('User-Agent', 'Mozilla/5.0')
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read().decode())
        if data.get('success') and data.get('country_code') == 'CN':
            region = data.get('region', '')
            _cached_loc = REGION_TO_LOC.get(region, 'JS')  # 默认江苏
            return _cached_loc
    except Exception:
        pass
    _cached_loc = 'JS'  # 定位失败默认江苏
    return _cached_loc


class DevHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=SITE_DIR, **kwargs)

    def do_GET(self):
        if self.path == '/cdn-cgi/trace':
            loc = get_location()
            body = f"loc={loc}\nip=127.0.0.1\n"
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body.encode())
        else:
            super().do_GET()

    def log_message(self, format, *args):
        # 简化日志
        print(f"  {args[0]}")


if __name__ == '__main__':
    loc = get_location()
    print(f"  📍 定位结果: {loc}")
    print(f"  🚀 本地服务器: http://localhost:{PORT}")
    print(f"  按 Ctrl+C 停止\n")
    with socketserver.TCPServer(("", PORT), DevHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n  已停止")
