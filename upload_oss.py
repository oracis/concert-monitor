"""
上传 site/ 整个目录到阿里云 OSS
用法: python upload_oss.py <access_key_id> <access_key_secret> [bucket_name] [endpoint]
"""
import sys, os, time, hashlib, hmac, base64, urllib.request, urllib.error

BUCKET = sys.argv[3] if len(sys.argv) > 3 else 'concert-monitor-hk'
ENDPOINT = sys.argv[4] if len(sys.argv) > 4 else 'oss-cn-hongkong.aliyuncs.com'
SITE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'site')

MIME_TYPES = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css',
    '.js': 'application/javascript',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
    '.ttf': 'font/ttf',
    '.eot': 'application/vnd.ms-fontobject',
}

def sign(verb, key, content_type, date_str, secret):
    """阿里云 OSS Signature V1"""
    string_to_sign = f"{verb}\n\n{content_type}\n{date_str}\n/{BUCKET}/{key}"
    signature = base64.b64encode(
        hmac.new(secret.encode(), string_to_sign.encode(), hashlib.sha1).digest()
    ).decode()
    return signature

def upload_v1(access_key_id, access_key_secret):
    """使用 Signature V1 上传（无需安装 SDK）"""
    files = []
    for root, dirs, filenames in os.walk(SITE_DIR):
        for f in filenames:
            files.append(os.path.join(root, f))

    for filepath in files:
        relpath = os.path.relpath(filepath, SITE_DIR)
        ext = os.path.splitext(relpath)[1].lower()
        content_type = MIME_TYPES.get(ext, 'application/octet-stream')
        date_str = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime())

        with open(filepath, 'rb') as fp:
            body = fp.read()

        signature = sign('PUT', relpath, content_type, date_str, access_key_secret)
        url = f"https://{BUCKET}.{ENDPOINT}/{relpath}"
        req = urllib.request.Request(url, data=body, method='PUT')
        req.add_header('Content-Type', content_type)
        req.add_header('Date', date_str)
        req.add_header('Authorization', f"OSS {access_key_id}:{signature}")

        try:
            resp = urllib.request.urlopen(req)
            print(f"✅ {relpath} → HTTP {resp.status}")
        except urllib.error.HTTPError as e:
            print(f"❌ 上传失败: {relpath} → {e.code} {e.reason}")
            print(f"   Response: {e.read().decode()}")
            sys.exit(1)

    print(f"✅ 全部上传完成，共 {len(files)} 个文件")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("用法: python upload_oss.py <access_key_id> <access_key_secret> [bucket_name] [endpoint]")
        sys.exit(1)
    upload_v1(sys.argv[1], sys.argv[2])
