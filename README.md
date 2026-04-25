# 江苏演唱会·集

聚合江苏地区演唱会信息，数据源：票牛网 + 有票网。

## 架构

```
GitHub Actions（每30分钟）
  → 运行 scraper.py 爬取数据
  → 输出 site/data.json
  → 自动提交到仓库
  → Cloudflare Pages 自动部署
```

## 本地开发

```bash
# 安装依赖
pip install -r requirements.txt

# 运行爬虫
python scraper.py

# 本地预览（任选一种）
cd site && python -m http.server 8080
# 或用 Flask 版本
python app.py
```
