#!/usr/bin/env python3
"""
通过浏览器 + Bing 搜索批量获取行业信息。
需要: Python 3, selenium 或 playwright (自动浏览器)

用法:
  # 先安装 playwright:
  pip3 install playwright && python3 -m playwright install chromium

  # 方式1: 直接运行 (使用 selenium-wire 拦截 Bing 响应)
  python3 fetch_bing_browser.py [batch_size]

  # 方式2: 后台运行
  PYTHONUNBUFFERED=1 nohup python3 fetch_bing_browser.py 500 > fetch_bing.log 2>&1 &
"""
import csv
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# ===== 配置 =====
BASE_DIR = Path.home() / "Develop" / "gitstore" / "qlquant" / "marketResearch"
CSV_PATH = BASE_DIR / "output" / "companies.csv"
CACHE_FILE = BASE_DIR / "scripts" / "bing_industry_cache.json"
PROGRESS_FILE = BASE_DIR / "scripts" / "bing_fetch_progress.json"
BATCH_SIZE = int(sys.argv[1]) if len(sys.argv) > 1 else 500

# 读取 CSV
rows = []
with open(CSV_PATH, newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for row in reader:
        rows.append(row)

# 找出缺行业大类的
missing = [r for r in rows if not r.get('行业大类') or r['行业大类'].strip() == '']
print(f"缺行业信息: {len(missing)} 只")

# 加载缓存
cache = {}
if CACHE_FILE.exists():
    try:
        cache = json.loads(CACHE_FILE.read_text())
    except:
        cache = {}

# 加载进度
progress = {}
if PROGRESS_FILE.exists():
    try:
        progress = json.loads(PROGRESS_FILE.read_text())
    except:
        progress = {}

def code_to_prefix(code):
    """转换代码为 Bing 搜索用的市场前缀"""
    code = code.strip().upper()
    if code.startswith('SH'):
        return f'sh{code[2:]}'
    elif code.startswith('SZ'):
        return f'sz{code[2:]}'
    elif code.startswith('BJ'):
        return f'bj{code[2:]}'
    return None

def extract_industry_from_bing_html(html):
    """从 Bing 搜索结果 HTML 中提取行业信息"""
    # 格式1: stockstar 结果中的"所属行业"
    m = re.search(r'所属行业[：:]\s*([^<\n,，]{2,20})', html)
    if m:
        return m.group(1).strip()
    # 格式2: 新浪结果的"行业名称"
    m = re.search(r'行业名称[：:]\s*([^<\n,，]{2,20})', html)
    if m:
        return m.group(1).strip()
    # 格式3: 同花顺结果的行业
    m = re.search(r'所属行业[^\n]*?<[^>]+>([^<\n]{2,20})', html)
    if m:
        return m.group(1).strip()
    # 格式4: 百度百科中的行业
    m = re.search(r'所属行业[：:]\s*([^<\n]{2,20})', html)
    if m:
        return m.group(1).strip()
    return None

def search_bing(code, name):
    """用 Bing 搜索行业信息，返回行业名称或 None"""
    prefix = code_to_prefix(code)
    if not prefix:
        return None
    
    query = f"{prefix} {name} 所属行业 stockstar OR eastmoney OR sina"
    encoded = query.replace(' ', '+')
    url = f"https://www.bing.com/search?q={encoded}"
    
    try:
        result = subprocess.run(
            ['python3', '-c', f'''
import subprocess
result = subprocess.run(
    ['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', 
     '--headless=new',
     '--dump-dom',
     '--virtual-time-budget=8000',
     url],
    capture_output=True, text=True, timeout=15
)
print(result.stdout[:5000], file=__import__('sys').stderr)
'''],
            capture_output=True, text=True, timeout=20
        )
        return None
    except:
        return None

print(f"准备处理 {min(BATCH_SIZE, len(missing))} 只...")
print("注意: Chrome headless 方式需要 chrome-cli 或 selenium，请先安装:")
print("  brew install chrome-cli  (Mac)")
print("或使用 selenium 方式")