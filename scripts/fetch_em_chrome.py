#!/usr/bin/env python3
"""
通过 Chrome headless 批量获取东方财富 F10 行业信息。
用法: python3 fetch_em_chrome.py [batch_size]
"""
import csv
import json
import re
import subprocess
import sys
import time
from pathlib import Path

# ===== 配置 =====
BASE_DIR = Path.home() / "Develop" / "gitstore" / "qlquant" / "marketResearch"
CSV_PATH = BASE_DIR / "output" / "companies.csv"
CACHE_FILE = BASE_DIR / "scripts" / "em_chrome_industry_cache.json"
PROGRESS_FILE = BASE_DIR / "scripts" / "em_chrome_progress.json"
BATCH_SIZE = int(sys.argv[1]) if len(sys.argv) > 1 else 200
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
TIMEOUT = 12  # 秒

# 读取 CSV
rows = []
with open(CSV_PATH, newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for row in reader:
        rows.append(row)

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

def code_to_em(code):
    """转换代码为 East Money 格式"""
    code = code.strip().upper()
    if code.startswith('SH'):
        return f"S{code[2:]}"
    elif code.startswith('SZ'):
        return f"S{code[2:]}"
    elif code.startswith('BJ'):
        return f"BJ{code[2:]}"
    # 纯数字代码: 6开头=上海, 0/4开头=深圳, 8开头=北交所
    elif code.isdigit():
        if code.startswith('6') or code.startswith('9'):
            return f"S{code}"
        elif code.startswith('4') or code.startswith('8'):
            return f"BJ{code}"
        else:
            return f"S{code}"
    return None

def fetch_industry_chrome(code, name):
    """用 Chrome headless 获取行业信息"""
    em_code = code_to_em(code)
    if not em_code:
        return None
    
    url = f"https://emweb.securities.eastmoney.com/PC_HSF10/pages/index.html?type=web&code={em_code}&color=b#/gsgk"
    
    try:
        result = subprocess.run(
            [CHROME, '--headless=new', '--dump-dom',
             f'--virtual-time-budget={TIMEOUT * 1000}', url],
            capture_output=True, text=True, timeout=TIMEOUT + 5
        )
        html = result.stdout
        
        # 解析"所属东财行业"字段
        # 格式: <th>所属东财行业</th><td>房地产-房地产开发-住宅开发</td>
        m = re.search(r'<th[^>]*>\s*所属东财行业\s*</th>\s*<td>([^<]+)</td>', html)
        if m:
            industry = m.group(1).strip()
            return industry
        
        # 备选: 解析"所属行业"字段
        m = re.search(r'<th[^>]*>\s*所属行业\s*</th>\s*<td>([^<]+)</td>', html)
        if m:
            return m.group(1).strip()
            
    except Exception as e:
        print(f"  Error fetching {code}: {e}")
    
    return None

total = len(missing)
updated = 0

print(f"开始处理 {min(BATCH_SIZE, total)} 只...")

for i, row in enumerate(missing[:BATCH_SIZE]):
    code = row['股票代码'].strip()
    name = row['股票名称'].strip()
    
    industry = cache.get(code)
    
    if not industry:
        if code in progress and progress[code]:
            industry = progress[code]
        
    if not industry:
        industry = fetch_industry_chrome(code, name)
        if industry:
            cache[code] = industry
    
    if industry:
        for r in rows:
            if r['股票代码'].strip() == code:
                r['行业大类'] = industry
                updated += 1
                break
    
    progress[code] = industry or ''
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False, indent=2))
    
    status = f"✓ {industry}" if industry else "✗ 未找到"
    print(f"[{i+1}/{min(BATCH_SIZE,total)}] {code} {name} -> {status}")
    
    # 每处理10只或最后一只时保存CSV和缓存
    if (i + 1) % 10 == 0 or i == min(BATCH_SIZE, total) - 1:
        with open(CSV_PATH, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2))
    
    time.sleep(1)

print(f"\n完成! 本批更新 {updated} 只, 累计缓存 {len(cache)} 只")
print(f"CSV: {CSV_PATH}")