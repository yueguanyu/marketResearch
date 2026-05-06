#!/usr/bin/env python3
"""
通过 East Money F10 公司概况页批量获取行业信息。
依赖: Python 3, subprocess (调用 OS 级浏览器)
用法: python3 fetch_em_f10.py [每批数量]
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
CACHE_FILE = BASE_DIR / "scripts" / "em_industry_cache.json"
PROGRESS_FILE = BASE_DIR / "scripts" / "em_fetch_progress.json"
BATCH_SIZE = int(sys.argv[1]) if len(sys.argv) > 1 else 200

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

total = len(missing)
updated = 0

def get_em_url(code, name):
    """构造 East Money F10 公司概况 URL"""
    code = code.strip()
    if code.startswith('SH'):
        return f"https://emweb.securities.eastmoney.com/PC_HSF10/pages/index.html?type=web&code=S{code[2:]}&color=b#/gsgk"
    elif code.startswith('SZ'):
        return f"https://emweb.securities.eastmoney.com/PC_HSF10/pages/index.html?type=web&code=S{code[2:]}&color=b#/gsgk"
    elif code.startswith('BJ'):
        return f"https://emweb.securities.eastmoney.com/PC_HSF10/pages/index.html?type=web&code=BJ{code[2:]}&color=b#/gsgk"
    return None

def extract_industry_from_html(html):
    """从 East Money F10 HTML 中提取行业信息"""
    # 东方财富行业分类一般在页面的行业字段中
    # 格式1: 所属行业：银行
    m = re.search(r'所属行业[：:]\s*([^<\n,，]{2,20})', html)
    if m:
        return m.group(1).strip()
    # 格式2: 行业类别：房地产
    m = re.search(r'行业类别[：:]\s*([^<\n,，]{2,20})', html)
    if m:
        return m.group(1).strip()
    return None

# 用 curl 抓 East Money 股票页面（非API，纯HTML）
def fetch_em_page(code):
    """用 curl 抓 East Money 股票详情页"""
    url = f"https://quote.eastmoney.com/{code.lower()}.html"
    try:
        result = subprocess.run(
            ['curl', '-s', '--max-time', '8', '-L',
             '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
             '-H', 'Referer: https://quote.eastmoney.com/',
             url],
            capture_output=True, text=True, timeout=12
        )
        if result.returncode == 0:
            return result.stdout
    except:
        pass
    return None

# 用 curl 抓 East Money F10 API
def fetch_em_f10_api(code):
    """调 East Money F10 API 获取公司概况"""
    # 转换代码格式
    if code.startswith('SH'):
        secid = f'1.{code[2:]}'
    elif code.startswith('SZ'):
        secid = f'0.{code[2:]}'
    elif code.startswith('BJ'):
        secid = f'0.{code[2:]}'
    else:
        return None
    
    url = f"https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/Page?code={secid}"
    try:
        result = subprocess.run(
            ['curl', '-s', '--max-time', '8', '-L',
             '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
             '-H', 'Referer: https://emweb.securities.eastmoney.com/',
             '-H', 'Accept: application/json',
             url],
            capture_output=True, text=True, timeout=12
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout
    except:
        pass
    return None

print(f"开始处理 {min(BATCH_SIZE, total)} 只股票...")
for i, row in enumerate(missing[:BATCH_SIZE]):
    code = row['股票代码'].strip()
    name = row['股票名称'].strip()
    
    if code in progress:
        print(f"[{i+1}/{BATCH_SIZE}] 跳过 {code} {name} (已处理)")
        continue
    
    industry = cache.get(code)
    
    if not industry:
        # 先试 F10 API
        raw = fetch_em_f10_api(code)
        if raw:
            # 尝试解析 JSON
            try:
                data = json.loads(raw)
                # 尝试从 JSON 中提取行业
                if isinstance(data, dict):
                    # 常见字段路径
                    for key in [' corpType ', ' industry ', ' industryCode ', ' mainIndustry ']:
                        # 模糊搜索
                        s = json.dumps(data)
                        m = re.search(r'["\']?(?:industry|行业)["\']\s*[:\s]+["\']?([^"\']{2,30})["\']?', s, re.IGNORECASE)
                        if m:
                            industry = m.group(1).strip()
                            break
            except:
                pass
        
        # 再试 HTML 页面
        if not industry:
            html = fetch_em_page(code)
            if html:
                industry = extract_industry_from_html(html)
        
        if industry:
            cache[code] = industry
            CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2))
    
    if industry:
        # 更新 CSV
        for r in rows:
            if r['股票代码'].strip() == code:
                r['行业大类'] = industry
                updated += 1
                break
    
    progress[code] = industry or ''
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False))
    
    print(f"[{i+1}/{BATCH_SIZE}] {code} {name} -> {industry or '未找到'}")
    time.sleep(0.5)

# 保存 CSV
with open(CSV_PATH, 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"\n完成! 本批更新 {updated} 只, 缓存 {len(cache)} 只")
print(f"CSV 已保存至: {CSV_PATH}")