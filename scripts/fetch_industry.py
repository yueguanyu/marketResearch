#!/usr/bin/env python3
"""
从东方财富 API 批量获取 A 股行业信息，补充 companies.csv 中缺失的行业数据。
使用方法: python3 fetch_industry.py
"""
import csv
import json
import subprocess
import time
import sys
import os
from collections import defaultdict

CSV_PATH = '/Users/mac/Develop/gitstore/qlquant/marketResearch/output/companies.csv'
CACHE_PATH = '/Users/mac/Develop/gitstore/qlquant/marketResearch/output/industry_cache.json'
BATCH_SIZE = 50  # 每批获取数量
DELAY = 0.5       # 请求间隔（秒）

def curl_json(url):
    """用 curl 获取 JSON 数据"""
    headers = [
        'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Referer: https://www.eastmoney.com/',
    ]
    cmd = ['curl', '-s', '--max-time', '15']
    for h in headers:
        cmd += ['-H', h]
    cmd.append(url)
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        return json.loads(result.stdout)
    except Exception as e:
        print(f"  [WARN] 请求失败: {e}", file=sys.stderr)
        return None

def get_market_code(code):
    """根据代码判断市场 (sh/sz)"""
    code = code.strip()
    if code.startswith('6') or code.startswith('5') or code.startswith('9'):
        return f'1.{code}'  # 上交所
    elif code.startswith('8') or code.startswith('4'):
        return f'1.{code}'  # 北交所
    else:
        return f'0.{code}'  # 深交所

def fetch_industry_batch(codes):
    """批量获取股票行业信息（通过东方财富单只股票API）"""
    results = {}
    
    for code in codes:
        mkt_code = get_market_code(code)
        url = (f'https://push2.eastmoney.com/api/qt/stock/get'
               f'?ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2'
               f'&fields=f12,f14,f100&secid={mkt_code}')
        
        data = curl_json(url)
        if data and data.get('data'):
            d = data['data']
            results[code] = {
                'name': d.get('f14', ''),
                'industry': d.get('f100', '')
            }
        else:
            results[code] = {'name': '', 'industry': ''}
        
        time.sleep(DELAY)
        
        if len(results) % 10 == 0:
            print(f"  已处理 {len(results)}/{len(codes)}", file=sys.stderr)
    
    return results

def load_cache():
    """加载已有缓存"""
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    """保存缓存"""
    with open(CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def load_missing_stocks():
    """加载缺行业的股票"""
    missing = []
    with open(CSV_PATH, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get('股票代码', '').strip().strip('"')
            industry = row.get('行业大类', '').strip().strip('"')
            name = row.get('股票名称', '').strip().strip('"')
            if code and not industry:
                missing.append({'code': code, 'name': name})
    return missing

def update_csv(updates):
    """更新 CSV 文件"""
    rows = []
    with open(CSV_PATH, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            code = row.get('股票代码', '').strip().strip('"')
            if code in updates:
                row['行业大类'] = updates[code].get('industry', '')
            rows.append(row)
    
    with open(CSV_PATH, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main():
    print("=" * 60)
    print("东方财富行业信息批量获取工具")
    print("=" * 60)
    
    # 加载缺失行业的股票
    missing = load_missing_stocks()
    print(f"缺行业股票总数: {len(missing)}")
    
    if not missing:
        print("没有缺行业的股票，退出。")
        return
    
    # 加载缓存
    cache = load_cache()
    print(f"已有缓存: {len(cache)} 条")
    
    # 过滤掉已经有缓存的
    to_fetch = [s for s in missing if s['code'] not in cache]
    print(f"需要获取: {len(to_fetch)} 条")
    
    if not to_fetch:
        print("全部已有缓存，使用缓存更新 CSV。")
        updates = {code: {'industry': info['industry']} for code, info in cache.items()}
        update_csv(updates)
        print(f"已更新 {len(updates)} 条行业信息到 CSV。")
        return
    
    # 分批获取
    total_batches = (len(to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"分 {total_batches} 批获取，每批 {BATCH_SIZE} 条，间隔 {DELAY}秒")
    print("-" * 60)
    
    for i in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"\n第 {batch_num}/{total_batches} 批 (股票 {batch[0]['code']} ~ {batch[-1]['code']})")
        
        codes = [s['code'] for s in batch]
        results = fetch_industry_batch(codes)
        
        # 更新缓存
        cache.update(results)
        save_cache(cache)
        
        # 实时更新 CSV（每批完成后）
        updates = {code: {'industry': info['industry']} 
                   for code, info in results.items() if info['industry']}
        if updates:
            update_csv(updates)
            print(f"  本批更新 {len(updates)} 条到 CSV (累计缓存 {len(cache)} 条)")
        
        if batch_num < total_batches:
            print(f"  等待 {DELAY*2} 秒...")
            time.sleep(DELAY * 2)
    
    print("\n" + "=" * 60)
    print(f"完成! 共获取 {len(cache)} 条行业信息")
    print(f"缓存已保存到: {CACHE_PATH}")
    print("=" * 60)

if __name__ == '__main__':
    main()
