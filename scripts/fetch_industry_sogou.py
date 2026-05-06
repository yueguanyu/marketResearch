#!/usr/bin/env python3
"""
通过搜狗搜索引擎批量获取 A 股行业信息，补充 companies.csv 中缺失的行业数据。
使用方法: python3 fetch_industry_sogou.py [每批数量]
"""
import csv
import json
import re
import subprocess
import time
import sys
import os
import random
from urllib.parse import quote

CSV_PATH = '/Users/mac/Develop/gitstore/qlquant/marketResearch/output/companies.csv'
CACHE_PATH = '/Users/mac/Develop/gitstore/qlquant/marketResearch/output/industry_cache.json'
BATCH_SIZE = int(sys.argv[1]) if len(sys.argv) > 1 else 50
DELAY = 2.0  # 请求间隔（秒）
PROGRESS_FILE = '/Users/mac/Develop/gitstore/qlquant/marketResearch/output/fetch_progress.json'

def curl_sogou(query, stock_code):
    """用 curl 获取搜狗搜索结果，解析行业信息"""
    url = f'https://www.sogou.com/web?query={quote(stock_code)}+{quote(query)}+行业&ie=utf8'
    headers = [
        'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language: zh-CN,zh;q=0.9',
        'Referer: https://www.sogou.com/',
    ]
    cmd = ['curl', '-s', '--max-time', '15', '-L']
    for h in headers:
        cmd += ['-H', h]
    cmd.append(url)
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        return result.stdout
    except Exception as e:
        print(f"  [WARN] curl 失败: {e}", file=sys.stderr)
        return ''

def extract_industry_from_sogou(html, stock_name):
    """从搜狗搜索结果 HTML 中提取行业信息"""
    # 方法1: 搜索 "中财网" 结果，它显示"所属行业"
    # 中财网格式: 所属行业 房地产业
    patterns = [
        r'所属行业[:：]\s*([^\s<>，。,，、；;]+)',
        r'所属行业[:：]\s*<[^>]+>([^<]+)<',
        r'行业分类[:：]\s*([^\s<>，。,，、；;]+)',
        r'所处行业[:：]\s*([^\s<>，。,，、；;]+)',
        r'主营行业[:：]\s*([^\s<>，。,，、；;]+)',
    ]
    
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            return m.group(1).strip()
    
    # 方法2: 从百度/东方财富股票详情页URL中提取
    # 搜索结果中的板块链接
    board_pattern = r'boards2?-?\d*\.BK\d+["\']'
    boards = re.findall(r'boards2?-?\d*\.BK(\d+)', html)
    if boards:
        # BKxxxx 是东方财富板块代码，需要映射到行业名
        return f'板块{boards[0]}'
    
    # 方法3: 搜索结果摘要中包含行业关键词
    industry_keywords = [
        '银行业', '房地产业', '制造业', '建筑业', '信息传输', '软件和信息服务',
        '批发和零售业', '交通运输', '仓储和邮政业', '住宿和餐饮业',
        '金融业', '租赁和商务服务业', '科学研究和技术服务业',
        '水利、环境和公共设施管理业', '居民服务、修理和其他服务业',
        '教育', '卫生和社会工作', '文化、体育和娱乐业',
        '公共管理、社会保障和社会组织', '农、林、牧、渔业',
        '采矿业', '电力、热力、燃气及水生产和供应业',
        '上市公司', '有限公司', '股份公司',
        '半导体', '电子', '医药', '医疗', '新能源', '汽车',
        '家电', '食品', '饮料', '酿酒', '纺织', '服装',
        '化工', '钢铁', '有色金属', '煤炭', '石油', '石化',
        '机械设备', '电气机械', '汽车制造', '铁路', '船舶',
        '航空', '航天', '军工', '环保', '安防', '通信',
        '计算机', '软件', '互联网', '文化传媒', '游戏',
        '装修', '房地产', '开发', '物业管理', '中介',
        '证券', '基金', '保险', '信托', '银行',
    ]
    
    # 在搜索结果摘要中找行业关键词
    summary_pattern = r'"摘要"[^<]*<[^>]+>([^<]{10,100})<'
    summaries = re.findall(summary_pattern, html)
    for s in summaries:
        for kw in industry_keywords:
            if kw in s:
                return kw
    
    return ''

def get_market_code(code):
    """根据代码判断市场前缀"""
    code = code.strip()
    if code.startswith('6') or code.startswith('5') or code.startswith('9'):
        return 'sh'
    elif code.startswith('8') or code.startswith('4'):
        return 'bj'
    else:
        return 'sz'

def fetch_industry_for_stock(code, name):
    """获取单只股票的行业信息"""
    market = get_market_code(code)
    
    # 搜索查询
    queries = [
        f'{code}+{name}+所属行业+site:cfi.cn',
        f'{code}+{name}+所属行业',
        f'{name}+所属行业',
        f'{code}+{name}+行业分类',
    ]
    
    for query in queries:
        html = curl_sogou(query.replace(f'{code}+', '').replace(f'+{name}', ''), f'{code}+{name}')
        if not html:
            continue
        
        industry = extract_industry_from_sogou(html, name)
        if industry:
            return industry
        
        time.sleep(DELAY)
    
    return ''

def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'last_index': 0, 'updated': 0}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False)

def load_missing_stocks():
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
    rows = []
    with open(CSV_PATH, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            code = row.get('股票代码', '').strip().strip('"')
            if code in updates and updates[code].get('industry'):
                row['行业大类'] = updates[code]['industry']
            rows.append(row)
    
    with open(CSV_PATH, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main():
    print("=" * 60)
    print("搜狗搜索行业信息批量获取工具")
    print(f"每批处理 {BATCH_SIZE} 只, 间隔 {DELAY}秒")
    print("=" * 60)
    
    missing = load_missing_stocks()
    print(f"缺行业股票总数: {len(missing)}")
    
    if not missing:
        print("没有缺行业的股票。")
        return
    
    cache = load_cache()
    progress = load_progress()
    last_index = progress.get('last_index', 0)
    
    # 过滤已有缓存的
    to_fetch = [s for s in missing if s['code'] not in cache]
    print(f"已有缓存: {len(cache)} 条, 需要获取: {len(to_fetch)} 条")
    print(f"从第 {last_index + 1} 条继续...")
    
    # 截取待处理部分
    to_fetch = to_fetch[last_index:]
    total_batches = (len(to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"本次分 {total_batches} 批")
    print("-" * 60)
    
    updates = {}
    
    for i in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        for j, stock in enumerate(batch):
            code = stock['code']
            name = stock['name']
            idx = last_index + i + j + 1
            
            if code in cache:
                industry = cache[code]
            else:
                print(f"[{idx}/{len(to_fetch) + last_index}] {code} {name}...", end='', flush=True)
                industry = fetch_industry_for_stock(code, name)
                cache[code] = industry
                print(f" -> {industry if industry else '未找到'}")
            
            if industry:
                updates[code] = {'industry': industry}
            
            # 随机延迟 避免被限流
            time.sleep(DELAY + random.uniform(0, 1))
        
        # 每批完成后保存进度
        save_cache(cache)
        if updates:
            update_csv(updates)
            save_progress({'last_index': last_index + i + BATCH_SIZE, 'updated': progress.get('updated', 0) + len(updates)})
        
        print(f"\n第 {batch_num}/{total_batches} 批完成, 本批更新 {len([u for u in updates if u in [b['code'] for b in batch]])} 条")
        print("-" * 60)
        
        if batch_num < total_batches:
            time.sleep(DELAY * 2)
    
    print(f"\n完成! 累计缓存 {len(cache)} 条, 本次更新 {len(updates)} 条")
    print(f"缓存: {CACHE_PATH}")
    print(f"CSV: {CSV_PATH}")

if __name__ == '__main__':
    main()
