#!/usr/bin/env python3
"""
通过 360 搜索引擎批量获取 A 股行业信息，补充 companies.csv 中缺失的行业数据。
使用方法: python3 fetch_industry_360.py [每批数量]
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
DELAY = 3.0
PROGRESS_FILE = '/Users/mac/Develop/gitstore/qlquant/marketResearch/output/fetch_progress.json'

def fetch_360(query, code):
    """用 curl 获取 360 搜索结果"""
    url = f'https://www.so.com/s?q={quote(query)}+所属行业&ie=utf8'
    headers = [
        'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept: text/html,application/xhtml+xml',
        'Accept-Language: zh-CN,zh;q=0.9',
        'Referer: https://www.so.com/',
    ]
    cmd = ['curl', '-s', '--max-time', '15', '-L', '-A', headers[0]]
    for h in headers[1:]:
        cmd += ['-H', h]
    cmd.append(url)
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        return result.stdout
    except Exception as e:
        print(f"  [WARN] curl 失败: {e}", file=sys.stderr)
        return ''

def extract_industry(html, stock_name):
    """从 360 搜索结果 HTML 中提取行业信息"""
    # 优先从证券之星结果中提取
    # 格式: "所属行业 房地产开发 所属地域..."
    patterns = [
        # 证券之星格式
        r'所属行业\s*([^「」\s<>，,。.、;；]+)',
        r'所属行业[:：]\s*([^\s<>，,。.、；;]+)',
        r'所属行业\s*([^「」\s<>，,。.、；;]+)',
        r'所属行业[:：]\s*([^\s<>，,。.、；;]+)',
        # 新浪格式
        r'所属指数[^\n]*?行业\s*([^\s<>，,。.、；;]+)',
        # 东财格式
        r'行业\s*</[^>]+>\s*<[^>]+>\s*([^<{}]+)',
    ]
    
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            ind = m.group(1).strip()
            # 过滤掉太短或太长的
            if 2 <= len(ind) <= 10:
                return ind
    
    # 从搜索摘要中直接匹配行业关键词
    # 先清理标签
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text)
    
    # 常见行业关键词
    industries = [
        '房地产开发', '房地产', '建筑', '建筑装饰', '建筑工程',
        '银行', '证券', '保险', '多元金融', '信托',
        '汽车整车', '汽车零部件', '汽车服务', '新能源汽车',
        '半导体', '电子元件', '电子信息', '光学光电子', '消费电子',
        '医疗', '医药生物', '医疗器械', '化学制药', '中药', '生物制品',
        '食品加工', '食品饮料', '酿酒', '饮料制造', '乳制品', '调味品',
        '家电', '白色家电', '黑色家电', '小家电', '厨卫电器',
        '通信设备', '通信服务', '电信运营', '5G', '6G',
        '软件服务', '互联网服务', '计算机设备', '信息安全', '云计算',
        '传媒', '文化传媒', '游戏', '影视', '广告', '营销',
        '环保', '固废处理', '污水处理', '大气治理',
        '化工', '化学制品', '化学原料', '新材料', '塑料',
        '钢铁', '有色金属', '煤炭', '石油', '石化', '矿业',
        '电力', '电力设备', '电网', '光伏', '风电', '储能', '电池',
        '机械设备', '通用设备', '专用设备', '工程机械', '自动化',
        '军工', '航空', '航天', '船舶', '地面兵装',
        '纺织', '服装', '化妆品', '饰品',
        '轻工制造', '造纸', '包装',
        '交通运输', '物流', '港口', '公路', '铁路', '航空机场',
        '零售', '百货', '超市', '专业连锁', '电商',
        '农业', '种植业', '渔业', '林业', '畜牧',
        '综合', '公用事业', '燃气', '水务', '电力',
        '金属制品', '家具', '装修建材', '房地产服务',
        '化学纤维', '橡胶', '塑料',
        '电机', '电气自动化', '电源设备', '特高压',
        '采掘', '黄金', '稀土', '钴', '锂',
        '印制电路板', '光学', 'LED', '安防',
        '白色家电', '小家电', '厨卫电器', '专业服务', '酒店旅游',
    ]
    
    for ind in industries:
        if ind in text:
            return ind
    
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
    return {'last_index': 0}

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
    print("360搜索行业信息批量获取工具")
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
    
    to_fetch = [s for s in missing if s['code'] not in cache]
    print(f"已有缓存: {len(cache)} 条, 需要获取: {len(to_fetch)} 条")
    print(f"从第 {last_index + 1} 条继续 (索引 {last_index})")
    
    to_fetch = to_fetch[last_index:]
    
    total_count = len(to_fetch)
    if total_count == 0:
        print("全部已有缓存，使用缓存更新 CSV。")
        updates = {code: {'industry': info} for code, info in cache.items() if info}
        update_csv(updates)
        print(f"已更新 {len(updates)} 条行业信息。")
        return
    
    print(f"本次需处理: {total_count} 条")
    print("-" * 60)
    
    updates = {}
    
    for idx, stock in enumerate(to_fetch):
        code = stock['code']
        name = stock['name']
        global_idx = last_index + idx + 1
        
        if code in cache:
            industry = cache[code]
        else:
            query = f'{code}+{name}'
            print(f"[{global_idx}/{total_count + last_index}] {code} {name}...", end='', flush=True)
            html = fetch_360(query, code)
            industry = extract_industry(html, name)
            cache[code] = industry
            print(f" -> {industry if industry else '未找到'}")
        
        if industry:
            updates[code] = {'industry': industry}
        
        # 每50只保存一次
        if (idx + 1) % 50 == 0:
            save_cache(cache)
            if updates:
                update_csv(updates)
            save_progress({'last_index': last_index + idx + 1})
            print(f"\n  [进度] 已处理 {idx + 1}/{total_count}, 本次更新 {len(updates)} 条")
            print("-" * 60)
        
        time.sleep(DELAY + random.uniform(0, 1.5))
    
    # 最终保存
    save_cache(cache)
    if updates:
        update_csv(updates)
    save_progress({'last_index': last_index + total_count})
    
    print(f"\n完成! 累计缓存 {len(cache)} 条, 本次更新 {len(updates)} 条")
    print(f"缓存: {CACHE_PATH}")
    print(f"CSV: {CSV_PATH}")

if __name__ == '__main__':
    main()
