#!/usr/bin/env python3
"""
Stock Industry Classification Update Script

Fetches stock data from akshare and maintains industry classification
in output/companies.csv.

Usage:
    python update_industry_class.py --fetch          # Fetch latest stock list from akshare
    python update_industry_class.py --missing       # Update stocks without classification
    python update_industry_class.py --stock 688411  # Update single stock
    python update_industry_class.py --all            # Update all stocks (requires network)
"""

import argparse
import csv
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

# ==================== Industry Classification Rules ====================

LARGE_KEYWORDS = {
    "农业": ["农业", "种植", "养殖", "畜牧", "渔", "林业", "种子"],
    "采掘": ["采掘", "石油", "煤炭", "天然气", "矿山", "矿产"],
    "制造业": ["制造", "加工", "生产", "机械", "设备"],
    "水电煤气": ["电力", "水务", "燃气", "水电", "核电", "火电"],
    "建筑业": ["建筑", "施工", "土木", "工程", "装修"],
    "批发和零售业": ["零售", "批发", "贸易", "百货", "超市", "电商"],
    "交通运输、仓储和邮政业": ["运输", "物流", "航运", "航空", "铁路", "港口", "快递"],
    "住宿和餐饮业": ["酒店", "餐饮", "旅游"],
    "信息传输、软件和信息技术服务业": ["软件", "信息技术", "IT", "互联网", "计算机", "通信"],
    "金融保险": ["银行", "证券", "保险", "金融", "信托", "基金", "期货"],
    "房地产业": ["房地产", "地产", "物业", "园区"],
    "租赁和商务服务业": ["租赁", "商务服务"],
    "科学研究和技术服务业": ["科研", "研究", "技术"],
    "水利、环境和公共设施管理业": ["环保", "水务处理", "环境治理"],
    "居民服务、修理和其他服务业": ["居民服务", "修理"],
    "教育": ["教育", "培训"],
    "卫生和社会工作": ["医疗", "医院", "卫生"],
    "文化、体育和娱乐业": ["传媒", "文化", "体育", "娱乐", "出版"],
    "综合": ["综合"],
    "食品饮料": ["食品", "饮料", "白酒", "酒"],
    "纺织服装": ["纺织", "服装", "家纺"],
    "木材家具": ["木材", "家具"],
    "造纸印刷": ["造纸", "印刷"],
    "石化塑胶": ["化工", "石化", "塑料", "橡胶", "化纤"],
    "电子": ["电子", "芯片", "半导体", "集成电路"],
    "金融非金属": ["非金属", "建材"],
    "机械设备": ["机械", "设备"],
    "医药生物": ["医药", "生物", "中药", "西药"],
}

SMALL_KEYWORDS = {
    "银行": ["银行"],
    "证券": ["证券"],
    "保险": ["保险"],
    "电子元件": ["电子元件", "芯片", "半导体", "集成电路"],
    "电子设备": ["电子设备", "储能", "电池", "锂电池", "电源"],
    "光学光电": ["光学", "光电", "显示", "面板", "镜头"],
    "通信设备": ["通信设备", "基站", "光通信", "5G", "6G"],
    "软件开发": ["软件开发", "软件"],
    "IT服务": ["IT服务", "系统集成"],
    "互联网服务": ["互联网", "电商", "平台"],
    "化学制品": ["化学制品", "化工", "化学品", "新材料"],
    "通用机械": ["通用机械", "机械"],
    "专用设备": ["专用设备", "设备"],
    "医疗器械": ["医疗器械", "医疗设备"],
    "生物制品": ["生物制品", "生物医药", "疫苗"],
    "中药": ["中药", "中成药"],
    "西药": ["西药", "化学制药"],
    "食品制造": ["食品制造", "粮油"],
    "饮料制造": ["饮料", "酒", "白酒"],
    "造纸": ["造纸", "印刷"],
    "环保工程": ["环保", "水处理"],
    "金属制品": ["金属制品", "合金", "新材料"],
    "商业物业": ["商业物业", "租赁"],
    "专业连锁": ["专业连锁", "零售"],
}

HOT_TOPICS = [
    "人工智能", "AI", "机器学习", "深度学习",
    "新能源", "光伏", "风电", "储能", "氢能", "锂电池",
    "芯片", "半导体", "集成电路", "芯片设计", "芯片制造",
    "生物医药", "生物制药", "创新药", "疫苗", "基因工程",
    "新材料", "纳米材料", "复合材料", "超导材料",
    "5G", "6G", "通信技术", "物联网", "云计算", "大数据",
    "区块链", "自动驾驶", "新能源汽车", "智能汽车",
    "军工", "国防", "航空航天",
    "碳中和", "环保", "节能减排", "数字经济",
    "产业互联网", "工业4.0", "元宇宙", "虚拟现实", "VR", "AR",
]


# ==================== Classification Functions ====================

def classify_by_keywords(business_desc: str) -> tuple:
    """
    Classify stock based on business description keywords.

    Returns:
        (industry_large, industry_small, hot_fields)
    """
    text = business_desc.lower() if business_desc else ""
    industry_large = "制造业"
    industry_small = "通用机械"
    hot_fields = []

    # Electronics/Semiconductors
    if any(k in text for k in ["芯片", "半导体", "集成电路", "晶圆", "微电子"]):
        industry_large = "电子"
        industry_small = "电子元件"
        hot_fields = ["芯片", "半导体"]

    elif any(k in text for k in ["光刻", "封测", "功率", "mosfet", "igbt", "fpga"]):
        industry_large = "电子"
        industry_small = "电子元件"
        hot_fields = ["芯片", "半导体"]

    # Communications/5G/IoT
    elif any(k in text for k in ["通信", "5g", "6g", "无线", "物联网", "射频", "光通信"]):
        industry_large = "信息传输、软件和信息技术服务业"
        industry_small = "通信设备"
        hot_fields = ["5G", "物联网"]

    # Software/IT
    elif any(k in text for k in ["软件", "开发", "操作系统", "数据库", "信息化", "saas"]):
        industry_large = "信息传输、软件和信息技术服务业"
        industry_small = "软件开发"
        hot_fields = ["软件"]
        if "人工智能" in text or "ai" in text:
            hot_fields.append("人工智能")

    # Biotech/Pharma
    elif any(k in text for k in ["医药", "制药", "药业", "生物", "疫苗", "抗体", "基因"]):
        industry_large = "医药生物"
        if "中药" in text:
            industry_small = "中药"
            hot_fields = ["中药"]
        elif any(k in text for k in ["生物", "疫苗", "抗体", "基因"]):
            industry_small = "生物制品"
            hot_fields = ["生物医药"]
        else:
            industry_small = "西药"
            hot_fields = ["医药"]

    elif any(k in text for k in ["医疗", "器械", "诊断", "检测", "影像", "手术"]):
        industry_large = "医药生物"
        industry_small = "医疗器械"
        hot_fields = ["医疗器械"]

    # New Energy/Solar/Storage
    elif any(k in text for k in ["光伏", "太阳能", "组件", "逆变器"]):
        industry_large = "制造业"
        industry_small = "专用设备"
        hot_fields = ["新能源", "光伏"]

    elif any(k in text for k in ["储能", "电池", "锂电池", "动力电池"]):
        industry_large = "制造业"
        industry_small = "电子设备"
        hot_fields = ["新能源", "储能"]

    elif any(k in text for k in ["风电", "风力", "风机"]):
        industry_large = "制造业"
        industry_small = "专用设备"
        hot_fields = ["新能源", "风电"]

    # Finance
    elif any(k in text for k in ["银行", "储蓄", "信贷"]):
        industry_large = "金融保险"
        industry_small = "银行"
        hot_fields = ["金融", "银行"]

    elif any(k in text for k in ["证券", "经纪", "承销"]):
        industry_large = "金融保险"
        industry_small = "证券"
        hot_fields = ["金融", "证券"]

    # Food/Beverage
    elif any(k in text for k in ["酒", "白酒", "啤酒", "葡萄酒"]):
        industry_large = "食品饮料"
        industry_small = "饮料制造"
        hot_fields = ["白酒"]

    elif any(k in text for k in ["食品", "预制菜", "肉制品", "调味品"]):
        industry_large = "食品饮料"
        industry_small = "食品制造"
        hot_fields = ["食品"]

    # Chemical/New Materials
    elif any(k in text for k in ["化工", "化学", "塑料", "橡胶", "涂料"]):
        industry_large = "石化塑胶"
        industry_small = "化学制品"
        hot_fields = ["化工"] if "新材料" not in text else ["新材料"]

    elif any(k in text for k in ["新材料", "纳米", "碳纤维", "复合材料", "合金"]):
        industry_large = "制造业"
        industry_small = "金属制品"
        hot_fields = ["新材料"]

    # Military/Aerospace
    elif any(k in text for k in ["军工", "国防", "航空航天", "军品", "导弹"]):
        industry_large = "制造业"
        industry_small = "专用设备"
        hot_fields = ["军工", "国防", "航空航天"]

    return industry_large, industry_small, ", ".join(hot_fields) if hot_fields else ""


def search_company_info(symbol: str, name: str) -> dict:
    """
    Search company information via network (Agent Browser).

    This is a STUB - implementation pending agent-browser integration.

    Args:
        symbol: Stock code
        name: Company name

    Returns:
        dict with keys: business_desc, industry_large, industry_small, hot_fields
    """
    # TODO: Implement with agent-browser
    # Expected implementation:
    # 1. Use browser agent to search for "{name} {symbol} 主营业务"
    # 2. Extract business description from search results
    # 3. Classify based on extracted information

    return {
        "business_desc": "",
        "industry_large": "",
        "industry_small": "",
        "hot_fields": "",
    }


def classify_by_name(name: str) -> tuple:
    """
    Simple classification based only on stock name (fallback).

    Returns:
        (industry_large, industry_small, hot_fields)
    """
    return classify_by_keywords(name)


# ==================== Data Loading/Saving ====================

def load_companies_csv(filepath: str) -> list:
    """Load companies.csv into list of dicts."""
    companies = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            companies.append(row)
    return companies


def save_companies_csv(filepath: str, companies: list, fieldnames: list):
    """Save companies list to CSV."""
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(companies)


# ==================== Akshare Data Fetching ====================

def fetch_a_stocks() -> pd.DataFrame:
    """Fetch A-share stock list from akshare."""
    try:
        import akshare as ak
        print("Fetching A-share stocks from akshare...")
        df = ak.stock_info_a_code_name()
        df = df.rename(columns={"symbol": "股票代码", "name": "股票名称"})
        df["市场"] = "A股"
        print(f"  Fetched {len(df)} A-share stocks")
        return df[["股票代码", "股票名称", "市场"]]
    except ImportError:
        print("  akshare not installed, skipping A-share fetch")
        return pd.DataFrame()


def fetch_hk_stocks() -> pd.DataFrame:
    """Fetch HK stocks from akshare."""
    try:
        import akshare as ak
        print("Fetching HK stocks from akshare...")
        df = ak.stock_hk_spot_em()
        df = df.rename(columns={"代码": "股票代码", "中文名称": "股票名称"})
        df["市场"] = "港股"
        print(f"  Fetched {len(df)} HK stocks")
        return df[["股票代码", "股票名称", "市场"]]
    except ImportError:
        print("  akshare not installed or failed, skipping HK fetch")
        return pd.DataFrame()


def fetch_index_components() -> pd.DataFrame:
    """Fetch index constituent stocks from akshare."""
    try:
        import akshare as ak
        indices = {
            "上证50": "000016",
            "沪深300": "000300",
            "深证成指": "399001",
            "创业板指": "399006",
        }
        all_components = []
        for name, code in indices.items():
            try:
                print(f"Fetching {name} components...")
                df = ak.index_stock_cons(symbol=code)
                df["index_name"] = name
                all_components.append(df)
                print(f"  Fetched {len(df)} stocks for {name}")
            except Exception as e:
                print(f"  Failed to fetch {name}: {e}")
        if all_components:
            return pd.concat(all_components, ignore_index=True)
        return pd.DataFrame()
    except ImportError:
        print("  akshare not installed, skipping index fetch")
        return pd.DataFrame()


def fetch_all_stocks() -> pd.DataFrame:
    """Fetch all stocks (A-share + HK)."""
    stocks = []

    a_stocks = fetch_a_stocks()
    if not a_stocks.empty:
        stocks.append(a_stocks)

    hk_stocks = fetch_hk_stocks()
    if not hk_stocks.empty:
        stocks.append(hk_stocks)

    if stocks:
        return pd.concat(stocks, ignore_index=True)
    return pd.DataFrame()


# ==================== Update Functions ====================

def update_single_stock(symbol: str, name: str, market: str = "A股") -> dict:
    """Update classification for a single stock."""
    print(f"\nProcessing: {symbol} {name}")

    # Try network search first
    info = search_company_info(symbol, name)

    if info["business_desc"]:
        print(f"  Business: {info['business_desc'][:60]}...")
        print(f"  Classification: {info['industry_large']} / {info['industry_small']} / {info['hot_fields']}")
    else:
        # Fallback to name-based classification
        print(f"  No network info, using name-based classification")
        industry_large, industry_small, hot_fields = classify_by_name(name)
        info["industry_large"] = industry_large
        info["industry_small"] = industry_small
        info["hot_fields"] = hot_fields

    return info


def merge_with_existing(new_stocks: pd.DataFrame, existing_path: str) -> list:
    """Merge new stock list with existing classifications."""
    existing = {}
    if os.path.exists(existing_path):
        existing_companies = load_companies_csv(existing_path)
        for c in existing_companies:
            existing[c["股票代码"]] = c

    fieldnames = ["股票代码", "股票名称", "市场", "行业大类", "行业小类", "热点领域"]
    merged = []

    for _, row in new_stocks.iterrows():
        code = str(row["股票代码"])
        if code in existing:
            merged.append(existing[code])
        else:
            merged.append({
                "股票代码": code,
                "股票名称": row["股票名称"],
                "市场": row["市场"],
                "行业大类": "",
                "行业小类": "",
                "热点领域": "",
            })

    return merged, fieldnames


# ==================== Main CLI ====================

def main():
    parser = argparse.ArgumentParser(
        description="Stock industry classification updater"
    )
    parser.add_argument("--fetch", action="store_true", help="Fetch latest stock list from akshare")
    parser.add_argument("--missing", action="store_true", help="Update stocks without classification")
    parser.add_argument("--all", action="store_true", help="Update all stocks (requires network)")
    parser.add_argument("--stock", type=str, help="Update single stock by code")
    parser.add_argument("--output", default="output/companies.csv", help="Output CSV path")

    args = parser.parse_args()

    os.makedirs("output", exist_ok=True)

    if args.fetch:
        # Fetch stock list and merge with existing
        new_stocks = fetch_all_stocks()
        if new_stocks.empty:
            print("No stocks fetched, check akshare installation")
            return

        merged, fieldnames = merge_with_existing(new_stocks, args.output)
        save_companies_csv(args.output, merged, fieldnames)
        print(f"\nUpdated stock list: {len(merged)} stocks")
        return

    if args.stock:
        info = update_single_stock(args.stock, args.stock)
        print(f"\nResult: {info}")
        return

    if args.missing:
        if not os.path.exists(args.output):
            print(f"File not found: {args.output}")
            return

        companies = load_companies_csv(args.output)
        fieldnames = ["股票代码", "股票名称", "市场", "行业大类", "行业小类", "热点领域"]

        updated = 0
        for i, company in enumerate(companies):
            if not company.get("行业大类"):
                code = company["股票代码"]
                name = company["股票名称"]
                print(f"\n[{i+1}/{len(companies)}] Unclassified: {code} {name}")
                info = update_single_stock(code, name, company.get("市场", "A股"))
                company["行业大类"] = info.get("industry_large", "")
                company["行业小类"] = info.get("industry_small", "")
                company["热点领域"] = info.get("hot_fields", "")

                if (updated + 1) % 10 == 0:
                    save_companies_csv(args.output, companies, fieldnames)
                    print("  Progress saved")

                updated += 1
                time.sleep(0.3)

        save_companies_csv(args.output, companies, fieldnames)
        print(f"\nDone! Updated {updated} stocks")
        return

    if args.all:
        if not os.path.exists(args.output):
            print(f"File not found: {args.output}")
            return

        companies = load_companies_csv(args.output)
        fieldnames = ["股票代码", "股票名称", "市场", "行业大类", "行业小类", "热点领域"]

        for i, company in enumerate(companies):
            code = company["股票代码"]
            name = company["股票名称"]
            print(f"\n[{i+1}/{len(companies)}] {code} {name}")
            info = update_single_stock(code, name, company.get("市场", "A股"))
            company["行业大类"] = info.get("industry_large", "")
            company["行业小类"] = info.get("industry_small", "")
            company["热点领域"] = info.get("hot_fields", "")

            if (i + 1) % 10 == 0:
                save_companies_csv(args.output, companies, fieldnames)
                print("  Progress saved")

            time.sleep(0.3)

        save_companies_csv(args.output, companies, fieldnames)
        print(f"\nDone! Processed {len(companies)} stocks")
        return

    parser.print_help()
    print("\nExamples:")
    print("  python update_industry_class.py --fetch          # Fetch latest stock list")
    print("  python update_industry_class.py --missing       # Update unclassified stocks")
    print("  python update_industry_class.py --stock 688411  # Update single stock")


if __name__ == "__main__":
    main()
