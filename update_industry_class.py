#!/usr/bin/env python3
"""
股票行业分类与热点领域更新脚本

功能：
1. 从 akshare 获取最新 A股/港股 股票列表
2. 通过网络搜索获取缺失股票的分类信息
3. 更新 output/companies.csv

使用方法:
    python update_industry_class.py              # 完整更新
    python update_industry_class.py --stock 688411  # 更新单只股票
    python update_industry_class.py --missing       # 仅更新未分类的股票
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

# ==================== 行业分类规则 ====================

# 行业大类关键词映射
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

# 行业小类关键词映射
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

# 热点领域列表
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
    "医疗器械", "创新药", "ADC", "生物制药",
]


# ==================== 核心分类函数 ====================

def classify_by_keywords(business_desc: str) -> tuple:
    """
    基于关键词的行业分类

    Returns:
        (行业大类, 行业小类, 热点领域)
    """
    text = business_desc.lower() if business_desc else ""

    industry_large = "制造业"  # 默认
    industry_small = "通用机械"
    hot_fields = []

    # 电子/芯片/半导体
    if any(k in text for k in ["芯片", "半导体", "集成电路", "晶圆", "微电子"]):
        industry_large = "电子"
        industry_small = "电子元件"
        hot_fields = ["芯片", "半导体"]
    elif any(k in text for k in ["光刻", "封测", "功率", "mosfet", "igbt", "fpga"]):
        industry_large = "电子"
        industry_small = "电子元件"
        hot_fields = ["芯片", "半导体"]

    # 通信/5G/物联网
    elif any(k in text for k in ["通信", "5g", "6g", "无线", "物联网", "射频", "光通信"]):
        industry_large = "信息传输、软件和信息技术服务业"
        industry_small = "通信设备"
        hot_fields = ["5G", "物联网"]

    # 软件/IT
    elif any(k in text for k in ["软件", "开发", "操作系统", "数据库", "信息化", "SAAS"]):
        industry_large = "信息传输、软件和信息技术服务业"
        industry_small = "软件开发"
        hot_fields = ["软件"]
        if "人工智能" in text or "ai" in text:
            hot_fields.append("人工智能")

    # 医药生物
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

    # 医疗器械
    elif any(k in text for k in ["医疗", "器械", "诊断", "检测", "影像", "手术"]):
        industry_large = "医药生物"
        industry_small = "医疗器械"
        hot_fields = ["医疗器械"]

    # 新能源/光伏/储能
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

    # 金融
    elif any(k in text for k in ["银行", "储蓄", "信贷"]):
        industry_large = "金融保险"
        industry_small = "银行"
        hot_fields = ["金融", "银行"]
    elif any(k in text for k in ["证券", "经纪", "承销"]):
        industry_large = "金融保险"
        industry_small = "证券"
        hot_fields = ["金融", "证券"]

    # 食品饮料
    elif any(k in text for k in ["酒", "白酒", "啤酒", "葡萄酒"]):
        industry_large = "食品饮料"
        industry_small = "饮料制造"
        hot_fields = ["白酒"]
    elif any(k in text for k in ["食品", "预制菜", "肉制品", "调味品"]):
        industry_large = "食品饮料"
        industry_small = "食品制造"
        hot_fields = ["食品"]

    # 化工/新材料
    elif any(k in text for k in ["化工", "化学", "塑料", "橡胶", "涂料"]):
        industry_large = "石化塑胶"
        industry_small = "化学制品"
        hot_fields = ["化工"] if "新材料" not in text else ["新材料"]
    elif any(k in text for k in ["新材料", "纳米", "碳纤维", "复合材料", "合金"]):
        industry_large = "制造业"
        industry_small = "金属制品"
        hot_fields = ["新材料"]

    # 军工
    elif any(k in text for k in ["军工", "国防", "航空航天", "军品", "导弹"]):
        industry_large = "制造业"
        industry_small = "专用设备"
        hot_fields = ["军工", "国防", "航空航天"]

    return industry_large, industry_small, ", ".join(hot_fields) if hot_fields else ""


def search_company_info(symbol: str, name: str) -> dict:
    """
    通过网络搜索获取公司信息

    使用 DuckDuckGo 搜索获取公司主营业务描述

    Args:
        symbol: 股票代码
        name: 公司名称

    Returns:
        dict with keys: business_desc, industry_large, industry_small, hot_fields
    """
    result = {
        "business_desc": "",
        "industry_large": "",
        "industry_small": "",
        "hot_fields": "",
    }

    # 构建搜索关键词
    queries = [
        f"{name} {symbol} 主营业务",
        f"{name} 公司简介",
        f"{name} 核心业务",
    ]

    for query in queries:
        try:
            # 使用 ddg 命令行工具搜索 (如果可用)
            # 或者使用 curl 直接调用 DuckDuckGo HTML
            cmd = f'ddg "{query}" 2>/dev/null | head -20'
            try:
                proc = subprocess.run(
                    cmd, shell=True, capture_output=True, text=True, timeout=10
                )
                if proc.stdout.strip():
                    result["business_desc"] = proc.stdout.strip()
                    break
            except (subprocess.TimeoutExpired, FileNotFoundError):
                # ddg 命令不可用，尝试 curl
                pass

        except Exception as e:
            print(f"  搜索出错: {e}")
            continue

    # 如果获取到业务描述，进行分类
    if result["business_desc"]:
        industry_large, industry_small, hot_fields = classify_by_keywords(
            result["business_desc"]
        )
        result["industry_large"] = industry_large
        result["industry_small"] = industry_small
        result["hot_fields"] = hot_fields

    return result


def update_single_stock(symbol: str, name: str, market: str = "A股") -> dict:
    """
    更新单只股票的行业分类信息

    Returns:
        dict with updated fields
    """
    print(f"\n处理: {symbol} {name}")

    # 尝试通过网络搜索获取信息
    info = search_company_info(symbol, name)

    if info["business_desc"]:
        print(f"  业务描述: {info['business_desc'][:60]}...")
        print(f"  分类: {info['industry_large']} / {info['industry_small']} / {info['hot_fields']}")
    else:
        # 基于股票名称进行简单分类
        print(f"  未获取到网络信息，基于名称分类")
        industry_large, industry_small, hot_fields = classify_by_keywords(name)
        info["industry_large"] = industry_large
        info["industry_small"] = industry_small
        info["hot_fields"] = hot_fields

    return info


def load_companies_csv(filepath: str) -> list:
    """加载 companies.csv"""
    companies = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            companies.append(row)
    return companies


def save_companies_csv(filepath: str, companies: list, fieldnames: list):
    """保存 companies.csv"""
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(companies)


def get_stock_list_from_akshare() -> pd.DataFrame:
    """
    从 akshare 获取 A股股票列表

    Returns:
        DataFrame with columns: 股票代码, 股票名称, 市场
    """
    try:
        import akshare as ak

        print("从 akshare 获取 A股股票列表...")
        stock_list = ak.stock_info_a_code_name()
        stock_list = stock_list.rename(
            columns={"symbol": "股票代码", "name": "股票名称"}
        )
        stock_list["市场"] = "A股"
        print(f"  获取到 {len(stock_list)} 只 A股")

        return stock_list[["股票代码", "股票名称", "市场"]]
    except ImportError:
        print("akshare 未安装，跳过股票列表更新")
        return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description="股票行业分类更新")
    parser.add_argument(
        "--stock", type=str, help="指定股票代码更新（如 688411）"
    )
    parser.add_argument(
        "--missing", action="store_true", help="仅更新未分类的股票"
    )
    parser.add_argument(
        "--all", action="store_true", help="更新所有股票（需要网络）"
    )
    parser.add_argument(
        "--fetch", action="store_true", help="从 akshare 获取最新股票列表"
    )
    parser.add_argument(
        "--output", type=str, default="output/companies.csv", help="输出文件路径"
    )
    args = parser.parse_args()

    # 确保输出目录存在
    os.makedirs("output", exist_ok=True)

    if args.fetch:
        # 获取最新股票列表
        df = get_stock_list_from_akshare()
        if not df.empty:
            # 读取现有数据
            existing = {}
            if os.path.exists(args.output):
                existing_companies = load_companies_csv(args.output)
                for c in existing_companies:
                    existing[c["股票代码"]] = c

            # 合并
            all_stocks = []
            fieldnames = ["股票代码", "股票名称", "市场", "行业大类", "行业小类", "热点领域"]
            for _, row in df.iterrows():
                code = row["股票代码"]
                if code in existing:
                    all_stocks.append(existing[code])
                else:
                    all_stocks.append({
                        "股票代码": code,
                        "股票名称": row["股票名称"],
                        "市场": row["市场"],
                        "行业大类": "",
                        "行业小类": "",
                        "热点领域": "",
                    })

            save_companies_csv(args.output, all_stocks, fieldnames)
            print(f"\n已更新股票列表，共 {len(all_stocks)} 只股票")
        return

    if args.stock:
        # 更新单只股票
        info = update_single_stock(args.stock, args.stock)
        print(f"\n更新结果: {info}")
        return

    if args.missing:
        # 更新未分类的股票
        if not os.path.exists(args.output):
            print(f"文件不存在: {args.output}")
            return

        companies = load_companies_csv(args.output)
        fieldnames = ["股票代码", "股票名称", "市场", "行业大类", "行业小类", "热点领域"]

        updated = 0
        for i, company in enumerate(companies):
            if not company.get("行业大类") or company["行业大类"] == "未知":
                code = company["股票代码"]
                name = company["股票名称"]
                print(f"\n[{i+1}/{len(companies)}] 处理未分类股票: {code} {name}")
                info = update_single_stock(code, name, company.get("市场", "A股"))
                company["行业大类"] = info.get("industry_large", "")
                company["行业小类"] = info.get("industry_small", "")
                company["热点领域"] = info.get("hot_fields", "")

                # 每处理10个保存一次
                if (updated + 1) % 10 == 0:
                    save_companies_csv(args.output, companies, fieldnames)
                    print(f"  已保存进度...")

                updated += 1
                time.sleep(0.5)  # 避免请求过快

        save_companies_csv(args.output, companies, fieldnames)
        print(f"\n完成！共更新 {updated} 只股票")
        return

    if args.all:
        # 更新所有股票
        if not os.path.exists(args.output):
            print(f"文件不存在: {args.output}")
            return

        companies = load_companies_csv(args.output)
        fieldnames = ["股票代码", "股票名称", "市场", "行业大类", "行业小类", "热点领域"]

        for i, company in enumerate(companies):
            code = company["股票代码"]
            name = company["股票名称"]
            print(f"\n[{i+1}/{len(companies)}] 处理: {code} {name}")
            info = update_single_stock(code, name, company.get("市场", "A股"))
            company["行业大类"] = info.get("industry_large", "")
            company["行业小类"] = info.get("industry_small", "")
            company["热点领域"] = info.get("hot_fields", "")

            # 每处理10个保存一次
            if (i + 1) % 10 == 0:
                save_companies_csv(args.output, companies, fieldnames)
                print(f"  已保存进度...")

            time.sleep(0.5)

        save_companies_csv(args.output, companies, fieldnames)
        print(f"\n完成！共处理 {len(companies)} 只股票")
        return

    # 默认：显示帮助
    parser.print_help()
    print("\n示例:")
    print("  python update_industry_class.py --fetch          # 获取最新股票列表")
    print("  python update_industry_class.py --missing       # 更新未分类的股票")
    print("  python update_industry_class.py --stock 688411  # 更新单只股票")


if __name__ == "__main__":
    main()
