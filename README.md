# Stock Industry Classification (marketResearch)

A dataset and tools for maintaining industry classification and hot topics for Chinese stocks (A-share + HK stocks).

## Overview

This project maintains `output/companies.csv` — a comprehensive CSV file containing:
- **Stock Code** (股票代码)
- **Stock Name** (股票名称)
- **Market** (市场: A股, 港股通, etc.)
- **Industry Large Category** (行业大类: 28 categories)
- **Industry Small Category** (行业小类: 100+ categories)
- **Hot Topics** (热点领域: AI, 新能源, 芯片, etc.)

## Quick Start

### Fetch Latest Stock List
```bash
python update_industry_class.py --fetch
```

### Update Missing Classifications
```bash
python update_industry_class.py --missing
```

### Update Single Stock
```bash
python update_industry_class.py --stock 688411
```

## File Structure

```
marketResearch/
├── README.md
├── update_industry_class.py   # Main maintenance script
├── classify_companies.py      # Keyword classification rules
├── output/
│   ├── companies.csv           # Main data file (2800+ stocks)
│   ├── industry_large.csv      # Industry large category reference
│   └── industry_small.csv     # Industry small category reference
└── docs/
    └── 股票行业划分.xlsx       # Original industry classification standard
```

## Industry Categories

28 Large Categories: 农业, 采掘, 制造业, 水电煤气, 建筑业, 批发和零售业, 交通运输, 信息传输, 金融保险, 房地产, 租赁商务, 科学研究, 水利环境, 居民服务, 教育, 卫生社会, 文化体育, 综合, 食品饮料, 纺织服装, 木材家具, 造纸印刷, 石化塑胶, 电子, 金融非金属, 机械设备, 医药生物

## Hot Topics

人工智能, AI, 新能源, 光伏, 风电, 储能, 芯片, 半导体, 生物医药, 创新药, 新材料, 5G, 6G, 物联网, 云计算, 大数据, 区块链, 自动驾驶, 新能源汽车, 军工, 航空航天, 碳中和 等

## Usage with qlquant

This dataset is used by `qlquant/scripts/research/industry_classifier.py` for:
- Industry classification of watchlist stocks
- Hot topic filtering
- Industry-level research aggregation

## License

Open source for research and educational purposes.
