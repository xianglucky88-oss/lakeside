#!/bin/sh
# demo.sh —— 90 秒内录完的固定演示。
#
#   1. 全量构建（ODS -> DWD -> DWS -> ADS）
#   2. 打印各层行数
#   3. 跑守恒断言，展示金额被放大多少倍
#   4. 再跑一次构建，观察"重跑是否翻倍"
#   5. 打印月末归属错误样本
set -eu
cd "$(dirname "$0")/.."
export LAKE_ROOT="${LAKE_ROOT:-/tmp/lakeside-demo}"

say() { printf '\n=== %s ===\n' "$1"; }

say "1. 全量构建"
python3 -m lakeside build

say "2. 目录结构（层 / 表 / 文件数）"
find "$LAKE_ROOT" -name "*.parquet" | sed "s|$LAKE_ROOT/||" | sort

say "3. 守恒断言"
python3 scripts/verify_lake.py || true

say "4. 重跑一次，观察 DWS 是否翻倍"
python3 -m lakeside build --layer dws >/dev/null
python3 scripts/verify_lake.py || true

say "5. 月末归属"
python3 -c "
from lakeside.pipeline import Pipeline
p = Pipeline()
rows = p.month_boundary_check()
bad = [r for r in rows if r['dwd_date'] != r['true_local_date']]
print(f'抽查 {len(rows)} 条深夜结算，跨日错误 {len(bad)} 条')
for r in bad[:5]:
    print(' ', r['settlement_id'], r['settled_at_utc'], '->', r['dwd_date'], '应为', r['true_local_date'])
p.close()
"
echo
