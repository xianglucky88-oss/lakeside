#!/bin/sh
# demo.sh —— 固定演示序列。
#
#   1. 全量构建（ODS -> DWD -> DWS -> ADS）
#   2. 打印各层行数与快照分区
#   3. 跑守恒断言与履约风险分布
#   4. 再跑一次构建，观察整表/分区覆盖是否幂等（金额不翻倍）
#   5. 打印月末归属校验（UTC 跨日按本地时区换算后应为 0 错误）
set -eu
cd "$(dirname "$0")/.."
export LAKE_ROOT="${LAKE_ROOT:-/tmp/lakeside-demo}"

say() { printf '\n=== %s ===\n' "$1"; }

say "1. 全量构建"
python3 -m lakeside build

say "2. 目录结构（层 / 表 / 分区文件数）"
find "$LAKE_ROOT" -name "*.parquet" | sed "s|$LAKE_ROOT/||" | sort

say "3. 守恒断言与风险分布"
python3 scripts/verify_lake.py || true

say "4. 重跑一次，观察金额是否翻倍（不应翻倍）"
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
