SHELL := /bin/sh
PY ?= python3
LAKE_ROOT ?= lake

.PHONY: help
help:
	@echo "make build       全量构建管线（含准入/隔离）"
	@echo "make verify      守恒断言（带具体数字）"
	@echo "make admission   查看准入批次与逐表接受/暂缓/隔离结论"
	@echo "make quarantine  列出暂缓/隔离记录、证据与恢复条件"
	@echo "make contracts   查看数据契约版本"
	@echo "make demo        固定演示序列（含异常注入与确定性重处理）"
	@echo "make test        单元测试"
	@echo "make api         起 HTTP 服务（9300）"
	@echo "make clean       删除本地湖目录"

.PHONY: build
build:
	LAKE_ROOT=$(LAKE_ROOT) $(PY) -m lakeside build

.PHONY: verify
verify:
	LAKE_ROOT=$(LAKE_ROOT) $(PY) scripts/verify_lake.py

.PHONY: admission
admission:
	LAKE_ROOT=$(LAKE_ROOT) $(PY) -m lakeside admission

.PHONY: quarantine
quarantine:
	LAKE_ROOT=$(LAKE_ROOT) $(PY) -m lakeside quarantine || true

.PHONY: contracts
contracts:
	LAKE_ROOT=$(LAKE_ROOT) $(PY) -m lakeside contracts

.PHONY: demo
demo:
	LAKE_ROOT=$(LAKE_ROOT) sh scripts/demo.sh

.PHONY: test
test:
	$(PY) -m pytest -q

.PHONY: api
api:
	LAKE_ROOT=$(LAKE_ROOT) $(PY) -m uvicorn lakeside.api:app --port 9300 --reload

.PHONY: spark-build
spark-build:
	@echo "生产路径，需要 Spark 集群。本地验证请用 make build。"
	$(PY) -c "from lakeside.spark_engine import SparkIcebergLake; SparkIcebergLake().build_all()"

.PHONY: clean
clean:
	rm -rf $(LAKE_ROOT) .pytest_cache _warehouse
	# landing / quarantine / contracts 都在 LAKE_ROOT 下，随之一并清除。
