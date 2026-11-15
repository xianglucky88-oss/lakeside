"""ODS 数据准入与隔离层。

分区职责：
    landing/     原始证据，只追加、内容寻址，永不改写；
    quarantine/  row_ledger（逐行结论台账）+ batch_registry（批次级结论）；
    ods/         仅由台账中 accepted 行确定性重建的可消费表。

入口类：AdmissionGate
契约：lakeside.admission.contracts
状态与错误码：lakeside.admission.models
"""

from __future__ import annotations

from .contracts import (
    ContractBreaking,
    ContractRegistry,
    FieldSpec,
    TableContract,
    initial_contracts,
)
from .gate import AdmissionGate
from . import models

__all__ = [
    "AdmissionGate",
    "ContractRegistry",
    "ContractBreaking",
    "TableContract",
    "FieldSpec",
    "initial_contracts",
    "models",
]
