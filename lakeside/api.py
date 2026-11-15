"""HTTP 接口。让专家能在浏览器里点一下就看到守恒指标。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from . import __version__
from .config import settings
from .pipeline import Pipeline

app = FastAPI(title="lakeside", version=__version__)
_pipeline: Pipeline | None = None


def get_pipeline() -> Pipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = Pipeline()
    return _pipeline


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__, "logical_date": settings.logical_date}


@app.post("/build")
def build(layer: str = "all", keep_ods: bool = False) -> dict[str, Any]:
    p = get_pipeline()
    if layer == "all":
        return p.run_all(regenerate=not keep_ods)
    # 分层重建：总是先保证上游存在
    if not keep_ods and layer in ("ods", "dwd", "dws", "ads"):
        p.build_ods(regenerate=True)
    if layer in ("dwd", "dws", "ads"):
        p.build_dwd()
    if layer in ("dws", "ads"):
        p.build_dws()
    if layer == "ads":
        p.build_ads()
    return {"layer": layer, "assets": [r.to_dict() for r in p.results]}


@app.get("/invariants")
def invariants() -> dict[str, float]:
    return get_pipeline().invariants()


@app.get("/temporal")
def temporal() -> dict[str, Any]:
    """供应商 SCD2 时态维度的构建报告与隔离区样本。"""
    p = get_pipeline()
    report = p.temporal_report or {}
    rejects = []
    if p.lake.exists("dwd", "dim_supplier_reject"):
        rejects = p.lake.query_df(
            "SELECT supplier_id, reject_reason, valid_from, updated_at "
            "FROM dwd_dim_supplier_reject ORDER BY supplier_id LIMIT 50"
        ).to_dict("records")
    return {
        "report": report,
        "rejected_samples": rejects,
    }


@app.get("/month-boundary")
def month_boundary() -> dict[str, Any]:
    rows = get_pipeline().month_boundary_check()
    mismatches = [r for r in rows if r["dwd_date"] != r["true_local_date"]]
    return {
        "checked": len(rows),
        "mismatch": len(mismatches),
        "samples": mismatches[:10],
    }


@app.get("/catalog")
def catalog() -> dict[str, Any]:
    """列出各层有哪些表、多少文件、多少行。"""
    p = get_pipeline()
    out: dict[str, list[dict[str, Any]]] = {}
    for layer in ("ods", "dwd", "dws", "ads"):
        d = settings.lake_root / layer
        if not d.exists():
            continue
        items = []
        for sub in sorted(d.iterdir()):
            if sub.is_dir():
                items.append(
                    {
                        "table": sub.name,
                        "files": len(list(sub.rglob("*.parquet"))),
                        "rows": p.lake.count(layer, sub.name),
                    }
                )
        out[layer] = items
    return out


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    p = get_pipeline()
    inv = p.invariants()
    rows = "".join(
        f"<tr><td>{k}</td><td style='text-align:right'>{v:,.0f}</td></tr>"
        for k, v in sorted(inv.items())
    )
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>lakeside</title>
<style>body{{font-family:system-ui,-apple-system,sans-serif;max-width:760px;margin:40px auto;padding:0 16px}}
table{{border-collapse:collapse;width:100%}}td{{border-bottom:1px solid #eee;padding:6px 4px}}
code{{background:#f4f4f4;padding:2px 5px;border-radius:3px}}</style></head>
<body><h1>lakeside · 湖仓管线</h1>
<p>逻辑日期 <code>{settings.logical_date}</code> ｜ 版本 <code>{__version__}</code></p>
<table>{rows}</table>
<p style="margin-top:24px;color:#666">DWS / ADS 当前是草稿状态，口径尚未定稿。</p>
</body></html>"""


@app.post("/reset")
def reset() -> JSONResponse:
    """丢弃内存里的管线对象，下次请求重新连接湖目录。"""
    global _pipeline
    if _pipeline is not None:
        _pipeline.close()
    _pipeline = None
    return JSONResponse({"reset": True})
