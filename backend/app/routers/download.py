"""结果分片下载路由：创建/恢复下载、领取分片、确认回执与对账查询。

业务判断全部在 :class:`GeoExportService` 内完成，路由层只做参数转发，
与项目「状态流转只允许在 services 里改」的约定保持一致。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.geo_export import GeoExportService

router = APIRouter(prefix="/api/exports", tags=["结果分片下载"])


def _service() -> GeoExportService:
    return GeoExportService()


class CreateDownloadPayload(BaseModel):
    batch_key: str
    export_format: str = "jsonl"
    shard_size: int = Field(default=100, ge=1)


class ResumeDownloadPayload(BaseModel):
    download_id: str


class ClaimPayload(BaseModel):
    download_id: str


class AckPayload(BaseModel):
    download_id: str
    shard_no: int = Field(ge=1)


@router.post("/downloads")
def create_download(payload: CreateDownloadPayload) -> dict[str, Any]:
    """为批次创建一次分片下载并返回交付清单。"""
    try:
        return _service().create_download(
            payload.batch_key, payload.export_format, payload.shard_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/downloads/resume")
def resume_download(payload: ResumeDownloadPayload) -> dict[str, Any]:
    """从同一次下载续传，补齐未写完整的分片。"""
    try:
        return _service().resume_download(payload.download_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/shards/claim")
def claim_shard(payload: ClaimPayload) -> dict[str, Any]:
    """领取下一个未确认分片；没有可领取分片时返回空对象。"""
    try:
        shard = _service().claim_next_shard(payload.download_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"shard": shard}


@router.post("/shards/ack")
def acknowledge_shard(payload: AckPayload) -> dict[str, Any]:
    """确认分片回执，重复确认幂等。"""
    service = _service()
    try:
        return service.acknowledge_shard(payload.download_id, payload.shard_no)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/downloads/{download_id}/manifest")
def download_manifest(download_id: str) -> dict[str, Any]:
    """查回本次下载采用的交付清单（分类条数、分片与已领状态）。"""
    manifest = _service().get_download_manifest(download_id)
    if not manifest:
        raise HTTPException(status_code=404, detail=f"下载 {download_id} 的清单不存在")
    return manifest


@router.get("/downloads/{download_id}/ledger")
def delivery_ledger(download_id: str) -> dict[str, Any]:
    """查回本次下载实际领到的分片与全部领取/确认事件。"""
    service = _service()
    if not service.get_download_manifest(download_id):
        raise HTTPException(status_code=404, detail=f"下载 {download_id} 的清单不存在")
    return {"download_id": download_id, "events": service.get_delivery_ledger(download_id)}


@router.get("/downloads/{download_id}/progress")
def delivery_progress(download_id: str) -> dict[str, Any]:
    """查回分片确认进度与各分类已确认条数。"""
    try:
        return _service().get_delivery_progress(download_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/downloads/{download_id}/shards/{shard_no}")
def shard_records(download_id: str, shard_no: int) -> dict[str, Any]:
    """按下载实际格式把指定分片逐字段读回，便于核对区名等原文。"""
    service = _service()
    try:
        status = service.get_shard_status(download_id, shard_no)
        records = service.read_shard_records(download_id, shard_no)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"shard": status, "records": records}
