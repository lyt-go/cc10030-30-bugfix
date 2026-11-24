"""区域维度领域服务：维护行政与业务区域层级维度表，将网格索引归属到区域维度中生成区域聚合数据集。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class RegionDimService(BaseModuleService):
    MODULE = "region_dim"
    ENTITY = "区域层级"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["区域编号", "上级区域", "区域名称", "区域级别", "边界编码", "网格数量", "维度版本", "生效时间"]
    REQUIRED_FIELDS = ["区域编号", "上级区域", "区域名称"]
    STATUS_ORDER = ["草稿", "待校验", "已发布", "已下线"]
    ACTION_RULES = {"提交校验": "待校验", "发布维度": "已发布", "标记下线": "已下线"}
    NEGATIVE_ACTIONS = []

    def aggregate_by_region(self, batch_key: str, region_id: int = 1) -> dict[str, Any]:
        """读取网格索引数据集，将网格按区域归属聚合生成区域维度汇总数据集。"""
        region = store.find(self.MODULE, region_id)
        if region is None:
            region = {"区域编号": "REGION-001", "区域名称": "东城区", "区域级别": "区级",
                      "边界编码": "110101", "上级区域": "北京市"}

        # 读取网格索引
        try:
            grid_svc = self._safe_import("app.services.grid_index", "GridIndexService")()
            grids, _ = grid_svc.get_grid_index(batch_key)
        except Exception:
            grids_file = self._data_dir("grid_index") / f"{batch_key}_grid.jsonl"
            if not grids_file.exists():
                raise FileNotFoundError(f"批次 {batch_key} 的网格索引数据不存在")
            grids = self._read_jsonl(grids_file)

        if not grids:
            return {"batch_key": batch_key, "total_points_aggregated": 0}

        total_points = sum(g["point_count"] for g in grids)
        region_center_lat = sum(g["center_lat"] * g["point_count"] for g in grids) / max(total_points, 1)
        region_center_lon = sum(g["center_lon"] * g["point_count"] for g in grids) / max(total_points, 1)

        # 区域名称原样保留（含逗号、双引号、换行等字符），不做任何裁剪或转义，
        # 工作台展示与区域聚合产物、结果导出读回的文本必须逐字一致。
        region_name = region.get("区域名称", "未知")
        summary = {
            "record_category": "region_aggregate",
            "batch_key": batch_key,
            "region_id": region_id,
            "region_name": region_name,
            "region_level": region.get("区域级别", "未知"),
            "boundary_code": region.get("边界编码", ""),
            "grid_count": len(grids),
            "total_points": total_points,
            "region_center_lat": round(region_center_lat, 6),
            "region_center_lon": round(region_center_lon, 6),
            "generated_at": time.time(),
        }

        output_file = self._data_dir("region_aggregates") / f"{batch_key}_region.jsonl"
        written = self._write_jsonl(output_file, [summary])

        # 更新区域维度记录
        if region.get("id"):
            region["网格数量"] = len(grids)

        return {
            "batch_key": batch_key,
            "region_name": summary["region_name"],
            "grids_aggregated": len(grids),
            "total_points": total_points,
            "output_file": str(output_file),
        }

    def get_region_aggregates(self, batch_key: str) -> tuple[list[dict[str, Any]], str]:
        """读取批次的区域聚合数据集文件。"""
        file_path = self._data_dir("region_aggregates") / f"{batch_key}_region.jsonl"
        data = self._read_jsonl(file_path)
        return data, str(file_path)