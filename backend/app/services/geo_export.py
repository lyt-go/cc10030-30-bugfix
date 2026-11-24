"""结果导出领域服务：将空间分析结果按记录类别和字段范围分片导出。

设计要点：
- 每条结果记录都带 ``record_category``（heatmap / od_matrix / region_aggregate），
  导出按各类结果的实际字段生成表格；选择 CSV 时写出符合 RFC 4180 的 CSV，
  区名中的逗号、双引号、换行均可逐字段读回，JSONL 仍按原有约定逐行交付。
- 每次导出是一次独立下载，拥有清单（manifest）和领取台账（ledger）。
- 分片以「临时文件 + 原子改名」落盘，写入中断只可能留下临时文件，
  下载方永远领不到残缺表格；恢复后从同一次下载继续，已确认分片不重复交付。
"""
from __future__ import annotations

import csv
import io
import json
import os
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService

# 各类结果在同一次下载中的固定先后顺序，也决定了分片编号的排布。
CATEGORY_ORDER = ["heatmap", "od_matrix", "region_aggregate"]

# 各记录类别在表格中使用的中文名与实际字段（字段名即 JSONL 产物中的键）。
CATEGORY_LABELS = {
    "heatmap": "热力记录",
    "od_matrix": "流量记录",
    "region_aggregate": "区域记录",
}
CATEGORY_COLUMNS: dict[str, list[str]] = {
    "heatmap": [
        "record_category", "cell_id", "batch_key", "geohash", "grid_level",
        "time_bucket", "time_bucket_str", "point_count", "center_lat", "center_lon",
    ],
    "od_matrix": [
        "record_category", "unit_id", "batch_key", "origin_geohash", "dest_geohash",
        "time_bucket", "trip_count", "avg_duration", "travel_mode",
    ],
    "region_aggregate": [
        "record_category", "batch_key", "region_id", "region_name", "region_level",
        "boundary_code", "grid_count", "total_points",
        "region_center_lat", "region_center_lon", "generated_at",
    ],
}

# 分片在台账中的状态机：ready 可领取；claimed 已领待回执；acked 已确认。
SHARD_READY = "ready"
SHARD_CLAIMED = "claimed"
SHARD_ACKED = "acked"


class GeoExportService(BaseModuleService):
    MODULE = "geo_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["导出编号", "快照编号", "导出格式", "区域范围", "字段范围", "记录条数", "分片大小", "导出水位"]
    REQUIRED_FIELDS = ["导出编号", "快照编号", "导出格式"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    ACTION_RULES = {"启动导出": "导出中", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 结果收集：按类别读取可完整解析的业务记录并打上类别标签
    # ------------------------------------------------------------------

    def _collect_records(self, batch_key: str) -> dict[str, list[dict[str, Any]]]:
        """读取批次下热力、OD、区域聚合三类 JSONL 产物，按类别归集。

        只有能被完整解析的业务记录才计入；文件缺失或存在坏行时跳过坏行，
        因此清单中的分类条数与实际能解析的完整业务记录保持一致。
        """
        sources = {
            "heatmap": self._data_dir("heatmap") / f"{batch_key}_heatmap.jsonl",
            "od_matrix": self._data_dir("od_matrix") / f"{batch_key}_od.jsonl",
            "region_aggregate": self._data_dir("region_aggregates") / f"{batch_key}_region.jsonl",
        }
        collected: dict[str, list[dict[str, Any]]] = {}
        for category in CATEGORY_ORDER:
            records = self._read_jsonl(sources[category]) if sources[category].exists() else []
            tagged: list[dict[str, Any]] = []
            for record in records:
                if not isinstance(record, dict):
                    continue
                record = dict(record)
                record["record_category"] = category
                tagged.append(record)
            collected[category] = tagged
        return collected

    @staticmethod
    def _shard_records(records_by_category: dict[str, list[dict[str, Any]]],
                       shard_size: int) -> list[tuple[str, list[dict[str, Any]]]]:
        """按类别顺序切分，单个分片只承载同一类别的记录，便于按实际字段出表。"""
        shards: list[tuple[str, list[dict[str, Any]]]] = []
        for category in CATEGORY_ORDER:
            records = records_by_category.get(category, [])
            for i in range(0, len(records), shard_size):
                shards.append((category, records[i:i + shard_size]))
        return shards

    # ------------------------------------------------------------------
    # 表格渲染：CSV 严格转义，JSONL 沿用既有约定
    # ------------------------------------------------------------------

    def _render_shard(self, category: str, records: list[dict[str, Any]],
                      export_format: str) -> str:
        """按类别实际字段把分片记录渲染成 CSV 或 JSONL 文本。"""
        fmt = export_format.lower()
        if fmt == "jsonl":
            return "".join(
                json.dumps(record, ensure_ascii=False, default=str) + "\n"
                for record in records
            )
        if fmt != "csv":
            raise ValueError(f"不支持的导出格式：{export_format}（仅支持 csv / jsonl）")

        columns = CATEGORY_COLUMNS[category]
        buffer = io.StringIO()
        # QUOTE_MINIMAL 会自动对含逗号、双引号、换行的字段加引号并双写内部引号，
        # lineterminator 使用 CRLF 符合 RFC 4180，换行符可被 csv 读回为字段内换行。
        writer = csv.DictWriter(
            buffer, fieldnames=columns, extrasaction="ignore",
            quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n",
        )
        writer.writeheader()
        for record in records:
            writer.writerow({
                column: "" if record.get(column) is None else record.get(column, "")
                for column in columns
            })
        return buffer.getvalue()

    def _atomic_write(self, final_path: Path, content: str) -> None:
        """临时文件写全后原子改名；中途失败不会产生可被领取的残缺分片。"""
        self._data_dir(final_path.parent.name)
        temp_path = final_path.with_name(final_path.name + ".part")
        with open(temp_path, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, final_path)

    # ------------------------------------------------------------------
    # 下载创建、清单与分片落盘
    # ------------------------------------------------------------------

    def create_download(self, batch_key: str, export_format: str = "jsonl",
                        shard_size: int = 100) -> dict[str, Any]:
        """为批次创建一次独立下载：固化清单、暂存记录并写全部分片。

        返回清单字典。任一分片写入失败都会抛错，已写好的分片仍以临时/缺失
        形态留在磁盘上且不会登记进清单，下载方无法领取残缺表格，
        随后可对同一次下载调用 :meth:`resume_download` 续传。
        """
        fmt = export_format.lower()
        if fmt not in ("csv", "jsonl"):
            raise ValueError(f"不支持的导出格式：{export_format}（仅支持 csv / jsonl）")
        if shard_size <= 0:
            raise ValueError("分片大小必须为正整数")

        records_by_category = self._collect_records(batch_key)
        category_counts = {
            category: len(records_by_category.get(category, []))
            for category in CATEGORY_ORDER
        }
        total_records = sum(category_counts.values())
        planned = self._shard_records(records_by_category, shard_size)

        download_id = f"EXPORT-{batch_key}-{int(time.time() * 1000)}"
        extension = "csv" if fmt == "csv" else "jsonl"

        # 暂存本次下载快照的全部记录：恢复时不再重读源产物，
        # 区域事后修正的名称只会随另一次下载发出，不污染本次下载。
        staging_path = self._data_dir("exports") / f"{download_id}_staging.jsonl"
        staged = [record for _, shard in planned for record in shard]
        self._write_jsonl(staging_path, staged)

        manifest: dict[str, Any] = {
            "download_id": download_id,
            "batch_key": batch_key,
            "export_format": fmt,
            "shard_size": shard_size,
            "total_records": total_records,
            "category_counts": category_counts,
            "created_at": time.time(),
            "shards": [],
        }

        shard_files: list[str] = []
        for index, (category, records) in enumerate(planned, start=1):
            shard_no = f"{index:03d}"
            filename = f"{download_id}_shard_{shard_no}.{extension}"
            final_path = self._data_dir("exports") / filename
            content = self._render_shard(category, records, fmt)
            # 原子落盘：成功后才登记进清单，失败则本次下载停留在「导出中」。
            self._atomic_write(final_path, content)
            checksum = self._compute_checksum(content)
            shard_info = {
                "shard_no": index,
                "record_category": category,
                "category_label": CATEGORY_LABELS[category],
                "record_count": len(records),
                "filename": filename,
                "checksum": checksum,
                "status": SHARD_READY,
            }
            manifest["shards"].append(shard_info)
            shard_files.append(str(final_path))

        self._write_manifest(download_id, manifest)
        self._init_ledger(download_id, manifest)
        self._register_export_entry(download_id, batch_key, fmt, shard_size,
                                    total_records, category_counts, manifest)
        return manifest

    def _register_export_entry(self, download_id: str, batch_key: str, fmt: str,
                               shard_size: int, total_records: int,
                               category_counts: dict[str, int],
                               manifest: dict[str, Any]) -> None:
        """在结果导出模块登记一条与本次下载一一对应的导出任务。"""
        category_text = "_".join(
            f"{CATEGORY_LABELS[category]}{category_counts[category]}条"
            for category in CATEGORY_ORDER
        )
        entry, _ = self.create_entry({
            "导出编号": download_id,
            "快照编号": f"SNAP-{batch_key}",
            "导出格式": fmt,
            "区域范围": "all",
            "字段范围": "category_aware",
            "记录条数": str(total_records),
            "分片大小": str(shard_size),
            "导出水位": f"已导出{total_records}条/{len(manifest['shards'])}分片（{category_text}）",
        })
        if entry:
            self.run_action(entry["id"], "启动导出")

    # ------------------------------------------------------------------
    # 恢复：把未写完整的分片补齐，已确认内容不重复交付
    # ------------------------------------------------------------------

    def resume_download(self, download_id: str) -> dict[str, Any]:
        """恢复同一次下载：补齐缺失分片并重建台账，已确认分片保持原状态。"""
        manifest = self.get_download_manifest(download_id)
        if not manifest:
            raise FileNotFoundError(f"下载 {download_id} 的清单不存在，无法恢复")

        fmt = manifest["export_format"]
        extension = "csv" if fmt == "csv" else "jsonl"
        staged = self._read_jsonl(
            self._data_dir("exports") / f"{download_id}_staging.jsonl"
        )
        by_category: dict[str, list[dict[str, Any]]] = {
            category: [] for category in CATEGORY_ORDER
        }
        for record in staged:
            by_category.setdefault(record.get("record_category", ""), []).append(record)
        planned = self._shard_records(by_category, manifest["shard_size"])

        ledger = self._read_ledger(download_id)
        acked = {event["shard_no"] for event in ledger if event["event"] == "ack"}
        rendered: dict[int, str] = {}
        for index, (category, records) in enumerate(planned, start=1):
            filename = f"{download_id}_shard_{index:03d}.{extension}"
            final_path = self._data_dir("exports") / filename
            content = self._render_shard(category, records, fmt)
            rendered[index] = content
            if index in acked and final_path.exists():
                # 已确认分片原样保留，绝不重写、不重复交付。
                continue
            self._atomic_write(final_path, content)

        # 以台账为准重建清单分片视图：acked 状态不回退，新补齐的分片为 ready。
        rebuilt: list[dict[str, Any]] = []
        for index, (category, records) in enumerate(planned, start=1):
            filename = f"{download_id}_shard_{index:03d}.{extension}"
            rebuilt.append({
                "shard_no": index,
                "record_category": category,
                "category_label": CATEGORY_LABELS[category],
                "record_count": len(records),
                "filename": filename,
                "checksum": self._compute_checksum(rendered[index]),
                "status": SHARD_ACKED if index in acked else SHARD_READY,
            })
        manifest["shards"] = rebuilt
        manifest["resumed_at"] = time.time()
        self._write_manifest(download_id, manifest)
        self._refresh_export_watermark(download_id, manifest)
        return manifest

    # ------------------------------------------------------------------
    # 领取 / 确认：至少一次投递，已确认分片与分类记录不重复
    # ------------------------------------------------------------------

    def claim_next_shard(self, download_id: str) -> dict[str, Any] | None:
        """领取下一个可交付分片。

        - 已确认（acked）的分片永不再次返回；
        - 已领取但回执丢失（仍为 claimed）的分片会从同一次下载再次返回，
          其分类记录不会被重复记账，下载方凭 shard_no 幂等处理即可。
        """
        manifest = self.get_download_manifest(download_id)
        if not manifest:
            raise FileNotFoundError(f"下载 {download_id} 的清单不存在")
        ledger = self._read_ledger(download_id)
        acked = {event["shard_no"] for event in ledger if event["event"] == "ack"}

        for shard in manifest["shards"]:
            if shard["shard_no"] in acked:
                continue
            final_path = self._data_dir("exports") / shard["filename"]
            if not final_path.exists():
                # 残缺/未写完的分片不可领取，等待 resume_download 补齐。
                continue
            self._append_ledger(download_id, {
                "event": "claim",
                "shard_no": shard["shard_no"],
                "record_category": shard["record_category"],
                "record_count": shard["record_count"],
                "filename": shard["filename"],
                "checksum": shard["checksum"],
                "at": time.time(),
            })
            self._mark_shard_status(download_id, shard["shard_no"], SHARD_CLAIMED)
            return {
                "download_id": download_id,
                **shard,
                "status": SHARD_CLAIMED,
                "content": final_path.read_text(encoding="utf-8"),
            }
        return None

    def acknowledge_shard(self, download_id: str, shard_no: int) -> dict[str, Any]:
        """登记分片确认回执；重复确认幂等，且不会重复计入分类记录。"""
        manifest = self.get_download_manifest(download_id)
        if not manifest:
            raise FileNotFoundError(f"下载 {download_id} 的清单不存在")
        shard = next((item for item in manifest["shards"]
                      if item["shard_no"] == shard_no), None)
        if shard is None:
            raise ValueError(f"下载 {download_id} 不存在分片 {shard_no}")

        ledger = self._read_ledger(download_id)
        already_acked = any(
            event["event"] == "ack" and event["shard_no"] == shard_no
            for event in ledger
        )
        if not already_acked:
            self._append_ledger(download_id, {
                "event": "ack",
                "shard_no": shard_no,
                "record_category": shard["record_category"],
                "record_count": shard["record_count"],
                "filename": shard["filename"],
                "checksum": shard["checksum"],
                "at": time.time(),
            })
        self._mark_shard_status(download_id, shard_no, SHARD_ACKED)

        refreshed = self.get_download_manifest(download_id)
        self._refresh_export_watermark(download_id, refreshed)
        if all(item["status"] == SHARD_ACKED for item in refreshed["shards"]):
            self._mark_export_delivered(download_id)
        return self.get_shard_status(download_id, shard_no)

    # ------------------------------------------------------------------
    # 对账查询：清单、台账、分片状态、读回内容均可查
    # ------------------------------------------------------------------

    def get_download_manifest(self, download_id: str) -> dict[str, Any]:
        """读取一次下载的交付清单，不存在时返回空字典。"""
        path = self._data_dir("exports") / f"{download_id}_manifest.json"
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def get_delivery_ledger(self, download_id: str) -> list[dict[str, Any]]:
        """读取领取/确认台账事件流，供恢复前后对账。"""
        return self._read_ledger(download_id)

    def get_shard_status(self, download_id: str, shard_no: int) -> dict[str, Any]:
        """查询单个分片的类别、条数、文件名、校验值与已领状态。"""
        manifest = self.get_download_manifest(download_id)
        shard = next((item for item in manifest.get("shards", [])
                      if item["shard_no"] == shard_no), None)
        if shard is None:
            raise ValueError(f"下载 {download_id} 不存在分片 {shard_no}")
        ledger = self._read_ledger(download_id)
        events = [event for event in ledger if event["shard_no"] == shard_no]
        claimed = any(event["event"] == "claim" for event in events)
        acked = any(event["event"] == "ack" for event in events)
        status = SHARD_ACKED if acked else (SHARD_CLAIMED if claimed else SHARD_READY)
        return {
            "download_id": download_id,
            **shard,
            "status": status,
            "claimed": claimed,
            "acked": acked,
            "events": events,
        }

    def read_shard_records(self, download_id: str, shard_no: int) -> list[dict[str, Any]]:
        """按下载实际格式把分片内容逐字段读回为业务记录列表。"""
        manifest = self.get_download_manifest(download_id)
        shard = next((item for item in manifest.get("shards", [])
                      if item["shard_no"] == shard_no), None)
        if shard is None:
            raise ValueError(f"下载 {download_id} 不存在分片 {shard_no}")
        path = self._data_dir("exports") / shard["filename"]
        if not path.exists():
            raise FileNotFoundError(f"分片文件缺失：{shard['filename']}")
        if manifest["export_format"] == "jsonl":
            return self._read_jsonl(path)
        with open(path, "r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def get_delivery_progress(self, download_id: str) -> dict[str, Any]:
        """汇总一次下载的领取/确认进度与各分类已确认条数。"""
        manifest = self.get_download_manifest(download_id)
        if not manifest:
            raise FileNotFoundError(f"下载 {download_id} 的清单不存在")
        ledger = self._read_ledger(download_id)
        acked_shards = {event["shard_no"] for event in ledger if event["event"] == "ack"}
        acked_by_category = {category: 0 for category in CATEGORY_ORDER}
        for shard in manifest["shards"]:
            if shard["shard_no"] in acked_shards:
                acked_by_category[shard["record_category"]] = (
                    acked_by_category.get(shard["record_category"], 0)
                    + shard["record_count"]
                )
        return {
            "download_id": download_id,
            "total_shards": len(manifest["shards"]),
            "claimed_shards": sum(1 for item in manifest["shards"]
                                  if item["status"] != SHARD_READY),
            "acked_shards": len(acked_shards),
            "acked_records": sum(acked_by_category.values()),
            "acked_category_counts": acked_by_category,
            "manifest_category_counts": manifest["category_counts"],
        }

    # ------------------------------------------------------------------
    # 兼容既有入口：一次性导出并确认交付
    # ------------------------------------------------------------------

    def export_batch_results(self, batch_key: str, export_format: str = "jsonl",
                              shard_size: int = 100) -> dict[str, Any]:
        """读取分析快照数据集，按指定格式将热力、OD和区域数据分片导出。

        选择 ``csv`` 时分片文件为真实 CSV 文本（按各类别实际字段出表），
        选择 ``jsonl`` 时维持原有 JSONL 交付约定。
        """
        manifest = self.create_download(batch_key, export_format, shard_size)
        # 一次性导出默认全部确认交付，保持旧调用方的行为预期。
        for shard in manifest["shards"]:
            self.acknowledge_shard(manifest["download_id"], shard["shard_no"])

        export_summary = {
            "batch_key": batch_key,
            "download_id": manifest["download_id"],
            "export_format": manifest["export_format"],
            "total_records": manifest["total_records"],
            "category_counts": manifest["category_counts"],
            "shards": len(manifest["shards"]),
            "shard_size": manifest["shard_size"],
            "exported_at": time.time(),
        }
        summary_file = self._data_dir("exports") / f"{batch_key}_export_summary.jsonl"
        self._overwrite_jsonl(summary_file, [export_summary])

        return {
            "batch_key": batch_key,
            "download_id": manifest["download_id"],
            "exported_records": manifest["total_records"],
            "shards": len(manifest["shards"]),
            "format": manifest["export_format"],
            "shard_files": [
                str(self._data_dir("exports") / shard["filename"])
                for shard in manifest["shards"]
            ],
        }

    def get_export_summary(self, batch_key: str) -> dict[str, Any]:
        """读取批次最近一次导出任务的汇总数据集。"""
        file_path = self._data_dir("exports") / f"{batch_key}_export_summary.jsonl"
        summaries = self._read_jsonl(file_path)
        return summaries[-1] if summaries else {}

    # ------------------------------------------------------------------
    # 内部持久化工具
    # ------------------------------------------------------------------

    def _manifest_path(self, download_id: str) -> Path:
        return self._data_dir("exports") / f"{download_id}_manifest.json"

    def _ledger_path(self, download_id: str) -> Path:
        return self._data_dir("exports") / f"{download_id}_ledger.jsonl"

    def _write_manifest(self, download_id: str, manifest: dict[str, Any]) -> None:
        path = self._manifest_path(download_id)
        temp_path = path.with_name(path.name + ".part")
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2, default=str)
        os.replace(temp_path, path)

    def _init_ledger(self, download_id: str, manifest: dict[str, Any]) -> None:
        self._ledger_path(download_id).write_text("", encoding="utf-8")

    def _read_ledger(self, download_id: str) -> list[dict[str, Any]]:
        path = self._ledger_path(download_id)
        return self._read_jsonl(path) if path.exists() else []

    def _append_ledger(self, download_id: str, event: dict[str, Any]) -> None:
        self._write_jsonl(self._ledger_path(download_id), [event])

    def _mark_shard_status(self, download_id: str, shard_no: int, status: str) -> None:
        manifest = self.get_download_manifest(download_id)
        for shard in manifest.get("shards", []):
            if shard["shard_no"] == shard_no:
                shard["status"] = status
        self._write_manifest(download_id, manifest)

    def _refresh_export_watermark(self, download_id: str,
                                  manifest: dict[str, Any]) -> None:
        entry = self._find_export_entry(download_id)
        if entry is None:
            return
        acked = sum(1 for shard in manifest["shards"] if shard["status"] == SHARD_ACKED)
        category_text = "_".join(
            f"{CATEGORY_LABELS[category]}{manifest['category_counts'].get(category, 0)}条"
            for category in CATEGORY_ORDER
        )
        entry["导出水位"] = (
            f"已确认{acked}/{len(manifest['shards'])}分片，"
            f"共{manifest['total_records']}条（{category_text}）"
        )

    def _mark_export_delivered(self, download_id: str) -> None:
        entry = self._find_export_entry(download_id)
        if entry is not None and entry.get("status") != self.STATUS_ORDER[-1]:
            self.run_action(entry["id"], "确认交付")

    def _find_export_entry(self, download_id: str) -> dict[str, Any] | None:
        from app.store import store
        for row in store.rows(self.MODULE):
            if row.get("导出编号") == download_id:
                return row
        return None

    def _overwrite_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> None:
        """以覆盖方式写 JSONL（汇总文件每次下载只保留最新口径）。"""
        self._data_dir(filepath.parent.name)
        temp_path = filepath.with_name(filepath.name + ".part")
        with open(temp_path, "w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        os.replace(temp_path, filepath)
