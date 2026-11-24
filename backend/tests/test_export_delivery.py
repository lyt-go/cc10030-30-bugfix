"""结果导出交付测试：CSV 格式、原文逐字段读回、分片中断恢复与不重复交付。

覆盖验收口径：
- 选择 CSV 必须写出真实 CSV（不再是 JSONL），按各类结果的实际字段出表；
- 区域名称中的逗号、双引号、换行可逐字段读回，且与区域工作台原文一致；
- 清单分类条数等于实际能解析的完整业务记录数；
- 分片写入中断时下载方领不到残缺表格，恢复后从同一次下载继续；
- 已确认分片与分类记录不重复交付，回执丢失可凭同一下载再次领取；
- 区域事后修正的名称只随另一次下载发出。
"""
from __future__ import annotations

import csv
import json
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def setup_data_dir(monkeypatch):
    """每个测试用例使用独立临时目录写入数据产物文件。"""
    tmpdir = Path(tempfile.mkdtemp(prefix="geo_export_test_"))
    import app.services.base as base_mod
    original_root = base_mod.DATA_ROOT
    base_mod.DATA_ROOT = tmpdir
    (tmpdir / "exports").mkdir(parents=True, exist_ok=True)
    (tmpdir / "checkpoints").mkdir(parents=True, exist_ok=True)
    yield tmpdir
    base_mod.DATA_ROOT = original_root
    shutil.rmtree(tmpdir, ignore_errors=True)


TRICKY_REGION_NAME = '东城,朝阳"试点\n片区'  # 同时含逗号、双引号和换行


def _write_source_files(service, batch_key: str,
                         region_name: str = TRICKY_REGION_NAME) -> None:
    """构造 2 条热力记录 + 1 条名称带特殊字符的区域记录（无 OD）。"""
    heat_records = [
        {
            "cell_id": 1, "batch_key": batch_key, "geohash": "wx4g01",
            "grid_level": "L6", "time_bucket": 1_700_000_000,
            "time_bucket_str": "2023-11-14T22:13:20", "point_count": 7,
            "center_lat": 39.9042, "center_lon": 116.4074,
        },
        {
            "cell_id": 2, "batch_key": batch_key, "geohash": "wx4g02",
            "grid_level": "L6", "time_bucket": 1_700_003_600,
            "time_bucket_str": "2023-11-14T23:13:20", "point_count": 3,
            "center_lat": 39.9101, "center_lon": 116.4155,
        },
    ]
    region_record = {
        "record_category": "region_aggregate",
        "batch_key": batch_key,
        "region_id": 1,
        "region_name": region_name,
        "region_level": "区级",
        "boundary_code": "110101",
        "grid_count": 4,
        "total_points": 10,
        "region_center_lat": 39.907,
        "region_center_lon": 116.411,
        "generated_at": 1_700_007_200.0,
    }
    service._write_jsonl(
        service._data_dir("heatmap") / f"{batch_key}_heatmap.jsonl", heat_records)
    service._write_jsonl(
        service._data_dir("region_aggregates") / f"{batch_key}_region.jsonl",
        [region_record])


def _broken_line_file(path: Path) -> None:
    """在 JSONL 中插入一条无法解析的坏行。"""
    with open(path, "a", encoding="utf-8") as handle:
        handle.write('{"cell_id": 9, "point_count": 破损\n')


class TestCsvExportFormat:
    """CSV 格式与特殊字符逐字段读回。"""

    def test_csv_selection_writes_real_csv_not_jsonl(self, setup_data_dir):
        from app.services.geo_export import GeoExportService

        service = GeoExportService()
        batch_key = "BATCH-CSV-01"
        _write_source_files(service, batch_key)

        manifest = service.create_download(batch_key, export_format="csv", shard_size=2)

        # 清单标注的格式必须与实际交付文件一致。
        assert manifest["export_format"] == "csv"
        for shard in manifest["shards"]:
            path = setup_data_dir / "exports" / shard["filename"]
            assert path.suffix == ".csv"
            text = path.read_text(encoding="utf-8")
            # 第一行是 CSV 表头而不是 JSON 对象。
            first_column = text.splitlines()[0].split(",")[0]
            assert first_column in ("record_category", '"record_category"')
            with pytest.raises(json.JSONDecodeError):
                json.loads(text.splitlines()[0])

    def test_region_name_round_trips_field_by_field(self, setup_data_dir):
        from app.services.geo_export import CATEGORY_COLUMNS, GeoExportService

        service = GeoExportService()
        batch_key = "BATCH-CSV-02"
        _write_source_files(service, batch_key)

        manifest = service.create_download(batch_key, export_format="csv", shard_size=2)
        region_shard = next(
            shard for shard in manifest["shards"]
            if shard["record_category"] == "region_aggregate"
        )
        records = service.read_shard_records(manifest["download_id"],
                                             region_shard["shard_no"])

        assert len(records) == 1
        # 逗号、双引号、换行全部逐字段读回，列不错位、换行不丢失。
        assert records[0]["region_name"] == TRICKY_REGION_NAME
        assert "\n" in records[0]["region_name"]
        assert records[0]["record_category"] == "region_aggregate"
        # 表头即该类别实际字段，区域记录不带热力字段，反之亦然。
        path = setup_data_dir / "exports" / region_shard["filename"]
        with open(path, "r", encoding="utf-8", newline="") as handle:
            header = next(csv.reader(handle))
        assert header == CATEGORY_COLUMNS["region_aggregate"]

    def test_workbench_region_text_matches_export_original(self, setup_data_dir):
        """区域聚合产物保留区名原文，导出读回与工作台所持文本属性一致。"""
        from app.services.region_dim import RegionDimService
        from app.services.geo_export import GeoExportService
        from app.store import store

        region = store.find("region_dim", 1)
        region["区域名称"] = TRICKY_REGION_NAME

        region_service = RegionDimService()
        batch_key = "BATCH-CSV-03"
        # 直接给出网格索引文件，避免依赖随机点生成链路。
        grid_records = [
            {"geohash": "wx4g01", "point_count": 6, "center_lat": 39.9, "center_lon": 116.4},
            {"geohash": "wx4g02", "point_count": 4, "center_lat": 39.91, "center_lon": 116.41},
        ]
        region_service._write_jsonl(
            region_service._data_dir("grid_index") / f"{batch_key}_grid.jsonl",
            grid_records)
        region_service.aggregate_by_region(batch_key, region_id=1)

        aggregates, _ = region_service.get_region_aggregates(batch_key)
        assert aggregates[0]["region_name"] == TRICKY_REGION_NAME
        assert aggregates[0]["record_category"] == "region_aggregate"

        export_service = GeoExportService()
        manifest = export_service.create_download(batch_key, export_format="csv", shard_size=10)
        region_shard = next(
            shard for shard in manifest["shards"]
            if shard["record_category"] == "region_aggregate")
        records = export_service.read_shard_records(
            manifest["download_id"], region_shard["shard_no"])
        # 工作台原文 == 聚合产物 == CSV 读回值。
        assert records[0]["region_name"] == region["区域名称"]

    def test_jsonl_still_follows_existing_convention(self, setup_data_dir):
        from app.services.geo_export import GeoExportService

        service = GeoExportService()
        batch_key = "BATCH-JSONL-01"
        _write_source_files(service, batch_key)

        manifest = service.create_download(batch_key, export_format="jsonl", shard_size=2)
        for shard in manifest["shards"]:
            path = setup_data_dir / "exports" / shard["filename"]
            assert path.suffix == ".jsonl"
            lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
            # 每一行都是独立、可解析的 JSON 对象。
            for line in lines:
                parsed = json.loads(line)
                assert parsed["record_category"] == shard["record_category"]


class TestManifestCounts:
    """清单分类条数与实际可解析的完整业务记录相符。"""

    def test_category_counts_match_parseable_records(self, setup_data_dir):
        from app.services.geo_export import GeoExportService

        service = GeoExportService()
        batch_key = "BATCH-MANIFEST-01"
        _write_source_files(service, batch_key)
        # 坏行不计入完整业务记录。
        _broken_line_file(
            service._data_dir("heatmap") / f"{batch_key}_heatmap.jsonl")

        manifest = service.create_download(batch_key, export_format="csv", shard_size=2)

        assert manifest["category_counts"] == {
            "heatmap": 2, "od_matrix": 0, "region_aggregate": 1}
        assert manifest["total_records"] == 3
        shard_counts: dict[str, int] = {}
        for shard in manifest["shards"]:
            shard_counts.setdefault(shard["record_category"], 0)
            shard_counts[shard["record_category"]] += shard["record_count"]
        assert shard_counts["heatmap"] == 2
        assert shard_counts["region_aggregate"] == 1


class TestInterruptedRecovery:
    """第二片写入中断、恢复续传与不重复交付。"""

    def test_interrupted_shard_is_not_claimable_and_retry_completes(self, setup_data_dir, monkeypatch):
        from app.services.geo_export import GeoExportService

        service = GeoExportService()
        batch_key = "BATCH-RECOVER-01"
        _write_source_files(service, batch_key)
        exports_dir = setup_data_dir / "exports"

        # 第 2 片（区域记录所在分片）落盘时模拟中断：临时文件残留、最终文件缺失。
        original_atomic_write = GeoExportService._atomic_write

        def failing_write(self, final_path, content):
            if final_path.name.endswith("_shard_002.csv"):
                temp_path = final_path.with_name(final_path.name + ".part")
                temp_path.write_text("残缺内容", encoding="utf-8")
                raise OSError("模拟第二片写入中断")
            return original_atomic_write(self, final_path, content)

        monkeypatch.setattr(GeoExportService, "_atomic_write", failing_write)
        with pytest.raises(OSError):
            service.create_download(batch_key, export_format="csv", shard_size=2)
        monkeypatch.undo()

        # 中断后：首片虽已原子落盘，但清单未生成，任何分片都不可领取；
        # 第二片只有临时残留文件，下载方拿不到残缺表格。
        final_shards = sorted(p.name for p in exports_dir.glob("*_shard_*.csv"))
        assert len(final_shards) == 1 and final_shards[0].endswith("_shard_001.csv")
        assert not list(exports_dir.glob("*_shard_002.csv"))
        assert list(exports_dir.glob("*_shard_002.csv.part"))
        assert not list(exports_dir.glob("*_manifest.json"))
        failed_download_id = final_shards[0].split("_shard_")[0]
        with pytest.raises(FileNotFoundError):
            service.claim_next_shard(failed_download_id)

        # 再试一次：第二片完整写出，得到一份可领取的同口径下载。
        manifest = service.create_download(batch_key, export_format="csv", shard_size=2)
        download_id = manifest["download_id"]
        # 新下载自身不允许残留任何临时文件。
        assert not list(exports_dir.glob(f"{download_id}*.part"))
        assert len(manifest["shards"]) == 2
        assert manifest["shards"][1]["record_category"] == "region_aggregate"

        # 首片领取并确认。
        first = service.claim_next_shard(download_id)
        assert first["shard_no"] == 1
        service.acknowledge_shard(download_id, 1)

        # 第二片领取后回执未送达：状态停留在 claimed。
        second = service.claim_next_shard(download_id)
        assert second["shard_no"] == 2
        status_before = service.get_shard_status(download_id, 2)
        assert status_before["claimed"] is True
        assert status_before["acked"] is False

        # 从同一个下载继续领取：返回的仍是第二片，第一片不再重复交付。
        again = service.claim_next_shard(download_id)
        assert again is not None and again["shard_no"] == 2
        service.acknowledge_shard(download_id, 2)

        # 全部确认后无分片可领，进度与清单分类条数一致。
        assert service.claim_next_shard(download_id) is None
        progress = service.get_delivery_progress(download_id)
        assert progress["acked_shards"] == 2
        assert progress["acked_records"] == 3
        assert progress["acked_category_counts"] == {
            "heatmap": 2, "od_matrix": 0, "region_aggregate": 1}

    def test_resume_keeps_acked_shards_and_does_not_duplicate(self, setup_data_dir):
        from app.services.geo_export import GeoExportService

        service = GeoExportService()
        batch_key = "BATCH-RESUME-02"
        _write_source_files(service, batch_key)

        manifest = service.create_download(batch_key, export_format="csv", shard_size=2)
        download_id = manifest["download_id"]
        first_path = setup_data_dir / "exports" / manifest["shards"][0]["filename"]
        original_bytes = first_path.read_bytes()

        first = service.claim_next_shard(download_id)
        service.acknowledge_shard(download_id, first["shard_no"])

        # 模拟第二片丢失后恢复：已确认的首片不被重写，第二片补齐。
        second_path = setup_data_dir / "exports" / manifest["shards"][1]["filename"]
        second_path.unlink()
        resumed = service.resume_download(download_id)

        assert first_path.read_bytes() == original_bytes
        assert second_path.exists()
        statuses = {shard["shard_no"]: shard["status"] for shard in resumed["shards"]}
        assert statuses[1] == "acked"
        assert statuses[2] == "ready"

        # 恢复后只能领到第二片。
        claim = service.claim_next_shard(download_id)
        assert claim["shard_no"] == 2
        service.acknowledge_shard(download_id, 2)
        assert service.claim_next_shard(download_id) is None

        # 台账里首片只有一组 claim/ack，确认不重复。
        ledger = service.get_delivery_ledger(download_id)
        first_events = [event for event in ledger if event["shard_no"] == 1]
        assert [event["event"] for event in first_events] == ["claim", "ack"]

    def test_reconcile_manifest_and_ledger_before_and_after_recovery(self, setup_data_dir):
        """实际领到的区域分片与对账清单都可查回，区名、分类条数、已领状态一致。"""
        from app.services.geo_export import GeoExportService

        service = GeoExportService()
        batch_key = "BATCH-RECON-01"
        _write_source_files(service, batch_key)

        manifest = service.create_download(batch_key, export_format="csv", shard_size=2)
        download_id = manifest["download_id"]
        region_shard_no = next(
            shard["shard_no"] for shard in manifest["shards"]
            if shard["record_category"] == "region_aggregate")

        service.claim_next_shard(download_id)  # 首片
        service.acknowledge_shard(download_id, 1)
        service.claim_next_shard(download_id)  # 区域片，回执暂缺

        # 恢复前对账：清单可查、台账可查、区域片为已领未确认。
        stored_manifest = service.get_download_manifest(download_id)
        assert stored_manifest["category_counts"]["region_aggregate"] == 1
        ledger = service.get_delivery_ledger(download_id)
        assert any(event["event"] == "claim"
                   and event["shard_no"] == region_shard_no for event in ledger)
        region_status = service.get_shard_status(download_id, region_shard_no)
        assert region_status["record_count"] == 1
        assert region_status["claimed"] is True

        records_before = service.read_shard_records(download_id, region_shard_no)
        assert records_before[0]["region_name"] == TRICKY_REGION_NAME

        service.acknowledge_shard(download_id, region_shard_no)
        # 确认后再查回：区名不变、条数不变、状态推进为已确认。
        records_after = service.read_shard_records(download_id, region_shard_no)
        assert records_after[0]["region_name"] == records_before[0]["region_name"]
        region_status_after = service.get_shard_status(download_id, region_shard_no)
        assert region_status_after["acked"] is True
        assert service.get_delivery_progress(download_id)["acked_category_counts"][
            "region_aggregate"] == 1

    def test_later_region_rename_only_ships_in_another_download(self, setup_data_dir):
        from app.services.geo_export import GeoExportService

        service = GeoExportService()
        batch_key = "BATCH-RENAME-01"
        _write_source_files(service, batch_key, region_name=TRICKY_REGION_NAME)

        first_manifest = service.create_download(batch_key, export_format="csv", shard_size=2)
        first_id = first_manifest["download_id"]

        # 区域名称事后被修正，并重写区域聚合产物。
        corrected_name = "东城区（已勘误）"
        region_file = service._data_dir("region_aggregates") / f"{batch_key}_region.jsonl"
        region_records = service._read_jsonl(region_file)
        region_records[0]["region_name"] = corrected_name
        service._overwrite_jsonl(region_file, region_records)

        # 第一次下载的暂存快照不受影响，读回仍是旧名称。
        old_region_shard = next(
            shard for shard in first_manifest["shards"]
            if shard["record_category"] == "region_aggregate")
        old_records = service.read_shard_records(first_id, old_region_shard["shard_no"])
        assert old_records[0]["region_name"] == TRICKY_REGION_NAME

        # 新名称只随另一次下载发出。
        second_manifest = service.create_download(batch_key, export_format="csv", shard_size=2)
        assert second_manifest["download_id"] != first_id
        new_region_shard = next(
            shard for shard in second_manifest["shards"]
            if shard["record_category"] == "region_aggregate")
        new_records = service.read_shard_records(
            second_manifest["download_id"], new_region_shard["shard_no"])
        assert new_records[0]["region_name"] == corrected_name


class TestLegacyExportEntryPoint:
    """旧入口 export_batch_results 的兼容性与汇总口径。"""

    def test_legacy_entry_uses_declared_format_and_summary(self, setup_data_dir):
        from app.services.geo_export import GeoExportService

        service = GeoExportService()
        batch_key = "BATCH-LEGACY-01"
        _write_source_files(service, batch_key)

        result = service.export_batch_results(batch_key, export_format="csv", shard_size=2)
        assert result["format"] == "csv"
        assert result["exported_records"] == 3
        assert result["shards"] == 2
        assert all(path.endswith(".csv") for path in result["shard_files"])

        summary = service.get_export_summary(batch_key)
        assert summary["total_records"] == 3
        assert summary["category_counts"] == {
            "heatmap": 2, "od_matrix": 0, "region_aggregate": 1}

        # 登记的导出任务已交付，水位包含分类条数。
        from app.store import store
        entry = next(row for row in store.rows("geo_export")
                     if row["导出编号"] == result["download_id"])
        assert entry["导出格式"] == "csv"
        assert entry["status"] == "已交付"
        assert "热力记录2条" in entry["导出水位"]
        assert "区域记录1条" in entry["导出水位"]
