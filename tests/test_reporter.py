"""
reporter.py 테스트

테스트 대상:
    - _load_json / _load_csv: 안전 로딩, 없는 파일/잘못된 파일 처리
    - _get_insight_section: 섹션 추출 + 빈 데이터 폴백
    - _load_report_data: 전체 데이터 로딩 (빈 디렉토리 포함)
    - 차트 생성: 빈 데이터 시 None 반환
    - generate_report: 빈 데이터로 PPT 생성 (에러 없이 완료)
    - ReportData 기본값
"""

import json
import sys
import shutil
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reporter import (
    NO_DATA_MSG,
    ReportData,
    _get_insight_section,
    _load_csv,
    _load_json,
    _load_report_data,
    _chart_format_comparison,
    _chart_category_pie,
    _chart_posting_frequency,
    _chart_day_of_week,
    _chart_sentiment_pie,
    generate_report,
)


# ──────────────────────────────────────────────
# _load_json
# ──────────────────────────────────────────────


class TestLoadJson(unittest.TestCase):

    def test_valid_json(self):
        """유효한 JSON 파일 정상 로딩"""
        tmp = Path("/tmp/test_reporter_json")
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            p = tmp / "test.json"
            p.write_text('{"key": "value"}', encoding="utf-8")
            result = _load_json(p)
            self.assertEqual(result, {"key": "value"})
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_file_returns_none(self):
        """없는 파일이면 None 반환"""
        result = _load_json(Path("/tmp/nonexistent_file_12345.json"))
        self.assertIsNone(result)

    def test_invalid_json_returns_none(self):
        """잘못된 JSON이면 None 반환"""
        tmp = Path("/tmp/test_reporter_bad_json")
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            p = tmp / "bad.json"
            p.write_text("{invalid json", encoding="utf-8")
            result = _load_json(p)
            self.assertIsNone(result)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ──────────────────────────────────────────────
# _load_csv
# ──────────────────────────────────────────────


class TestLoadCsv(unittest.TestCase):

    def test_valid_csv(self):
        """유효한 CSV 파일 정상 로딩"""
        tmp = Path("/tmp/test_reporter_csv")
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            p = tmp / "test.csv"
            df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
            df.to_csv(p, index=False)
            result = _load_csv(p)
            self.assertIsNotNone(result)
            self.assertEqual(len(result), 2)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_file_returns_none(self):
        """없는 파일이면 None 반환"""
        result = _load_csv(Path("/tmp/nonexistent_file_12345.csv"))
        self.assertIsNone(result)

    def test_empty_csv_returns_none(self):
        """빈 CSV(헤더도 없음)이면 None 반환"""
        tmp = Path("/tmp/test_reporter_empty_csv")
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            p = tmp / "empty.csv"
            p.write_text("", encoding="utf-8")
            result = _load_csv(p)
            self.assertIsNone(result)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ──────────────────────────────────────────────
# _get_insight_section
# ──────────────────────────────────────────────


class TestGetInsightSection(unittest.TestCase):

    def test_returns_section_text(self):
        """정상적으로 섹션 텍스트 반환"""
        insights = {"sections": {"summary": "좋은 채널입니다."}}
        result = _get_insight_section(insights, "summary")
        self.assertEqual(result, "좋은 채널입니다.")

    def test_missing_section_returns_no_data(self):
        """없는 섹션이면 NO_DATA_MSG 반환"""
        insights = {"sections": {"summary": "text"}}
        result = _get_insight_section(insights, "nonexistent")
        self.assertEqual(result, NO_DATA_MSG)

    def test_none_insights_returns_no_data(self):
        """insights가 None이면 NO_DATA_MSG 반환"""
        result = _get_insight_section(None, "summary")
        self.assertEqual(result, NO_DATA_MSG)

    def test_empty_sections_returns_no_data(self):
        """sections가 빈 딕셔너리면 NO_DATA_MSG 반환"""
        result = _get_insight_section({"sections": {}}, "summary")
        self.assertEqual(result, NO_DATA_MSG)

    def test_none_value_returns_no_data(self):
        """섹션 값이 None이면 NO_DATA_MSG 반환"""
        insights = {"sections": {"summary": None}}
        result = _get_insight_section(insights, "summary")
        self.assertEqual(result, NO_DATA_MSG)


# ──────────────────────────────────────────────
# ReportData 기본값
# ──────────────────────────────────────────────


class TestReportData(unittest.TestCase):

    def test_default_values(self):
        """기본 ReportData의 필드들이 올바른 기본값"""
        data = ReportData()
        self.assertEqual(data.profile, {})
        self.assertTrue(data.enriched.empty)
        self.assertTrue(data.format_stats.empty)
        self.assertIsNone(data.categories)
        self.assertIsNone(data.caption_style)
        self.assertIsNone(data.sentiment)
        self.assertIsNone(data.audience_age)
        self.assertIsNone(data.visual)
        self.assertIsNone(data.top_posts)
        self.assertIsNone(data.insights)


# ──────────────────────────────────────────────
# _load_report_data (빈 디렉토리)
# ──────────────────────────────────────────────


class TestLoadReportData(unittest.TestCase):

    def test_empty_directory(self):
        """빈 디렉토리에서 ReportData 로딩 — 에러 없이 빈 데이터 반환"""
        tmp = Path("/tmp/test_reporter_load_data")
        raw_dir = tmp / "raw"
        analysis_dir = tmp / "analysis"
        raw_dir.mkdir(parents=True, exist_ok=True)
        analysis_dir.mkdir(parents=True, exist_ok=True)

        try:
            data = _load_report_data(tmp)
            self.assertEqual(data.profile, {})
            self.assertTrue(data.enriched.empty)
            self.assertIsNone(data.categories)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_with_profile_only(self):
        """프로필만 있는 경우 정상 로딩"""
        tmp = Path("/tmp/test_reporter_profile_only")
        raw_dir = tmp / "raw"
        analysis_dir = tmp / "analysis"
        raw_dir.mkdir(parents=True, exist_ok=True)
        analysis_dir.mkdir(parents=True, exist_ok=True)

        try:
            profile = {"username": "testuser", "followers": 1000}
            (raw_dir / "profile.json").write_text(
                json.dumps(profile, ensure_ascii=False), encoding="utf-8"
            )
            data = _load_report_data(tmp)
            self.assertEqual(data.profile["username"], "testuser")
            self.assertEqual(data.profile["followers"], 1000)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ──────────────────────────────────────────────
# 차트 — 빈 데이터 시 None 반환
# ──────────────────────────────────────────────


class TestChartEmptyData(unittest.TestCase):

    def setUp(self):
        self.charts_dir = Path("/tmp/test_reporter_charts")
        self.charts_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.charts_dir, ignore_errors=True)
        import matplotlib.pyplot as plt
        plt.close("all")

    def test_format_comparison_empty(self):
        """빈 format_stats → None 반환"""
        result = _chart_format_comparison(pd.DataFrame(), self.charts_dir)
        self.assertIsNone(result)

    def test_category_pie_empty(self):
        """categories가 None → None 반환"""
        result = _chart_category_pie(pd.DataFrame(), None, self.charts_dir)
        self.assertIsNone(result)

    def test_posting_frequency_empty(self):
        """빈 enriched → None 반환"""
        result = _chart_posting_frequency(pd.DataFrame(), self.charts_dir)
        self.assertIsNone(result)

    def test_day_of_week_empty(self):
        """빈 enriched → None 반환"""
        result = _chart_day_of_week(pd.DataFrame(), self.charts_dir)
        self.assertIsNone(result)

    def test_sentiment_pie_empty(self):
        """sentiment가 None → None 반환"""
        result = _chart_sentiment_pie(None, self.charts_dir)
        self.assertIsNone(result)


# ──────────────────────────────────────────────
# generate_report — 빈 데이터로 전체 PPT 생성
# ──────────────────────────────────────────────


class TestGenerateReport(unittest.TestCase):

    def test_empty_data_generates_pptx(self):
        """빈 데이터로 PPT 생성 — 에러 없이 파일 생성"""
        tmp = Path("/tmp/test_reporter_generate")
        raw_dir = tmp / "raw"
        analysis_dir = tmp / "analysis"
        raw_dir.mkdir(parents=True, exist_ok=True)
        analysis_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "images").mkdir(exist_ok=True)

        try:
            output = generate_report("test_channel", tmp)
            self.assertTrue(output.exists())
            self.assertTrue(output.name.endswith(".pptx"))
            # 파일 크기가 0보다 큰지 확인
            self.assertGreater(output.stat().st_size, 0)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            import matplotlib.pyplot as plt
            plt.close("all")

    def test_with_profile_data(self):
        """프로필 데이터가 있는 경우 PPT 생성"""
        tmp = Path("/tmp/test_reporter_with_profile")
        raw_dir = tmp / "raw"
        analysis_dir = tmp / "analysis"
        raw_dir.mkdir(parents=True, exist_ok=True)
        analysis_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "images").mkdir(exist_ok=True)

        try:
            profile = {
                "username": "test_channel",
                "full_name": "테스트 채널",
                "followers": 50000,
                "followees": 200,
                "biography": "테스트 바이오",
                "external_url": "https://example.com",
                "mediacount": 150,
                "is_verified": False,
                "business_category_name": "Food & Beverage",
                "collected_at": "2026-01-01T00:00:00Z",
            }
            (raw_dir / "profile.json").write_text(
                json.dumps(profile, ensure_ascii=False), encoding="utf-8"
            )

            output = generate_report("test_channel", tmp)
            self.assertTrue(output.exists())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            import matplotlib.pyplot as plt
            plt.close("all")


if __name__ == "__main__":
    unittest.main()
