"""
collector.py 테스트

테스트 대상:
    - _load_config: 설정 로드, 기본값 폴백, 잘못된 YAML 처리
    - _retry_on_error: 지수 백오프 재시도 로직
    - _MEDIA_TYPE_MAP: 미디어 타입 매핑
    - _cleanup_temp_files: 임시 파일 정리
    - collect: 비공개 계정 거부, 댓글 플래그, 프로필 실패 처리
"""

import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# instagrapi가 설치되어 있지 않을 수 있으므로 mock 모듈 생성
_instagrapi_mock = types.ModuleType("instagrapi")
_instagrapi_mock.Client = MagicMock

_exceptions_mock = types.ModuleType("instagrapi.exceptions")
_exceptions_mock.ChallengeRequired = type("ChallengeRequired", (Exception,), {})
_exceptions_mock.LoginRequired = type("LoginRequired", (Exception,), {})
_exceptions_mock.PleaseWaitFewMinutes = type("PleaseWaitFewMinutes", (Exception,), {})
_exceptions_mock.RateLimitError = type("RateLimitError", (Exception,), {})

sys.modules.setdefault("instagrapi", _instagrapi_mock)
sys.modules.setdefault("instagrapi.exceptions", _exceptions_mock)

from collector import (
    _DEFAULT_CONFIG,
    _MEDIA_TYPE_MAP,
    _cleanup_temp_files,
    _load_config,
    _retry_on_error,
    collect,
)
from instagrapi.exceptions import RateLimitError


# ──────────────────────────────────────────────
# _MEDIA_TYPE_MAP
# ──────────────────────────────────────────────


class TestMediaTypeMap(unittest.TestCase):
    """미디어 타입 매핑 검증"""

    def test_image(self):
        self.assertEqual(_MEDIA_TYPE_MAP[1], "GraphImage")

    def test_video(self):
        self.assertEqual(_MEDIA_TYPE_MAP[2], "GraphVideo")

    def test_sidecar(self):
        self.assertEqual(_MEDIA_TYPE_MAP[8], "GraphSidecar")

    def test_unknown_type_not_in_map(self):
        self.assertNotIn(99, _MEDIA_TYPE_MAP)


# ──────────────────────────────────────────────
# _load_config
# ──────────────────────────────────────────────


class TestLoadConfig(unittest.TestCase):
    """설정 로드 테스트"""

    @patch("collector.Path")
    def test_missing_file_returns_default(self, mock_path_cls):
        """config.yaml이 없으면 기본 설정 반환"""
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = False
        mock_path_cls.return_value = mock_path_instance

        result = _load_config()
        self.assertEqual(result, _DEFAULT_CONFIG)

    @patch("builtins.open")
    @patch("collector.Path")
    def test_valid_config_merges_with_defaults(self, mock_path_cls, mock_open):
        """유효한 config.yaml은 기본값과 병합"""
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = True
        mock_path_cls.return_value = mock_path_instance

        yaml_content = "instagram:\n  username: testuser\n  max_posts: 100\n"
        mock_open.return_value.__enter__ = MagicMock(
            return_value=__import__("io").StringIO(yaml_content)
        )
        mock_open.return_value.__exit__ = MagicMock(return_value=False)

        result = _load_config()
        self.assertEqual(result["instagram"]["username"], "testuser")
        self.assertEqual(result["instagram"]["max_posts"], 100)
        # 누락된 키는 기본값으로 보완
        self.assertEqual(result["instagram"]["delay_min"], 2)

    @patch("builtins.open")
    @patch("collector.Path")
    def test_missing_instagram_section_returns_default(self, mock_path_cls, mock_open):
        """instagram 섹션이 없으면 기본값 반환"""
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = True
        mock_path_cls.return_value = mock_path_instance

        yaml_content = "other_section:\n  key: value\n"
        mock_open.return_value.__enter__ = MagicMock(
            return_value=__import__("io").StringIO(yaml_content)
        )
        mock_open.return_value.__exit__ = MagicMock(return_value=False)

        result = _load_config()
        self.assertEqual(result, _DEFAULT_CONFIG)


# ──────────────────────────────────────────────
# _retry_on_error
# ──────────────────────────────────────────────


class TestRetryOnError(unittest.TestCase):
    """재시도 로직 테스트"""

    def test_success_on_first_try(self):
        """첫 번째 시도에서 성공하면 결과 반환"""
        func = MagicMock(return_value="ok")
        result = _retry_on_error(func, 3, 0)  # base_wait=0으로 딜레이 제거
        self.assertEqual(result, "ok")
        func.assert_called_once()

    @patch("collector.time.sleep")
    def test_success_on_retry(self, mock_sleep):
        """실패 후 재시도에서 성공"""
        func = MagicMock(side_effect=[RateLimitError("429"), "ok"])
        result = _retry_on_error(func, 3, 1)
        self.assertEqual(result, "ok")
        self.assertEqual(func.call_count, 2)
        mock_sleep.assert_called_once_with(1)  # base_wait * 2^0

    @patch("collector.time.sleep")
    def test_exponential_backoff(self, mock_sleep):
        """지수 백오프 대기 시간 검증"""
        func = MagicMock(
            side_effect=[RateLimitError("1"), RateLimitError("2"), "ok"]
        )
        _retry_on_error(func, 3, 10)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(10)   # 10 * 2^0
        mock_sleep.assert_any_call(20)   # 10 * 2^1

    @patch("collector.time.sleep")
    def test_all_retries_exhausted(self, mock_sleep):
        """모든 재시도 소진 시 예외 발생"""
        func = MagicMock(side_effect=RateLimitError("429"))
        with self.assertRaises(RateLimitError):
            _retry_on_error(func, 2, 0)
        # 첫 시도 + 2회 재시도 = 3회
        self.assertEqual(func.call_count, 3)

    @patch("collector.time.sleep")
    def test_connection_error_triggers_retry(self, mock_sleep):
        """ConnectionError도 재시도 대상"""
        func = MagicMock(side_effect=[ConnectionError("net"), "ok"])
        result = _retry_on_error(func, 3, 0)
        self.assertEqual(result, "ok")
        self.assertEqual(func.call_count, 2)

    def test_non_retryable_error_raises_immediately(self):
        """재시도 대상이 아닌 예외는 즉시 발생"""
        func = MagicMock(side_effect=ValueError("bad"))
        with self.assertRaises(ValueError):
            _retry_on_error(func, 3, 0)
        func.assert_called_once()


# ──────────────────────────────────────────────
# _cleanup_temp_files
# ──────────────────────────────────────────────


class TestCleanupTempFiles(unittest.TestCase):
    """임시 파일 정리 테스트"""

    def test_removes_json_and_mp4(self, ):
        """JSON, MP4, .json.xz 파일을 삭제"""
        tmp_dir = Path("/tmp/test_cleanup_images")
        tmp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 삭제 대상
            (tmp_dir / "temp.json").write_text("{}")
            (tmp_dir / "video.mp4").write_bytes(b"\x00")
            (tmp_dir / "data.json.xz").write_bytes(b"\x00")
            # 유지 대상
            (tmp_dir / "photo.jpg").write_bytes(b"\xff\xd8")

            _cleanup_temp_files(tmp_dir)

            remaining = [f.name for f in tmp_dir.iterdir()]
            self.assertIn("photo.jpg", remaining)
            self.assertNotIn("temp.json", remaining)
            self.assertNotIn("video.mp4", remaining)
            self.assertNotIn("data.json.xz", remaining)
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ──────────────────────────────────────────────
# collect (메인 함수 — 통합 테스트)
# ──────────────────────────────────────────────


class TestCollect(unittest.TestCase):
    """collect() 메인 함수 테스트"""

    @patch("collector._download_images")
    @patch("collector._collect_posts")
    @patch("collector._retry_on_error")
    @patch("collector._create_client")
    @patch("collector._load_config")
    def test_private_account_returns_false(
        self, mock_config, mock_client, mock_retry, mock_posts, mock_images
    ):
        """비공개 계정이면 False 반환"""
        mock_config.return_value = _DEFAULT_CONFIG
        mock_client.return_value = MagicMock()
        mock_retry.return_value = {"is_private": True, "pk": "123"}

        tmp_dir = Path("/tmp/test_collect_private")
        raw_dir = tmp_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        try:
            result = collect("test_channel", tmp_dir)
            self.assertFalse(result)
            mock_posts.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @patch("collector._download_images")
    @patch("collector._collect_posts")
    @patch("collector._retry_on_error")
    @patch("collector._create_client")
    @patch("collector._load_config")
    def test_client_creation_failure_returns_false(
        self, mock_config, mock_client, mock_retry, mock_posts, mock_images
    ):
        """클라이언트 생성 실패 시 False 반환"""
        mock_config.return_value = _DEFAULT_CONFIG
        mock_client.side_effect = Exception("auth fail")

        tmp_dir = Path("/tmp/test_collect_auth_fail")
        raw_dir = tmp_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        try:
            result = collect("test_channel", tmp_dir)
            self.assertFalse(result)
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @patch("collector._download_images")
    @patch("collector._collect_posts")
    @patch("collector._retry_on_error")
    @patch("collector._create_client")
    @patch("collector._load_config")
    def test_no_posts_still_returns_true(
        self, mock_config, mock_client, mock_retry, mock_posts, mock_images
    ):
        """게시물이 없어도 프로필 수집 성공이면 True"""
        mock_config.return_value = _DEFAULT_CONFIG
        mock_client.return_value = MagicMock()
        mock_retry.return_value = {"is_private": False, "pk": "123"}
        mock_posts.return_value = []

        tmp_dir = Path("/tmp/test_collect_no_posts")
        raw_dir = tmp_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        try:
            result = collect("test_channel", tmp_dir)
            self.assertTrue(result)
            mock_images.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @patch("collector._collect_comments")
    @patch("collector._download_images")
    @patch("collector._collect_posts")
    @patch("collector._retry_on_error")
    @patch("collector._create_client")
    @patch("collector._load_config")
    def test_comments_skipped_by_default(
        self, mock_config, mock_client, mock_retry, mock_posts, mock_images, mock_comments
    ):
        """기본적으로 댓글 수집 건너뜀"""
        mock_config.return_value = _DEFAULT_CONFIG
        mock_client.return_value = MagicMock()
        mock_retry.return_value = {"is_private": False, "pk": "123"}
        mock_posts.return_value = [{"shortcode": "abc", "pk": "1", "comments": 5}]

        tmp_dir = Path("/tmp/test_collect_no_comments")
        raw_dir = tmp_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        try:
            collect("test_channel", tmp_dir, with_comments=False)
            mock_comments.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @patch("collector._collect_comments")
    @patch("collector._download_images")
    @patch("collector._collect_posts")
    @patch("collector._retry_on_error")
    @patch("collector._create_client")
    @patch("collector._load_config")
    def test_comments_collected_when_flag_set(
        self, mock_config, mock_client, mock_retry, mock_posts, mock_images, mock_comments
    ):
        """with_comments=True이면 댓글 수집"""
        mock_config.return_value = _DEFAULT_CONFIG
        mock_client.return_value = MagicMock()
        mock_retry.return_value = {"is_private": False, "pk": "123"}
        mock_posts.return_value = [{"shortcode": "abc", "pk": "1", "comments": 5}]

        tmp_dir = Path("/tmp/test_collect_with_comments")
        raw_dir = tmp_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        try:
            collect("test_channel", tmp_dir, with_comments=True)
            mock_comments.assert_called_once()
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ──────────────────────────────────────────────
# DEFAULT_CONFIG 기본값 검증
# ──────────────────────────────────────────────


class TestDefaultConfig(unittest.TestCase):
    """기본 설정값 검증"""

    def test_delay_range(self):
        """딜레이 범위가 2~4초"""
        ig = _DEFAULT_CONFIG["instagram"]
        self.assertEqual(ig["delay_min"], 2)
        self.assertEqual(ig["delay_max"], 4)

    def test_comment_delay_range(self):
        """댓글 딜레이 1~2초"""
        ig = _DEFAULT_CONFIG["instagram"]
        self.assertEqual(ig["comment_delay_min"], 1)
        self.assertEqual(ig["comment_delay_max"], 2)

    def test_retry_on_429(self):
        """429 재시도 횟수 3회"""
        self.assertEqual(_DEFAULT_CONFIG["instagram"]["retry_on_429"], 3)

    def test_retry_wait_base(self):
        """재시도 기본 대기 60초"""
        self.assertEqual(_DEFAULT_CONFIG["instagram"]["retry_wait_base"], 60)

    def test_max_posts_default(self):
        """기본 최대 게시물 수"""
        self.assertEqual(_DEFAULT_CONFIG["instagram"]["max_posts"], 20)


if __name__ == "__main__":
    unittest.main()
