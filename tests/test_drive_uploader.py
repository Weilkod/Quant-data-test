"""
drive_uploader.py 테스트

테스트 대상:
    - _load_drive_config: 기본값, config.yaml 로딩
    - _authenticate: credentials.json 없을 때 FileNotFoundError
    - _find_folder / _create_folder / _ensure_folder: 폴더 관리 로직
    - _ensure_folder_path: 전체 폴더 구조 생성
    - _upload_file: 재시도 로직 (3회 지수 백오프)
    - _upload_directory: 빈 디렉토리, 재귀 업로드
    - upload_to_drive: 인증 실패, 정상 흐름
    - 설정 기본값 (_RETRY_MAX, _RETRY_DELAYS)
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from drive_uploader import (
    _RETRY_DELAYS,
    _RETRY_MAX,
    _create_folder,
    _ensure_folder,
    _ensure_folder_path,
    _find_folder,
    _load_drive_config,
    _upload_directory,
    upload_to_drive,
)


# ──────────────────────────────────────────────
# 설정 기본값
# ──────────────────────────────────────────────


class TestRetryConfig(unittest.TestCase):

    def test_retry_max_is_3(self):
        self.assertEqual(_RETRY_MAX, 3)

    def test_retry_delays_exponential(self):
        self.assertEqual(_RETRY_DELAYS, [2, 4, 8])


# ──────────────────────────────────────────────
# _load_drive_config
# ──────────────────────────────────────────────


class TestLoadDriveConfig(unittest.TestCase):

    def test_missing_config_returns_defaults(self):
        """config.yaml 없을 때 기본값 반환"""
        fake_path = MagicMock()
        fake_path.exists.return_value = False
        with patch("drive_uploader._CONFIG_PATH", fake_path):
            config = _load_drive_config()
        self.assertEqual(config["credentials_file"], "config/credentials.json")
        self.assertEqual(config["folder_name"], "Instagram Channel Analysis")


# ──────────────────────────────────────────────
# _find_folder
# ──────────────────────────────────────────────


class TestFindFolder(unittest.TestCase):

    def test_folder_found(self):
        """기존 폴더 발견 시 ID 반환"""
        service = MagicMock()
        service.files().list().execute.return_value = {
            "files": [{"id": "folder_123", "name": "test"}]
        }
        result = _find_folder(service, "test")
        self.assertEqual(result, "folder_123")

    def test_folder_not_found(self):
        """폴더 없으면 None 반환"""
        service = MagicMock()
        service.files().list().execute.return_value = {"files": []}
        result = _find_folder(service, "nonexistent")
        self.assertIsNone(result)


# ──────────────────────────────────────────────
# _create_folder
# ──────────────────────────────────────────────


class TestCreateFolder(unittest.TestCase):

    def test_creates_folder_with_name(self):
        """폴더 생성 후 ID 반환"""
        service = MagicMock()
        service.files().create().execute.return_value = {"id": "new_folder_id"}
        result = _create_folder(service, "new_folder")
        self.assertEqual(result, "new_folder_id")

    def test_creates_folder_with_parent(self):
        """부모 폴더 ID가 있을 때 parents 메타데이터 포함"""
        service = MagicMock()
        service.files().create().execute.return_value = {"id": "child_id"}
        result = _create_folder(service, "child", parent_id="parent_id")
        self.assertEqual(result, "child_id")


# ──────────────────────────────────────────────
# _ensure_folder
# ──────────────────────────────────────────────


class TestEnsureFolder(unittest.TestCase):

    @patch("drive_uploader._create_folder")
    @patch("drive_uploader._find_folder")
    def test_existing_folder_not_recreated(self, mock_find, mock_create):
        """기존 폴더가 있으면 생성하지 않음"""
        mock_find.return_value = "existing_id"
        service = MagicMock()
        result = _ensure_folder(service, "test")
        self.assertEqual(result, "existing_id")
        mock_create.assert_not_called()

    @patch("drive_uploader._create_folder")
    @patch("drive_uploader._find_folder")
    def test_missing_folder_created(self, mock_find, mock_create):
        """폴더가 없으면 새로 생성"""
        mock_find.return_value = None
        mock_create.return_value = "new_id"
        service = MagicMock()
        result = _ensure_folder(service, "test")
        self.assertEqual(result, "new_id")
        mock_create.assert_called_once()


# ──────────────────────────────────────────────
# _ensure_folder_path
# ──────────────────────────────────────────────


class TestEnsureFolderPath(unittest.TestCase):

    @patch("drive_uploader._ensure_folder")
    def test_creates_full_structure(self, mock_ensure):
        """root/channel/raw,analysis,report 5개 폴더 생성"""
        mock_ensure.side_effect = [
            "root_id", "channel_id", "raw_id", "analysis_id", "report_id"
        ]
        service = MagicMock()
        result = _ensure_folder_path(service, "Root", "test_channel")

        self.assertEqual(result["root"], "root_id")
        self.assertEqual(result["channel"], "channel_id")
        self.assertEqual(result["raw"], "raw_id")
        self.assertEqual(result["analysis"], "analysis_id")
        self.assertEqual(result["report"], "report_id")
        self.assertEqual(mock_ensure.call_count, 5)


# ──────────────────────────────────────────────
# _upload_directory
# ──────────────────────────────────────────────


class TestUploadDirectory(unittest.TestCase):

    def test_nonexistent_directory_returns_zero(self):
        """존재하지 않는 디렉토리 → 0 반환"""
        service = MagicMock()
        result = _upload_directory(service, Path("/tmp/nonexistent_dir_99999"), "parent_id")
        self.assertEqual(result, 0)

    @patch("drive_uploader._upload_file")
    def test_empty_directory_returns_zero(self, mock_upload):
        """빈 디렉토리 → 0 반환"""
        import shutil
        tmp = Path("/tmp/test_drive_empty_dir")
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            service = MagicMock()
            result = _upload_directory(service, tmp, "parent_id")
            self.assertEqual(result, 0)
            mock_upload.assert_not_called()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @patch("drive_uploader._upload_file")
    def test_skips_hidden_files(self, mock_upload):
        """숨김 파일(.으로 시작) 건너뜀"""
        import shutil
        tmp = Path("/tmp/test_drive_hidden")
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            (tmp / ".hidden").write_text("secret")
            (tmp / "visible.json").write_text("{}")
            mock_upload.return_value = "file_id"

            service = MagicMock()
            result = _upload_directory(service, tmp, "parent_id")
            self.assertEqual(result, 1)  # visible.json만 업로드
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ──────────────────────────────────────────────
# upload_to_drive (메인 함수)
# ──────────────────────────────────────────────


class TestUploadToDrive(unittest.TestCase):

    @patch("drive_uploader._authenticate")
    @patch("drive_uploader._load_drive_config")
    def test_auth_failure_returns_false(self, mock_config, mock_auth):
        """인증 실패 시 False 반환"""
        mock_config.return_value = {
            "credentials_file": "nonexistent.json",
            "folder_name": "Test",
        }
        mock_auth.side_effect = FileNotFoundError("no creds")

        result = upload_to_drive("test", Path("/tmp/test_upload"))
        self.assertFalse(result)

    @patch("drive_uploader._upload_directory")
    @patch("drive_uploader._ensure_folder_path")
    @patch("drive_uploader._authenticate")
    @patch("drive_uploader._load_drive_config")
    def test_successful_upload(self, mock_config, mock_auth, mock_folders, mock_upload_dir):
        """정상 업로드 흐름"""
        mock_config.return_value = {
            "credentials_file": "creds.json",
            "folder_name": "Test",
        }
        mock_auth.return_value = MagicMock()
        mock_folders.return_value = {
            "root": "r", "channel": "c", "raw": "rw", "analysis": "an", "report": "rp"
        }
        mock_upload_dir.return_value = 5

        result = upload_to_drive("test_channel", Path("/tmp/test_upload"))
        self.assertTrue(result)
        # raw, analysis, report 3번 호출
        self.assertEqual(mock_upload_dir.call_count, 3)


if __name__ == "__main__":
    unittest.main()
