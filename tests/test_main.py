"""
main.py 테스트

테스트 대상:
    - parse_args: 인자 파싱 (채널명, 플래그 조합)
    - ensure_dirs: 디렉토리 구조 생성
    - setup_logging: 로거 핸들러 구성
    - 채널명 정규화 (@제거)
"""

import logging
import shutil
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from main import ensure_dirs, parse_args, setup_logging


# ──────────────────────────────────────────────
# parse_args
# ──────────────────────────────────────────────


class TestParseArgs(unittest.TestCase):

    @patch("sys.argv", ["main.py", "@omuk_food"])
    def test_basic_channel(self):
        """기본 채널 인자만"""
        args = parse_args()
        self.assertEqual(args.channel, "@omuk_food")
        self.assertFalse(args.no_ai)
        self.assertFalse(args.skip_collect)
        self.assertFalse(args.no_upload)
        self.assertFalse(args.with_comments)
        self.assertFalse(args.force_reanalyze)
        self.assertIsNone(args.industry)

    @patch("sys.argv", ["main.py", "@test", "--no-ai"])
    def test_no_ai_flag(self):
        args = parse_args()
        self.assertTrue(args.no_ai)
        self.assertFalse(args.ai_text_only)

    @patch("sys.argv", ["main.py", "@test", "--ai-text-only"])
    def test_ai_text_only_flag(self):
        args = parse_args()
        self.assertTrue(args.ai_text_only)
        self.assertFalse(args.no_ai)

    @patch("sys.argv", ["main.py", "@test", "--skip-collect"])
    def test_skip_collect_flag(self):
        args = parse_args()
        self.assertTrue(args.skip_collect)

    @patch("sys.argv", ["main.py", "@test", "--no-upload"])
    def test_no_upload_flag(self):
        args = parse_args()
        self.assertTrue(args.no_upload)

    @patch("sys.argv", ["main.py", "@test", "--with-comments"])
    def test_with_comments_flag(self):
        args = parse_args()
        self.assertTrue(args.with_comments)

    @patch("sys.argv", ["main.py", "@test", "--industry", "food"])
    def test_industry_option(self):
        args = parse_args()
        self.assertEqual(args.industry, "food")

    @patch("sys.argv", ["main.py", "@test", "--force-reanalyze"])
    def test_force_reanalyze_flag(self):
        args = parse_args()
        self.assertTrue(args.force_reanalyze)

    @patch("sys.argv", [
        "main.py", "@test", "--no-ai", "--skip-collect", "--no-upload"
    ])
    def test_multiple_flags_combined(self):
        """여러 플래그 동시 사용"""
        args = parse_args()
        self.assertTrue(args.no_ai)
        self.assertTrue(args.skip_collect)
        self.assertTrue(args.no_upload)


# ──────────────────────────────────────────────
# 채널명 정규화
# ──────────────────────────────────────────────


class TestChannelNormalize(unittest.TestCase):

    def test_strip_at_sign(self):
        """@ 제거"""
        self.assertEqual("omuk_food", "@omuk_food".lstrip("@"))

    def test_no_at_sign(self):
        """@ 없는 경우 그대로"""
        self.assertEqual("omuk_food", "omuk_food".lstrip("@"))

    def test_multiple_at_signs(self):
        """여러 @ 제거"""
        self.assertEqual("channel", "@@channel".lstrip("@"))


# ──────────────────────────────────────────────
# ensure_dirs
# ──────────────────────────────────────────────


class TestEnsureDirs(unittest.TestCase):

    def setUp(self):
        self.test_base = Path("/tmp/test_main_ensure_dirs")
        if self.test_base.exists():
            shutil.rmtree(self.test_base)

    def tearDown(self):
        # data/ 디렉토리 정리
        data_dir = Path("data/test_channel_xyz")
        if data_dir.exists():
            shutil.rmtree(data_dir)
        # data/ 폴더가 비어있으면 삭제
        data_root = Path("data")
        if data_root.exists() and not any(data_root.iterdir()):
            data_root.rmdir()

    def test_creates_all_subdirs(self):
        """모든 하위 디렉토리 생성"""
        result = ensure_dirs("test_channel_xyz")
        self.assertEqual(result, Path("data/test_channel_xyz"))

        expected_dirs = [
            result / "raw" / "images",
            result / "analysis",
            result / "report" / "charts",
            result / "report" / "assets",
        ]
        for d in expected_dirs:
            self.assertTrue(d.exists(), f"디렉토리 없음: {d}")

    def test_returns_base_path(self):
        """data/{channel} 경로 반환"""
        result = ensure_dirs("test_channel_xyz")
        self.assertEqual(result, Path("data/test_channel_xyz"))

    def test_idempotent(self):
        """중복 호출해도 에러 없음"""
        ensure_dirs("test_channel_xyz")
        ensure_dirs("test_channel_xyz")  # 두 번째 호출도 성공


# ──────────────────────────────────────────────
# setup_logging
# ──────────────────────────────────────────────


class TestSetupLogging(unittest.TestCase):

    def setUp(self):
        self.tmp = Path("/tmp/test_main_logging")
        self.tmp.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        # 로거 핸들러 정리
        root = logging.getLogger()
        root.handlers.clear()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_creates_log_file(self):
        """pipeline.log 파일 생성"""
        setup_logging(self.tmp)
        log_file = self.tmp / "pipeline.log"
        # 로그 메시지 하나 기록해서 파일 생성 확인
        logging.getLogger("test").info("test message")
        self.assertTrue(log_file.exists())

    def test_adds_two_handlers(self):
        """파일 + stderr 핸들러 2개 등록"""
        setup_logging(self.tmp)
        root = logging.getLogger()
        self.assertEqual(len(root.handlers), 2)

    def test_root_level_debug(self):
        """루트 로거 레벨 DEBUG"""
        setup_logging(self.tmp)
        root = logging.getLogger()
        self.assertEqual(root.level, logging.DEBUG)

    def test_instagrapi_logger_suppressed(self):
        """instagrapi 로거 WARNING 레벨"""
        setup_logging(self.tmp)
        ig_logger = logging.getLogger("instagrapi")
        self.assertEqual(ig_logger.level, logging.WARNING)


if __name__ == "__main__":
    unittest.main()
