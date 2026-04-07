"""
파이프라인 백그라운드 워커 — CustomTkinter 데스크톱 앱용

app.py의 run_pipeline() 로직을 threading.Thread로 이식.
Streamlit 의존성 없이 queue.Queue를 통해 UI에 진행 상태 전달.
"""

import json
import logging
import os
import queue
import sys
import threading
from pathlib import Path

import pandas as pd

# ──────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("desktop_worker")

logging.getLogger("instagrapi").setLevel(logging.WARNING)
logging.getLogger("public_request").setLevel(logging.WARNING)
logging.getLogger("private_request").setLevel(logging.WARNING)


# ──────────────────────────────────────────────
# 공통 유틸
# ──────────────────────────────────────────────
def ensure_dirs(channel: str) -> Path:
    """파이프라인 디렉토리 구조 생성"""
    base = Path("data") / channel
    for d in [
        base / "raw" / "images",
        base / "analysis",
        base / "report" / "charts",
        base / "report" / "assets",
    ]:
        d.mkdir(parents=True, exist_ok=True)
    return base


def setup_file_logging(data_dir: Path) -> None:
    """파일 로깅 핸들러 추가 (중복 방지)"""
    root = logging.getLogger()
    log_path = data_dir / "pipeline.log"
    for h in root.handlers:
        if isinstance(h, logging.FileHandler) and str(log_path) in str(h.baseFilename):
            return
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root.addHandler(fh)


def apply_credentials(
    api_key: str,
    ig_username: str,
    ig_password: str,
) -> None:
    """UI에서 입력받은 자격증명을 환경변수/config.yaml에 반영"""
    if api_key:
        os.environ["ANTHROPIC_API_KEY"] = api_key

    if ig_username and ig_password:
        config_path = Path("config/config.yaml")
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_content = (
            "instagram:\n"
            f'  username: "{ig_username}"\n'
            f'  password: "{ig_password}"\n'
            '  session_file: "config/session.json"\n'
            "  max_posts: 20\n"
            "  delay_min: 2\n"
            "  delay_max: 4\n"
            "  comment_delay_min: 1\n"
            "  comment_delay_max: 2\n"
            "  retry_on_429: 3\n"
            "  retry_wait_base: 60\n"
        )
        config_path.write_text(config_content, encoding="utf-8")


# ──────────────────────────────────────────────
# 파이프라인 워커 스레드
# ──────────────────────────────────────────────
class PipelineWorker(threading.Thread):
    """백그라운드에서 분석 파이프라인을 실행하고 진행 상태를 큐로 전달.

    진행 메시지 형식: (stage: str, percent: int, message: str)
        - stage: "collect" | "estimate" | "analyze" | "report" | "upload" | "done" | "error"
        - percent: 0-100 (진행률), -1은 에러
        - message: UI에 표시할 한국어 메시지 (완료 시 report 경로 문자열)
    """

    def __init__(self, config: dict, progress_queue: queue.Queue) -> None:
        super().__init__(daemon=True)
        self.config = config
        self.q = progress_queue

    def _put(self, stage: str, percent: int, message: str) -> None:
        self.q.put((stage, percent, message))

    def run(self) -> None:
        cfg = self.config
        channel = cfg["channel"]
        industry = cfg["industry"]
        use_ai = cfg["use_ai"]
        use_vision = cfg["use_vision"]
        with_comments = cfg["with_comments"]
        upload_drive = cfg["upload_drive"]
        force_reanalyze = cfg["force_reanalyze"]

        data_dir = ensure_dirs(channel)
        setup_file_logging(data_dir)

        report_path = None

        try:
            # ── 1단계: 데이터 수집 ──
            self._put("collect", 5, "데이터 수집 중...")

            from collector import collect

            success = collect(channel, data_dir, with_comments=with_comments)
            if not success:
                self._put("error", -1, "데이터 수집 실패")
                return

            self._put("collect", 30, "데이터 수집 완료")

            # ── 1.5단계: 추정치 산출 ──
            self._put("estimate", 35, "추정치 산출 중...")

            from estimator import load_coefficients, enrich_posts, aggregate_by_format

            coeffs = load_coefficients()
            with open(data_dir / "raw" / "profile.json", encoding="utf-8") as f:
                profile = json.load(f)
            posts_df = pd.read_csv(data_dir / "raw" / "posts.csv")

            enriched = enrich_posts(posts_df, profile["followers"], coeffs)
            enriched.to_csv(
                data_dir / "analysis" / "posts_enriched.csv",
                index=False, encoding="utf-8-sig",
            )
            format_stats = aggregate_by_format(enriched)
            format_stats.to_csv(
                data_dir / "analysis" / "format_stats.csv",
                index=False, encoding="utf-8-sig",
            )

            self._put("estimate", 40, "추정치 산출 완료")

            # ── 2단계: AI 분석 ──
            if use_ai:
                self._put("analyze", 45, "AI 분석 중...")

                from analyzer import run_analysis

                text_only = not use_vision
                try:
                    analysis_results = run_analysis(
                        channel=channel,
                        data_dir=data_dir,
                        industry=industry,
                        text_only=text_only,
                        force=force_reanalyze,
                    )
                    completed = sum(1 for v in analysis_results.values() if v is not None)
                    total = len(analysis_results)
                    self._put("analyze", 70, f"AI 분석 완료 ({completed}/{total} 태스크)")
                except Exception as e:
                    self._put("analyze", 70, f"AI 분석 일부 실패: {e}")
            else:
                self._put("analyze", 70, "AI 분석 건너뜀 (비활성화)")

            # ── 3단계: 보고서 생성 ──
            self._put("report", 75, "보고서 생성 중...")

            from reporter import generate_report

            try:
                report_path = generate_report(channel, data_dir)
                self._put("report", 90, "보고서 생성 완료")
            except Exception as e:
                self._put("report", 90, f"보고서 생성 실패: {e}")

            # ── 4단계: Google Drive 업로드 ──
            if upload_drive:
                self._put("upload", 92, "Google Drive 업로드 중...")

                from drive_uploader import upload_to_drive

                try:
                    drive_ok = upload_to_drive(channel, data_dir)
                    if drive_ok:
                        self._put("upload", 98, "Google Drive 업로드 완료")
                    else:
                        self._put("upload", 98, "Google Drive 업로드 실패 (로컬 파일 유지)")
                except Exception as e:
                    self._put("upload", 98, f"Google Drive 업로드 오류: {e}")
            else:
                self._put("upload", 98, "Google Drive 업로드 건너뜀")

            # 완료
            result = str(report_path) if report_path else ""
            self._put("done", 100, result)

        except Exception as e:
            logger.error("파이프라인 오류: %s", e)
            self._put("error", -1, f"오류 발생: {e}")
