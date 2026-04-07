"""
인스타그램 채널 분석기 — CLI 진입점

사용법:
    python main.py @channel_name              # 전체 파이프라인 (댓글 제외)
    python main.py @channel_name --no-ai      # AI 분석 없이 수집+통계만
    python main.py @channel_name --with-comments  # 댓글 포함 수집
    python main.py @channel_name --skip-collect  # 수집 건너뛰고 기존 데이터 분석
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    """CLI 인자 파싱"""
    parser = argparse.ArgumentParser(
        description="인스타그램 채널 분석기 — 공개 데이터 기반 경쟁 채널 분석 보고서 생성"
    )
    parser.add_argument(
        "channel",
        type=str,
        help="분석할 인스타그램 채널명 (예: @omuk_food)",
    )
    parser.add_argument(
        "--no-ai",
        action="store_true",
        help="AI 분석 건너뛰기 — 수집 + 통계만 실행 (API 비용 $0)",
    )
    parser.add_argument(
        "--ai-text-only",
        action="store_true",
        help="Vision(이미지) 분석 제외 — 텍스트 AI 분석만 실행",
    )
    parser.add_argument(
        "--skip-collect",
        action="store_true",
        help="데이터 수집 건너뛰기 — 기존 raw/ 데이터로 분석만 재실행",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Google Drive 업로드 생략",
    )
    parser.add_argument(
        "--with-comments",
        action="store_true",
        help="댓글 수집 활성화 (기본 비활성화 — Instagram API 제한으로 401 에러 빈발)",
    )
    parser.add_argument(
        "--industry",
        type=str,
        default=None,
        help="업종 프리셋 (food|beauty|fashion|auto|custom:파일경로)",
    )
    return parser.parse_args()


def ensure_dirs(channel: str) -> Path:
    """파이프라인에 필요한 디렉토리 구조 생성

    Args:
        channel: 정규화된 채널명 (@없이)

    Returns:
        data/{channel}/ 경로
    """
    base = Path("data") / channel
    subdirs = [
        base / "raw" / "images",
        base / "analysis",
        base / "report" / "charts",
        base / "report" / "assets",
    ]
    for d in subdirs:
        d.mkdir(parents=True, exist_ok=True)
    return base


def setup_logging(data_dir: Path) -> None:
    """로깅 설정 — 파일(DEBUG) + stderr(INFO)

    Args:
        data_dir: data/{channel}/ 경로 (pipeline.log 저장 위치)
    """
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # 기존 핸들러 제거 (중복 방지)
    root_logger.handlers.clear()

    # 파일 핸들러 — DEBUG 레벨, 전체 로그 기록
    file_handler = logging.FileHandler(
        data_dir / "pipeline.log", encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(file_handler)

    # instagrapi 내부 로그 억제 (HTTP 요청마다 DEBUG 로그 → 파일 비대화 방지)
    logging.getLogger("instagrapi").setLevel(logging.WARNING)
    logging.getLogger("public_request").setLevel(logging.WARNING)
    logging.getLogger("private_request").setLevel(logging.WARNING)

    # stderr 핸들러 — INFO 레벨, 사용자에게 진행 상황 표시
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(stream_handler)


def main() -> None:
    """메인 파이프라인 실행"""
    args = parse_args()
    channel = args.channel.lstrip("@")

    # 디렉토리 생성 및 로깅 설정
    data_dir = ensure_dirs(channel)
    setup_logging(data_dir)

    logger = logging.getLogger("main")
    logger.info("파이프라인 시작 — @%s", channel)

    # 1단계: 데이터 수집
    if not args.skip_collect:
        from collector import collect

        success = collect(channel, data_dir, with_comments=args.with_comments)
        if not success:
            logger.error("데이터 수집 실패 — @%s", channel)
            sys.exit(1)
        logger.info("데이터 수집 완료 — @%s", channel)
    else:
        # --skip-collect: 기존 데이터 존재 확인
        raw_dir = data_dir / "raw"
        required = [raw_dir / "profile.json", raw_dir / "posts.csv"]
        for f in required:
            if not f.exists():
                logger.error("필수 파일 없음: %s (--skip-collect 사용 시 기존 데이터 필요)", f)
                sys.exit(1)
        logger.info("기존 수집 데이터 사용 — @%s", channel)

    # 1.5단계: 추정치 산출 (항상 실행 — 순수 계산, API 비용 $0)
    from estimator import load_coefficients, enrich_posts, aggregate_by_format

    coeffs = load_coefficients()
    with open(data_dir / "raw" / "profile.json", encoding="utf-8") as f:
        profile = json.load(f)
    posts_df = pd.read_csv(data_dir / "raw" / "posts.csv")

    enriched = enrich_posts(posts_df, profile["followers"], coeffs)
    enriched.to_csv(
        data_dir / "analysis" / "posts_enriched.csv",
        index=False,
        encoding="utf-8-sig",
    )

    format_stats = aggregate_by_format(enriched)
    format_stats.to_csv(
        data_dir / "analysis" / "format_stats.csv",
        index=False,
        encoding="utf-8-sig",
    )
    logger.info("추정치 산출 완료 → analysis/posts_enriched.csv, format_stats.csv")

    # 2단계: AI 분석 (--no-ai면 건너뜀)
    if not args.no_ai:
        logger.info("AI 분석 단계 — 아직 미구현 (Step 3에서 추가 예정)")
    else:
        logger.info("--no-ai 모드: AI 분석 건너뜀")

    logger.info("파이프라인 완료 — @%s", channel)


if __name__ == "__main__":
    main()
