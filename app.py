"""
인스타그램 채널 분석기 — Streamlit UI

CLAUDE.md 규칙:
    - steps 1-6 모듈 재사용 (collector, estimator, analyzer, reporter, drive_uploader)
    - UI: 채널 입력, 업종 드롭다운, 체크박스 옵션, 진행 바, 다운로드 버튼
    - Wireframe: INSTA.md D-1
"""

import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# ──────────────────────────────────────────────
# Streamlit secrets → 환경변수/config 동기화
# ──────────────────────────────────────────────
def _sync_secrets() -> None:
    """Streamlit Cloud secrets를 환경변수 및 config.yaml에 반영

    secrets.toml 또는 Streamlit Cloud 대시보드에 설정된 시크릿을
    각 모듈이 읽을 수 있도록 환경변수와 config 파일로 전달합니다.
    """
    # Claude API 키 → 환경변수 (anthropic 라이브러리가 자동 인식)
    if "ANTHROPIC_API_KEY" in st.secrets:
        os.environ["ANTHROPIC_API_KEY"] = st.secrets["ANTHROPIC_API_KEY"]

    # Instagram 계정 → config.yaml 자동 생성 (collector.py가 읽음)
    if "instagram" in st.secrets:
        config_path = Path("config/config.yaml")
        if not config_path.exists():
            config_path.parent.mkdir(parents=True, exist_ok=True)
            ig = st.secrets["instagram"]
            config_content = (
                "instagram:\n"
                f'  username: "{ig.get("username", "")}"\n'
                f'  password: "{ig.get("password", "")}"\n'
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


try:
    _sync_secrets()
except Exception:
    pass  # secrets 미설정 시 무시 (로컬 실행 등)


def _apply_credentials(
    api_key: str,
    ig_username: str,
    ig_password: str,
) -> None:
    """UI에서 입력받은 자격증명을 환경변수/config.yaml에 반영"""
    # Anthropic API 키 → 환경변수 (anthropic 라이브러리가 자동 인식)
    if api_key:
        os.environ["ANTHROPIC_API_KEY"] = api_key

    # Instagram 계정 → config.yaml 기록 (collector.py가 읽음)
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
# 로깅 설정
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("app")

# instagrapi 내부 로그 억제
logging.getLogger("instagrapi").setLevel(logging.WARNING)
logging.getLogger("public_request").setLevel(logging.WARNING)
logging.getLogger("private_request").setLevel(logging.WARNING)

# ──────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────
INDUSTRY_OPTIONS = {
    "선택 안 함": None,
    "푸드": "food",
    "뷰티": "beauty",
    "패션": "fashion",
    "자동 감지": "auto",
}


# ──────────────────────────────────────────────
# 파이프라인 공통 함수 (main.py에서 재사용)
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
    # 이미 같은 파일 핸들러가 있으면 건너뜀
    for h in root.handlers:
        if isinstance(h, logging.FileHandler) and str(log_path) in str(h.baseFilename):
            return
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root.addHandler(fh)


# ──────────────────────────────────────────────
# 파이프라인 실행
# ──────────────────────────────────────────────
def run_pipeline(
    channel: str,
    industry: str | None,
    use_ai: bool,
    use_vision: bool,
    with_comments: bool,
    upload_drive: bool,
    force_reanalyze: bool,
) -> Path | None:
    """전체 파이프라인 실행 (Streamlit 진행 바 연동)

    Returns:
        report.pptx 경로 또는 None (실패 시)
    """
    data_dir = ensure_dirs(channel)
    setup_file_logging(data_dir)

    progress = st.progress(0, text="준비 중...")
    status = st.status("파이프라인 실행 중", expanded=True)

    report_path = None

    try:
        # ── 1단계: 데이터 수집 ──
        progress.progress(5, text="데이터 수집 중...")
        status.write("📡 인스타그램 데이터 수집 시작...")

        from collector import collect

        success = collect(channel, data_dir, with_comments=with_comments)
        if not success:
            status.write("❌ 데이터 수집 실패")
            status.update(label="파이프라인 실패", state="error")
            return None

        status.write("✅ 데이터 수집 완료")
        progress.progress(30, text="추정치 산출 중...")

        # ── 1.5단계: 추정치 산출 ──
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

        status.write("✅ 추정치 산출 완료")
        progress.progress(40, text="AI 분석 중...")

        # ── 2단계: AI 분석 ──
        if use_ai:
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
                status.write(f"✅ AI 분석 완료 ({completed}/{total} 태스크)")
            except Exception as e:
                status.write(f"⚠️ AI 분석 일부 실패: {e}")
        else:
            status.write("⏭️ AI 분석 건너뜀 (비활성화)")

        progress.progress(70, text="보고서 생성 중...")

        # ── 3단계: 보고서 생성 ──
        from reporter import generate_report

        try:
            report_path = generate_report(channel, data_dir)
            status.write(f"✅ 보고서 생성 완료")
        except Exception as e:
            status.write(f"❌ 보고서 생성 실패: {e}")

        progress.progress(90, text="업로드 중...")

        # ── 4단계: Google Drive 업로드 ──
        if upload_drive:
            from drive_uploader import upload_to_drive

            try:
                drive_ok = upload_to_drive(channel, data_dir)
                if drive_ok:
                    status.write("✅ Google Drive 업로드 완료")
                else:
                    status.write("⚠️ Google Drive 업로드 실패 (로컬 파일 유지)")
            except Exception as e:
                status.write(f"⚠️ Google Drive 업로드 오류: {e}")
        else:
            status.write("⏭️ Google Drive 업로드 건너뜀")

        progress.progress(100, text="완료!")
        status.update(label="파이프라인 완료", state="complete")

    except Exception as e:
        logger.error("파이프라인 오류: %s", e)
        status.write(f"❌ 오류 발생: {e}")
        status.update(label="파이프라인 실패", state="error")

    return report_path


# ──────────────────────────────────────────────
# Streamlit UI
# ──────────────────────────────────────────────
def main() -> None:
    st.set_page_config(
        page_title="인스타그램 채널 분석기",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 인스타그램 채널 분석기")
    st.caption("공개 데이터 기반 경쟁 채널 분석 보고서 자동 생성")

    # ── 사이드바: 입력 ──
    with st.sidebar:
        st.header("분석 설정")

        channel_input = st.text_input(
            "채널명",
            placeholder="@omuk_food",
            help="분석할 인스타그램 채널의 계정명을 입력하세요",
        )

        industry_label = st.selectbox(
            "업종",
            options=list(INDUSTRY_OPTIONS.keys()),
            help="업종에 맞는 카테고리 분류 프리셋이 적용됩니다",
        )
        industry = INDUSTRY_OPTIONS[industry_label]

        st.subheader("API 설정")
        api_key_input = st.text_input(
            "Anthropic API Key",
            type="password",
            help="입력하지 않으면 환경변수 또는 config.yaml의 설정값을 사용합니다",
        )
        ig_username = st.text_input(
            "Instagram ID",
            help="인스타그램 로그인 계정 (입력하지 않으면 기존 설정 사용)",
        )
        ig_password = st.text_input(
            "Instagram Password",
            type="password",
            help="인스타그램 비밀번호",
        )

        st.subheader("옵션")
        use_ai = st.checkbox("AI 분석 포함", value=True,
                             help="Claude API를 사용한 텍스트 분석 (비용 발생)")
        use_vision = st.checkbox("Vision (이미지 분석)", value=True,
                                 help="Claude Vision으로 피드 이미지 분석 (추가 비용)")
        with_comments = st.checkbox("댓글 수집", value=False,
                                    help="댓글 수집 활성화 (Instagram API 제한으로 실패 가능)")
        upload_drive = st.checkbox("Google Drive 업로드", value=False,
                                   help="분석 결과를 Google Drive에 영구 저장")
        force_reanalyze = st.checkbox("분석 캐시 무시", value=False,
                                      help="이전 AI 분석 결과를 무시하고 처음부터 다시 실행")

        if not use_ai:
            use_vision = False

        st.divider()

        run_button = st.button("🚀 분석 시작", type="primary", use_container_width=True)

    # ── 메인 영역 ──
    if run_button:
        if not channel_input:
            st.error("채널명을 입력해주세요.")
            return

        channel = channel_input.lstrip("@").strip()
        if not channel:
            st.error("유효한 채널명을 입력해주세요.")
            return

        # UI 입력 자격증명 적용
        _apply_credentials(api_key_input, ig_username, ig_password)

        st.subheader(f"@{channel} 분석")

        report_path = run_pipeline(
            channel=channel,
            industry=industry,
            use_ai=use_ai,
            use_vision=use_vision,
            with_comments=with_comments,
            upload_drive=upload_drive,
            force_reanalyze=force_reanalyze,
        )

        # 결과 다운로드
        if report_path and report_path.exists():
            st.divider()
            st.subheader("📥 결과 다운로드")

            col1, col2 = st.columns(2)
            with col1:
                with open(report_path, "rb") as f:
                    st.download_button(
                        label="📄 report.pptx 다운로드",
                        data=f.read(),
                        file_name=f"{channel}_report.pptx",
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        use_container_width=True,
                    )

            # 차트 미리보기
            charts_dir = report_path.parent / "charts"
            if charts_dir.exists():
                chart_files = sorted(charts_dir.glob("*.png"))
                if chart_files:
                    st.subheader("📈 생성된 차트")
                    cols = st.columns(min(len(chart_files), 3))
                    for i, chart in enumerate(chart_files):
                        with cols[i % 3]:
                            st.image(str(chart), caption=chart.stem, use_container_width=True)

    else:
        # 초기 화면 — 기존 분석 결과 표시
        data_dir = Path("data")
        if data_dir.exists():
            channels = [d.name for d in data_dir.iterdir()
                        if d.is_dir() and (d / "raw" / "profile.json").exists()]
            if channels:
                st.subheader("📂 기존 분석 결과")
                for ch in sorted(channels):
                    report = data_dir / ch / "report" / "report.pptx"
                    profile_path = data_dir / ch / "raw" / "profile.json"

                    with open(profile_path, encoding="utf-8") as f:
                        profile = json.load(f)

                    col1, col2, col3 = st.columns([3, 2, 2])
                    with col1:
                        st.write(f"**@{ch}** — 팔로워 {profile.get('followers', 0):,}명")
                    with col2:
                        if report.exists():
                            st.write("✅ 보고서 있음")
                        else:
                            st.write("⏳ 보고서 없음")
                    with col3:
                        if report.exists():
                            with open(report, "rb") as f:
                                st.download_button(
                                    "다운로드",
                                    data=f.read(),
                                    file_name=f"{ch}_report.pptx",
                                    key=f"dl_{ch}",
                                )
            else:
                st.info("아직 분석된 채널이 없습니다. 사이드바에서 채널명을 입력하고 분석을 시작하세요.")
        else:
            st.info("아직 분석된 채널이 없습니다. 사이드바에서 채널명을 입력하고 분석을 시작하세요.")


if __name__ == "__main__":
    main()
