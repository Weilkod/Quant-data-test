"""
AI 분석 모듈 — Claude API 호출 전담

CLAUDE.md 규칙:
    - 모든 호출은 call_claude() 래퍼를 통과
    - 재시도: 2회, 30초 대기
    - 프롬프트는 prompts/*.txt에서 로드 (인라인 금지)
    - 출력 형식: JSON 스키마 지정 (regex 파싱 금지)
    - 결과 캐싱: analysis/{task}.json
    - Vision: 이미지 ≤1024px 리사이즈 후 base64

모델 배정 (INSTA.md D-4):
    - Haiku: 카테고리 분류, 캡션 스타일, 감성 분석
    - Sonnet: 연령대 추정, 비주얼 분석, 인기 게시물 분석, 보고서 내러티브
"""

import base64
import io
import json
import logging
import time
from pathlib import Path

import pandas as pd
import yaml
from PIL import Image

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 모델 ID (CLAUDE.md 엄격 준수)
# ──────────────────────────────────────────────
MODEL_HAIKU = "claude-haiku-4-5-20251001"
MODEL_SONNET = "claude-sonnet-4-6"

# 비용 단가 (USD per 1M tokens)
_COST_TABLE = {
    MODEL_HAIKU: {"input": 1.0, "output": 5.0},
    MODEL_SONNET: {"input": 3.0, "output": 15.0},
}

# ──────────────────────────────────────────────
# 프롬프트 로드
# ──────────────────────────────────────────────
_PROMPTS_DIR = Path(__file__).parent / "prompts"


def load_prompt(name: str) -> str:
    """prompts/{name}.txt 로드

    Args:
        name: 프롬프트 파일명 (확장자 제외)

    Returns:
        프롬프트 텍스트

    Raises:
        FileNotFoundError: 프롬프트 파일 없음
    """
    path = _PROMPTS_DIR / f"{name}.txt"
    if not path.exists():
        raise FileNotFoundError(f"프롬프트 파일 없음: {path}")
    return path.read_text(encoding="utf-8")


# ──────────────────────────────────────────────
# 프리셋 로드
# ──────────────────────────────────────────────
_PRESETS_DIR = Path(__file__).parent / "presets"


def load_preset(industry: str) -> dict | None:
    """업종 프리셋 로드

    Args:
        industry: 업종명 (food/beauty/fashion) 또는 "custom:path" 또는 "auto"

    Returns:
        프리셋 dict 또는 None (auto/없음)
    """
    if industry is None:
        return None

    if industry == "auto":
        return None  # auto는 analyze_auto_categories에서 생성

    if industry.startswith("custom:"):
        custom_path = Path(industry.split(":", 1)[1])
        if not custom_path.exists():
            raise FileNotFoundError(f"커스텀 프리셋 파일 없음: {custom_path}")
        with open(custom_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    preset_path = _PRESETS_DIR / f"{industry}.yaml"
    if not preset_path.exists():
        logger.warning("프리셋 '%s' 없음 — 기본 카테고리 사용", industry)
        return None

    with open(preset_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ──────────────────────────────────────────────
# Claude API 래퍼
# ──────────────────────────────────────────────

# 파이프라인 누적 비용 추적
_cumulative_cost_usd = 0.0


def _estimate_tokens(text: str) -> int:
    """텍스트의 대략적 토큰 수 추정 (한국어: ~1.5 chars/token)"""
    return max(1, int(len(text) / 1.5))


def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """예상 비용 계산 (USD)"""
    rates = _COST_TABLE.get(model, _COST_TABLE[MODEL_SONNET])
    return (input_tokens * rates["input"] + output_tokens * rates["output"]) / 1_000_000


def _create_client():
    """Anthropic 클라이언트 생성 (config.yaml에서 API 키 로드)"""
    import anthropic

    config_path = Path("config/config.yaml")
    api_key = None

    if config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        if config and "claude" in config:
            api_key = config["claude"].get("api_key")

    if not api_key:
        # 환경변수 ANTHROPIC_API_KEY 폴백
        return anthropic.Anthropic()

    return anthropic.Anthropic(api_key=api_key)


def call_claude(
    model: str,
    system_prompt: str,
    user_content: str | list,
    max_tokens: int = 4096,
    max_retries: int = 2,
    wait_time: int = 30,
) -> dict:
    """Claude API 호출 래퍼 — 재시도 + 로깅 + 비용 추적

    Args:
        model: 모델 ID (MODEL_HAIKU 또는 MODEL_SONNET)
        system_prompt: 시스템 프롬프트
        user_content: 사용자 메시지 (텍스트 또는 content 블록 리스트)
        max_tokens: 최대 출력 토큰
        max_retries: 최대 재시도 횟수
        wait_time: 재시도 대기 시간 (초)

    Returns:
        파싱된 JSON dict

    Raises:
        RuntimeError: 모든 재시도 실패 시
        json.JSONDecodeError: 응답 JSON 파싱 실패 시
    """
    global _cumulative_cost_usd

    # 토큰 추정
    if isinstance(user_content, str):
        est_input = _estimate_tokens(system_prompt + user_content)
    else:
        est_input = _estimate_tokens(system_prompt) + sum(
            _estimate_tokens(str(block)) for block in user_content
        )
    est_output = max_tokens // 2  # 보수적 추정
    est_cost = _estimate_cost(model, est_input, est_output)

    logger.info(
        "Claude %s 호출: ~%s input tokens, 예상 비용 ~$%.3f",
        model.split("-")[1] if "-" in model else model,
        f"{est_input:,}",
        est_cost,
    )

    client = _create_client()

    # 메시지 구성
    if isinstance(user_content, str):
        messages = [{"role": "user", "content": user_content}]
    else:
        messages = [{"role": "user", "content": user_content}]

    last_error = None
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system_prompt,
                messages=messages,
            )

            # 비용 추적
            usage = response.usage
            actual_cost = _estimate_cost(model, usage.input_tokens, usage.output_tokens)
            _cumulative_cost_usd += actual_cost
            logger.info(
                "Claude 응답: %d input / %d output tokens, 비용 $%.3f (누적 $%.3f)",
                usage.input_tokens,
                usage.output_tokens,
                actual_cost,
                _cumulative_cost_usd,
            )

            if _cumulative_cost_usd > 5.0:
                logger.warning(
                    "⚠️ 파이프라인 누적 비용 $%.2f — $5 초과!", _cumulative_cost_usd
                )

            # JSON 파싱
            raw_text = response.content[0].text.strip()

            # JSON 블록 추출 (```json ... ``` 감싸기 대응)
            if raw_text.startswith("```"):
                lines = raw_text.split("\n")
                # 첫 줄(```json)과 마지막 줄(```) 제거
                json_lines = []
                in_block = False
                for line in lines:
                    if line.strip().startswith("```") and not in_block:
                        in_block = True
                        continue
                    if line.strip() == "```" and in_block:
                        break
                    if in_block:
                        json_lines.append(line)
                raw_text = "\n".join(json_lines)

            return json.loads(raw_text)

        except json.JSONDecodeError as e:
            logger.error("JSON 파싱 실패 (시도 %d/%d): %s", attempt + 1, max_retries, e)
            last_error = e
            if attempt < max_retries - 1:
                logger.info("%d초 후 재시도...", wait_time)
                time.sleep(wait_time)
        except Exception as e:
            logger.error("Claude API 오류 (시도 %d/%d): %s", attempt + 1, max_retries, e)
            last_error = e
            if attempt < max_retries - 1:
                logger.info("%d초 후 재시도...", wait_time)
                time.sleep(wait_time)

    raise RuntimeError(f"Claude API 호출 실패 ({max_retries}회 재시도 후): {last_error}")


def get_cumulative_cost() -> float:
    """현재까지 누적 API 비용 반환 (USD)"""
    return _cumulative_cost_usd


def reset_cumulative_cost() -> None:
    """누적 비용 초기화 (테스트용)"""
    global _cumulative_cost_usd
    _cumulative_cost_usd = 0.0


# ──────────────────────────────────────────────
# 이미지 처리 (Vision용)
# ──────────────────────────────────────────────


def resize_and_encode_image(image_path: Path, max_size: int = 1024) -> str:
    """이미지를 ≤max_size px로 리사이즈 후 base64 인코딩

    Args:
        image_path: 이미지 파일 경로
        max_size: 최대 크기 (px, 가로/세로 중 긴 쪽)

    Returns:
        base64 인코딩 문자열
    """
    with Image.open(image_path) as img:
        img.thumbnail((max_size, max_size))
        buffer = io.BytesIO()
        # RGBA → RGB 변환 (JPEG 저장 위해)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        img.save(buffer, format="JPEG", quality=85)
        return base64.b64encode(buffer.getvalue()).decode("utf-8")


# ──────────────────────────────────────────────
# 캐싱 유틸리티
# ──────────────────────────────────────────────


def _load_cache(analysis_dir: Path, filename: str) -> dict | None:
    """캐시된 분석 결과 로드. 없으면 None 반환."""
    cache_path = analysis_dir / filename
    if cache_path.exists():
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)
    return None


def _save_cache(analysis_dir: Path, filename: str, data: dict) -> None:
    """분석 결과를 JSON으로 캐시 저장."""
    cache_path = analysis_dir / filename
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("캐시 저장: %s", cache_path)


# ──────────────────────────────────────────────
# 개별 분석 태스크
# ──────────────────────────────────────────────


def analyze_categories(
    posts_df: pd.DataFrame,
    preset: dict | None,
    analysis_dir: Path,
    force: bool = False,
) -> dict:
    """콘텐츠 카테고리 분류 (Haiku)

    Args:
        posts_df: 게시물 DataFrame (shortcode, caption 컬럼 필수)
        preset: 업종 프리셋 (None이면 범용 카테고리 사용)
        analysis_dir: 분석 결과 저장 디렉토리
        force: True면 캐시 무시

    Returns:
        분류 결과 dict
    """
    cache_name = "categories.json"
    if not force:
        cached = _load_cache(analysis_dir, cache_name)
        if cached:
            logger.info("카테고리 분류 캐시 사용")
            return cached

    # 카테고리 정의 구성
    if preset and "categories" in preset:
        categories_text = "\n".join(
            f"- {cat['code']}: {cat['name']} — {cat['definition']} "
            f"(키워드: {', '.join(cat['keywords'])})"
            for cat in preset["categories"]
        )
    else:
        categories_text = (
            "No preset provided. Create your own categories based on the content. "
            "Use codes C01~C10."
        )

    # 게시물 데이터 구성 (캡션 배치)
    posts_data = []
    for _, row in posts_df.iterrows():
        caption = str(row.get("caption", "")) if pd.notna(row.get("caption")) else ""
        posts_data.append({
            "shortcode": row["shortcode"],
            "caption": caption[:500],  # 토큰 절약: 캡션 500자 제한
        })

    prompt_template = load_prompt("categorize_captions")
    user_content = prompt_template.format(
        categories=categories_text,
        posts=json.dumps(posts_data, ensure_ascii=False),
    )

    result = call_claude(
        model=MODEL_HAIKU,
        system_prompt="You are an expert Korean social media content analyst. Respond only in valid JSON.",
        user_content=user_content,
        max_tokens=8192,
    )

    _save_cache(analysis_dir, cache_name, result)
    logger.info("카테고리 분류 완료 — %d개 게시물", len(posts_df))
    return result


def analyze_caption_style(
    posts_df: pd.DataFrame,
    analysis_dir: Path,
    force: bool = False,
) -> dict:
    """캡션 스타일 분석 (Haiku)

    Args:
        posts_df: 게시물 DataFrame
        analysis_dir: 분석 결과 저장 디렉토리
        force: True면 캐시 무시

    Returns:
        스타일 분석 결과 dict
    """
    cache_name = "caption_style.json"
    if not force:
        cached = _load_cache(analysis_dir, cache_name)
        if cached:
            logger.info("캡션 스타일 분석 캐시 사용")
            return cached

    captions = []
    for _, row in posts_df.iterrows():
        caption = str(row.get("caption", "")) if pd.notna(row.get("caption")) else ""
        if caption:
            captions.append(caption[:500])

    prompt_template = load_prompt("analyze_caption_style")
    user_content = prompt_template.format(
        captions=json.dumps(captions, ensure_ascii=False),
    )

    result = call_claude(
        model=MODEL_HAIKU,
        system_prompt="You are an expert Korean social media content analyst. Respond only in valid JSON.",
        user_content=user_content,
        max_tokens=4096,
    )

    _save_cache(analysis_dir, cache_name, result)
    logger.info("캡션 스타일 분석 완료")
    return result


def analyze_sentiment(
    posts_df: pd.DataFrame,
    comments_df: pd.DataFrame,
    analysis_dir: Path,
    force: bool = False,
) -> dict:
    """댓글 감성 분석 (Haiku)

    상위 20개 게시물의 댓글을 분석.

    Args:
        posts_df: 게시물 DataFrame (좋아요 기준 정렬용)
        comments_df: 댓글 DataFrame (shortcode, text 컬럼 필수)
        analysis_dir: 분석 결과 저장 디렉토리
        force: True면 캐시 무시

    Returns:
        감성 분석 결과 dict
    """
    cache_name = "sentiment.json"
    if not force:
        cached = _load_cache(analysis_dir, cache_name)
        if cached:
            logger.info("감성 분석 캐시 사용")
            return cached

    # 상위 20개 게시물 선택
    top_posts = posts_df.nlargest(20, "likes")
    top_shortcodes = set(top_posts["shortcode"].tolist())

    # 댓글 그룹핑
    comments_by_post = {}
    for _, row in comments_df.iterrows():
        sc = row.get("shortcode", "")
        if sc in top_shortcodes:
            text = str(row.get("text", "")) if pd.notna(row.get("text")) else ""
            if text:
                comments_by_post.setdefault(sc, []).append(text[:200])

    # 게시물당 최대 50개 댓글로 제한 (토큰 절약)
    for sc in comments_by_post:
        comments_by_post[sc] = comments_by_post[sc][:50]

    prompt_template = load_prompt("sentiment_analysis")
    user_content = prompt_template.format(
        comments=json.dumps(comments_by_post, ensure_ascii=False),
    )

    result = call_claude(
        model=MODEL_HAIKU,
        system_prompt="You are an expert Korean social media sentiment analyst. Respond only in valid JSON.",
        user_content=user_content,
        max_tokens=8192,
    )

    _save_cache(analysis_dir, cache_name, result)
    logger.info("감성 분석 완료 — %d개 게시물의 댓글", len(comments_by_post))
    return result


def analyze_audience_age(
    comments_df: pd.DataFrame,
    analysis_dir: Path,
    force: bool = False,
) -> dict:
    """오디언스 연령대 추정 (Sonnet)

    댓글 텍스트 샘플 500개를 기반으로 연령대 분포 추정.

    Args:
        comments_df: 댓글 DataFrame (text 컬럼 필수)
        analysis_dir: 분석 결과 저장 디렉토리
        force: True면 캐시 무시

    Returns:
        연령대 추정 결과 dict
    """
    cache_name = "audience_age.json"
    if not force:
        cached = _load_cache(analysis_dir, cache_name)
        if cached:
            logger.info("연령대 추정 캐시 사용")
            return cached

    # 댓글 샘플 500개 (랜덤 추출)
    comments_texts = []
    for _, row in comments_df.iterrows():
        text = str(row.get("text", "")) if pd.notna(row.get("text")) else ""
        if text and len(text) > 3:  # 너무 짧은 댓글 제외
            comments_texts.append(text[:200])

    if len(comments_texts) > 500:
        import random
        comments_texts = random.sample(comments_texts, 500)

    prompt_template = load_prompt("age_estimation")
    user_content = prompt_template.format(
        comments=json.dumps(comments_texts, ensure_ascii=False),
    )

    result = call_claude(
        model=MODEL_SONNET,
        system_prompt="You are a Korean language and demographics expert. Respond only in valid JSON.",
        user_content=user_content,
        max_tokens=4096,
    )

    _save_cache(analysis_dir, cache_name, result)
    logger.info("연령대 추정 완료 — %d개 댓글 분석", len(comments_texts))
    return result


def analyze_visual(
    data_dir: Path,
    posts_df: pd.DataFrame,
    analysis_dir: Path,
    force: bool = False,
) -> dict:
    """비주얼 톤 분석 (Sonnet + Vision)

    상위 20개 게시물의 이미지를 분석.

    Args:
        data_dir: data/{channel}/ 경로
        posts_df: 게시물 DataFrame
        analysis_dir: 분석 결과 저장 디렉토리
        force: True면 캐시 무시

    Returns:
        비주얼 분석 결과 dict
    """
    cache_name = "visual.json"
    if not force:
        cached = _load_cache(analysis_dir, cache_name)
        if cached:
            logger.info("비주얼 분석 캐시 사용")
            return cached

    # 상위 20개 게시물 이미지 수집
    top_posts = posts_df.nlargest(20, "likes")
    images_dir = data_dir / "raw" / "images"

    image_blocks = []
    loaded_count = 0
    for _, row in top_posts.iterrows():
        shortcode = row["shortcode"]
        # 이미지 파일 찾기 (jpg/png)
        for ext in [".jpg", ".jpeg", ".png"]:
            img_path = images_dir / f"{shortcode}{ext}"
            if img_path.exists():
                try:
                    b64_data = resize_and_encode_image(img_path)
                    image_blocks.append({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/jpeg",
                            "data": b64_data,
                        },
                    })
                    loaded_count += 1
                except Exception as e:
                    logger.warning("이미지 로드 실패 %s: %s", img_path, e)
                break

    if loaded_count == 0:
        logger.warning("분석 가능한 이미지 없음 — 비주얼 분석 건너뜀")
        result = {
            "feed_coherence_score": 0,
            "summary": "분석 가능한 이미지가 없습니다.",
            "skipped": True,
        }
        _save_cache(analysis_dir, cache_name, result)
        return result

    logger.info("비주얼 분석: %d개 이미지 로드 완료", loaded_count)

    prompt_template = load_prompt("visual_analysis")
    # Vision API: 이미지 블록 + 텍스트 프롬프트
    content_blocks = image_blocks + [{"type": "text", "text": prompt_template}]

    result = call_claude(
        model=MODEL_SONNET,
        system_prompt="You are a visual design analyst specializing in Instagram content. Respond only in valid JSON.",
        user_content=content_blocks,
        max_tokens=4096,
    )

    _save_cache(analysis_dir, cache_name, result)
    logger.info("비주얼 분석 완료 — %d개 이미지", loaded_count)
    return result


def analyze_top_posts(
    posts_df: pd.DataFrame,
    categories: dict | None,
    analysis_dir: Path,
    force: bool = False,
) -> dict:
    """인기 게시물 공통점 분석 (Sonnet)

    Args:
        posts_df: enriched 게시물 DataFrame
        categories: 카테고리 분류 결과 (있으면 포함)
        analysis_dir: 분석 결과 저장 디렉토리
        force: True면 캐시 무시

    Returns:
        인기 게시물 분석 결과 dict
    """
    cache_name = "top_posts_analysis.json"
    if not force:
        cached = _load_cache(analysis_dir, cache_name)
        if cached:
            logger.info("인기 게시물 분석 캐시 사용")
            return cached

    # 카테고리 매핑 구성
    cat_map = {}
    if categories and "classifications" in categories:
        for c in categories["classifications"]:
            cat_map[c["shortcode"]] = c.get("category_code", "N/A")

    def _post_summary(row: pd.Series) -> dict:
        caption = str(row.get("caption", "")) if pd.notna(row.get("caption")) else ""
        return {
            "shortcode": row["shortcode"],
            "typename": row.get("typename", ""),
            "likes": int(row.get("likes", 0)),
            "comments": int(row.get("comments", 0)),
            "date_utc": str(row.get("date_utc", "")),
            "caption_preview": caption[:200],
            "category": cat_map.get(row["shortcode"], "N/A"),
            "caption_hashtags": str(row.get("caption_hashtags", "")),
        }

    top10 = posts_df.nlargest(10, "likes")
    bottom10 = posts_df.nsmallest(10, "likes")

    top_posts_data = [_post_summary(row) for _, row in top10.iterrows()]
    bottom_posts_data = [_post_summary(row) for _, row in bottom10.iterrows()]

    stats = {
        "total_posts": len(posts_df),
        "avg_likes": round(posts_df["likes"].mean(), 1) if len(posts_df) > 0 else 0,
        "avg_comments": round(posts_df["comments"].mean(), 1) if len(posts_df) > 0 else 0,
    }

    prompt_template = load_prompt("top_posts_insights")
    user_content = prompt_template.format(
        top_posts=json.dumps(top_posts_data, ensure_ascii=False),
        bottom_posts=json.dumps(bottom_posts_data, ensure_ascii=False),
        stats=json.dumps(stats, ensure_ascii=False),
    )

    result = call_claude(
        model=MODEL_SONNET,
        system_prompt="You are an expert Korean social media strategist. Respond only in valid JSON.",
        user_content=user_content,
        max_tokens=4096,
    )

    _save_cache(analysis_dir, cache_name, result)
    logger.info("인기 게시물 분석 완료")
    return result


def generate_narrative(
    profile: dict,
    analysis_dir: Path,
    force: bool = False,
) -> dict:
    """최종 보고서 내러티브 생성 (Sonnet)

    이전 분석 결과를 종합하여 보고서 내러티브 텍스트 생성.

    Args:
        profile: profile.json 데이터
        analysis_dir: 분석 결과 디렉토리 (이전 분석 JSON 읽기)
        force: True면 캐시 무시

    Returns:
        내러티브 결과 dict
    """
    cache_name = "insights.json"
    if not force:
        cached = _load_cache(analysis_dir, cache_name)
        if cached:
            logger.info("보고서 내러티브 캐시 사용")
            return cached

    # 이전 분석 결과 수집
    analysis_results = {}
    for name in ["categories", "caption_style", "sentiment", "audience_age",
                  "visual", "top_posts_analysis", "format_stats"]:
        path = analysis_dir / f"{name}.json"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                analysis_results[name] = json.load(f)
        # CSV 파일 (format_stats)
        csv_path = analysis_dir / f"{name}.csv"
        if csv_path.exists() and name not in analysis_results:
            analysis_results[name] = pd.read_csv(csv_path).to_dict(orient="records")

    channel_info = {
        "username": profile.get("username", ""),
        "followers": profile.get("followers", 0),
        "biography": profile.get("biography", ""),
        "mediacount": profile.get("mediacount", 0),
    }

    prompt_template = load_prompt("report_narrative")
    user_content = prompt_template.format(
        channel_info=json.dumps(channel_info, ensure_ascii=False),
        analysis_results=json.dumps(analysis_results, ensure_ascii=False, default=str),
    )

    result = call_claude(
        model=MODEL_SONNET,
        system_prompt="You are a professional Korean marketing analyst. Respond only in valid JSON.",
        user_content=user_content,
        max_tokens=8192,
    )

    _save_cache(analysis_dir, cache_name, result)
    logger.info("보고서 내러티브 생성 완료")
    return result


def analyze_auto_categories(
    posts_df: pd.DataFrame,
    channel: str,
    analysis_dir: Path,
) -> dict:
    """자동 카테고리 생성 (Sonnet) — --industry auto 전용

    최근 50개 캡션을 분석하여 카테고리 체계 자동 생성.

    Args:
        posts_df: 게시물 DataFrame
        channel: 채널명
        analysis_dir: 분석 결과 저장 디렉토리

    Returns:
        생성된 프리셋 dict
    """
    # 최근 50개 캡션 샘플
    sample = posts_df.head(50)
    captions = []
    for _, row in sample.iterrows():
        caption = str(row.get("caption", "")) if pd.notna(row.get("caption")) else ""
        if caption:
            captions.append(caption[:300])

    prompt_template = load_prompt("auto_categories")
    user_content = prompt_template.format(
        captions=json.dumps(captions, ensure_ascii=False),
    )

    result = call_claude(
        model=MODEL_SONNET,
        system_prompt="You are an expert Korean social media analyst. Respond only in valid JSON.",
        user_content=user_content,
        max_tokens=4096,
    )

    # presets/auto_{channel}.yaml로 저장
    preset_path = _PRESETS_DIR / f"auto_{channel}.yaml"
    _PRESETS_DIR.mkdir(parents=True, exist_ok=True)
    with open(preset_path, "w", encoding="utf-8") as f:
        yaml.dump(result, f, allow_unicode=True, default_flow_style=False)
    logger.info("자동 카테고리 생성 완료 → %s", preset_path)

    return result


# ──────────────────────────────────────────────
# 메인 분석 오케스트레이터
# ──────────────────────────────────────────────


def run_analysis(
    channel: str,
    data_dir: Path,
    industry: str | None = None,
    text_only: bool = False,
    force: bool = False,
) -> dict:
    """전체 AI 분석 파이프라인 실행

    Args:
        channel: 채널명
        data_dir: data/{channel}/ 경로
        industry: 업종 (food/beauty/fashion/auto/custom:path/None)
        text_only: True면 Vision 분석 건너뜀 (--ai-text-only)
        force: True면 모든 캐시 무시 (--force-reanalyze)

    Returns:
        전체 분석 결과를 담은 dict
    """
    reset_cumulative_cost()
    analysis_dir = data_dir / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    # 입력 데이터 로드
    posts_df = pd.read_csv(data_dir / "raw" / "posts.csv")
    with open(data_dir / "raw" / "profile.json", encoding="utf-8") as f:
        profile = json.load(f)

    comments_path = data_dir / "raw" / "comments.csv"
    has_comments = comments_path.exists()
    comments_df = pd.read_csv(comments_path) if has_comments else None

    logger.info(
        "AI 분석 시작 — @%s, %d개 게시물, 댓글 %s, Vision %s",
        channel,
        len(posts_df),
        "있음" if has_comments else "없음",
        "OFF" if text_only else "ON",
    )

    results = {}

    # 1. 프리셋 로드 / auto 모드
    preset = None
    if industry == "auto":
        preset = analyze_auto_categories(posts_df, channel, analysis_dir)
    elif industry:
        preset = load_preset(industry)

    # 2. 카테고리 분류 (Haiku)
    try:
        results["categories"] = analyze_categories(
            posts_df, preset, analysis_dir, force=force
        )
    except Exception as e:
        logger.error("카테고리 분류 실패 — 건너뜀: %s", e)
        results["categories"] = None

    # 3. 캡션 스타일 분석 (Haiku)
    try:
        results["caption_style"] = analyze_caption_style(
            posts_df, analysis_dir, force=force
        )
    except Exception as e:
        logger.error("캡션 스타일 분석 실패 — 건너뜀: %s", e)
        results["caption_style"] = None

    # 4. 감성 분석 (Haiku) — 댓글 있을 때만
    if has_comments and comments_df is not None and len(comments_df) > 0:
        try:
            results["sentiment"] = analyze_sentiment(
                posts_df, comments_df, analysis_dir, force=force
            )
        except Exception as e:
            logger.error("감성 분석 실패 — 건너뜀: %s", e)
            results["sentiment"] = None
    else:
        logger.info("댓글 데이터 없음 — 감성 분석 건너뜀")
        results["sentiment"] = None

    # 5. 연령대 추정 (Sonnet) — 댓글 있을 때만
    if has_comments and comments_df is not None and len(comments_df) > 0:
        try:
            results["audience_age"] = analyze_audience_age(
                comments_df, analysis_dir, force=force
            )
        except Exception as e:
            logger.error("연령대 추정 실패 — 건너뜀: %s", e)
            results["audience_age"] = None
    else:
        logger.info("댓글 데이터 없음 — 연령대 추정 건너뜀")
        results["audience_age"] = None

    # 6. 비주얼 분석 (Sonnet + Vision) — text_only면 건너뜀
    if not text_only:
        try:
            results["visual"] = analyze_visual(
                data_dir, posts_df, analysis_dir, force=force
            )
        except Exception as e:
            logger.error("비주얼 분석 실패 — 건너뜀: %s", e)
            results["visual"] = None
    else:
        logger.info("--ai-text-only 모드: 비주얼 분석 건너뜀")
        results["visual"] = None

    # 7. 인기 게시물 공통점 (Sonnet)
    try:
        results["top_posts"] = analyze_top_posts(
            posts_df, results.get("categories"), analysis_dir, force=force
        )
    except Exception as e:
        logger.error("인기 게시물 분석 실패 — 건너뜀: %s", e)
        results["top_posts"] = None

    # 8. 보고서 내러티브 (Sonnet)
    try:
        results["narrative"] = generate_narrative(
            profile, analysis_dir, force=force
        )
    except Exception as e:
        logger.error("보고서 내러티브 생성 실패 — 건너뜀: %s", e)
        results["narrative"] = None

    cost = get_cumulative_cost()
    logger.info(
        "AI 분석 완료 — @%s, 총 비용 $%.3f",
        channel,
        cost,
    )
    return results
