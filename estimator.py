"""
추정치 산출 모듈 — 순수 함수 기반

INSTA.md 부록 A의 공식을 구현:
    - 추정 저장 수 (A-1)
    - 추정 공유 수 (A-2)
    - 추정 조회 수 (A-3)
    - 추정 인게이지먼트율 (A-4)
    - 추정 전체 인게이지먼트율 (E)

규칙 (CLAUDE.md):
    - 순수 함수만 — API 호출 없음
    - 모든 계수는 config/coefficients.yaml에서 로드
    - 음수 likes/comments → 0으로 클램프
    - followers=0 → 인게이지먼트율 0.0
"""

import logging
from pathlib import Path

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# typename → coefficients 키 매핑
_FORMAT_MAP: dict[str, str] = {
    "GraphImage": "image",
    "GraphVideo": "reels",
    "GraphSidecar": "carousel",
}


def load_coefficients(path: str = "config/coefficients.yaml") -> dict:
    """coefficients.yaml 로드

    Args:
        path: YAML 파일 경로

    Returns:
        계수 딕셔너리

    Raises:
        FileNotFoundError: 파일이 없을 때
        yaml.YAMLError: 파싱 실패 시
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"계수 파일 없음: {path}")
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _format_key(typename: str) -> str:
    """typename → 계수 키 매핑. 알 수 없는 타입은 'image' 폴백 (가장 보수적)."""
    key = _FORMAT_MAP.get(typename, "image")
    if typename not in _FORMAT_MAP:
        logger.debug("알 수 없는 typename '%s' → 'image' 폴백", typename)
    return key


def _get_follower_tier(followers: int, tiers: list[dict]) -> dict:
    """팔로워 수에 해당하는 tier 반환.

    Args:
        followers: 팔로워 수
        tiers: views_by_tier 리스트 (max_followers 오름차순 정렬)

    Returns:
        해당 tier의 dict (reels, carousel, image 키 포함)
    """
    for tier in tiers:
        if followers <= tier["max_followers"]:
            return tier
    # 모든 tier 초과 시 마지막 tier 반환
    return tiers[-1]


def estimate_saves(
    likes: int,
    typename: str,
    coeffs: dict,
    category: str = "default",
) -> float:
    """추정 저장 수 = max(0, likes) × save_coeff[format] × category_modifier

    Args:
        likes: 좋아요 수
        typename: 게시물 유형 (GraphImage/GraphVideo/GraphSidecar)
        coeffs: load_coefficients() 반환값
        category: 카테고리 (educational/entertainment/sponsored/default)

    Returns:
        추정 저장 수 (소수점)
    """
    likes = max(0, likes)
    fmt = _format_key(typename)
    base_coeff = coeffs["save_coefficients"].get(fmt, coeffs["save_coefficients"]["image"])
    modifier = coeffs["save_category_modifiers"].get(category, 1.0)
    return likes * base_coeff * modifier


def estimate_shares(
    likes: int,
    typename: str,
    coeffs: dict,
    category: str = "default",
) -> float:
    """추정 공유 수 = max(0, likes) × share_coeff[format] × category_modifier

    Args:
        likes: 좋아요 수
        typename: 게시물 유형
        coeffs: load_coefficients() 반환값
        category: 카테고리

    Returns:
        추정 공유 수 (소수점)
    """
    likes = max(0, likes)
    fmt = _format_key(typename)
    base_coeff = coeffs["share_coefficients"].get(fmt, coeffs["share_coefficients"]["image"])
    modifier = coeffs["share_category_modifiers"].get(category, 1.0)
    return likes * base_coeff * modifier


def estimate_views(
    followers: int,
    typename: str,
    coeffs: dict,
) -> float:
    """추정 조회 수 — Socialinsider 팔로워 규모별 룩업 테이블

    Args:
        followers: 팔로워 수
        typename: 게시물 유형
        coeffs: load_coefficients() 반환값

    Returns:
        추정 조회 수
    """
    followers = max(0, followers)
    fmt = _format_key(typename)
    tiers = coeffs["views_by_tier"]
    tier = _get_follower_tier(followers, tiers)
    return float(tier.get(fmt, tier.get("image", 0)))


def estimate_engagement_rate(
    likes: int,
    comments: int,
    followers: int,
) -> float:
    """추정 인게이지먼트율 = (likes + comments) / followers × 100

    Args:
        likes: 좋아요 수
        comments: 댓글 수
        followers: 팔로워 수

    Returns:
        인게이지먼트율 (%)
    """
    if followers <= 0:
        return 0.0
    likes = max(0, likes)
    comments = max(0, comments)
    return (likes + comments) / followers * 100


def estimate_full_engagement_rate(
    likes: int,
    comments: int,
    est_saves: float,
    est_shares: float,
    est_views: float,
) -> float:
    """추정 전체 인게이지먼트율 = (likes + comments + saves + shares) / views × 100

    Args:
        likes: 좋아요 수
        comments: 댓글 수
        est_saves: 추정 저장 수
        est_shares: 추정 공유 수
        est_views: 추정 조회 수

    Returns:
        전체 인게이지먼트율 (%)
    """
    if est_views <= 0:
        return 0.0
    likes = max(0, likes)
    comments = max(0, comments)
    return (likes + comments + est_saves + est_shares) / est_views * 100


def enrich_posts(
    posts_df: pd.DataFrame,
    followers: int,
    coeffs: dict,
    category: str = "default",
) -> pd.DataFrame:
    """posts DataFrame에 추정치 컬럼 5개 추가

    추가 컬럼:
        - est_saves: 추정 저장 수
        - est_shares: 추정 공유 수
        - est_views: 추정 조회 수
        - est_eng_rate: 추정 인게이지먼트율 (%)
        - est_full_eng_rate: 추정 전체 인게이지먼트율 (%)

    Args:
        posts_df: raw/posts.csv 로드한 DataFrame
        followers: 팔로워 수
        coeffs: load_coefficients() 반환값
        category: 카테고리 (기본 "default")

    Returns:
        추정치 컬럼이 추가된 DataFrame (원본 변경 없음)
    """
    df = posts_df.copy()

    if df.empty:
        for col in ["est_saves", "est_shares", "est_views", "est_eng_rate", "est_full_eng_rate"]:
            df[col] = pd.Series(dtype="float64")
        return df

    df["est_saves"] = df.apply(
        lambda r: estimate_saves(int(r["likes"]), r["typename"], coeffs, category),
        axis=1,
    )
    df["est_shares"] = df.apply(
        lambda r: estimate_shares(int(r["likes"]), r["typename"], coeffs, category),
        axis=1,
    )
    df["est_views"] = df.apply(
        lambda r: estimate_views(followers, r["typename"], coeffs),
        axis=1,
    )
    df["est_eng_rate"] = df.apply(
        lambda r: estimate_engagement_rate(int(r["likes"]), int(r["comments"]), followers),
        axis=1,
    )
    df["est_full_eng_rate"] = df.apply(
        lambda r: estimate_full_engagement_rate(
            int(r["likes"]),
            int(r["comments"]),
            r["est_saves"],
            r["est_shares"],
            r["est_views"],
        ),
        axis=1,
    )

    # 소수점 반올림 (보고서 가독성)
    df["est_saves"] = df["est_saves"].round(1)
    df["est_shares"] = df["est_shares"].round(1)
    df["est_views"] = df["est_views"].round(0).astype(int)
    df["est_eng_rate"] = df["est_eng_rate"].round(2)
    df["est_full_eng_rate"] = df["est_full_eng_rate"].round(2)

    logger.info(
        "추정치 산출 완료 — %d개 게시물, 팔로워 %s, 카테고리 '%s'",
        len(df),
        f"{followers:,}",
        category,
    )
    return df


def aggregate_by_format(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """포맷(typename)별 집계 통계

    Returns:
        typename별 통계 DataFrame:
            - count: 게시물 수
            - ratio_pct: 비율(%)
            - avg_likes, avg_comments: 평균 좋아요/댓글
            - avg_est_saves, avg_est_shares, avg_est_views: 평균 추정치
            - avg_est_eng_rate, avg_est_full_eng_rate: 평균 인게이지먼트율
    """
    total = len(enriched_df)
    if total == 0:
        return pd.DataFrame()

    agg = enriched_df.groupby("typename").agg(
        count=("typename", "size"),
        avg_likes=("likes", "mean"),
        avg_comments=("comments", "mean"),
        avg_est_saves=("est_saves", "mean"),
        avg_est_shares=("est_shares", "mean"),
        avg_est_views=("est_views", "mean"),
        avg_est_eng_rate=("est_eng_rate", "mean"),
        avg_est_full_eng_rate=("est_full_eng_rate", "mean"),
    ).reset_index()

    agg["ratio_pct"] = (agg["count"] / total * 100).round(1)

    # 소수점 정리
    for col in ["avg_likes", "avg_comments", "avg_est_saves", "avg_est_shares"]:
        agg[col] = agg[col].round(1)
    agg["avg_est_views"] = agg["avg_est_views"].round(0).astype(int)
    agg["avg_est_eng_rate"] = agg["avg_est_eng_rate"].round(2)
    agg["avg_est_full_eng_rate"] = agg["avg_est_full_eng_rate"].round(2)

    # 컬럼 순서 정리
    cols = [
        "typename", "count", "ratio_pct",
        "avg_likes", "avg_comments",
        "avg_est_saves", "avg_est_shares", "avg_est_views",
        "avg_est_eng_rate", "avg_est_full_eng_rate",
    ]
    return agg[cols]
