"""
인스타그램 데이터 수집 모듈 — Instaloader 기반

수집 항목:
    - 프로필 메타데이터 → raw/profile.json
    - 게시물 데이터 (최대 200개) → raw/posts.csv
    - 댓글 데이터 → raw/comments.csv
    - 게시물 이미지 → raw/images/{shortcode}.jpg

규칙:
    - 모든 요청 사이 2~4초 랜덤 딜레이 (절대 생략 금지)
    - 댓글 페이지네이션: 추가 1~2초 딜레이
    - 429 에러: 60초 대기 후 최대 3회 지수 백오프 재시도
    - 개별 게시물/댓글 실패 시 건너뛰고 계속 진행 (파이프라인 중단 금지)
"""

import json
import logging
import random
import time
import urllib.request
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import instaloader
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 설정 기본값
# ──────────────────────────────────────────────
_DEFAULT_CONFIG: dict = {
    "instagram": {
        "username": "",
        "session_file": "",
        "max_posts": 200,
        "delay_min": 2,
        "delay_max": 4,
        "comment_delay_min": 1,
        "comment_delay_max": 2,
        "retry_on_429": 3,
        "retry_wait_base": 60,
    }
}


# ──────────────────────────────────────────────
# 유틸리티 함수
# ──────────────────────────────────────────────


def _load_config() -> dict:
    """config/config.yaml 로드. 파일 없으면 기본값 사용."""
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        logger.warning("config/config.yaml 없음 — 기본 설정으로 진행")
        return _DEFAULT_CONFIG
    try:
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        if not config or "instagram" not in config:
            logger.warning("config.yaml에 instagram 섹션 없음 — 기본값 사용")
            return _DEFAULT_CONFIG
        # 누락된 키는 기본값으로 보완
        merged = _DEFAULT_CONFIG.copy()
        merged["instagram"] = {**_DEFAULT_CONFIG["instagram"], **config["instagram"]}
        return merged
    except yaml.YAMLError as e:
        logger.warning("config.yaml 파싱 실패: %s — 기본값 사용", e)
        return _DEFAULT_CONFIG


def _random_delay(min_s: float, max_s: float) -> None:
    """랜덤 딜레이 — 요청 사이 반드시 호출"""
    delay = random.uniform(min_s, max_s)
    logger.debug("딜레이 %.1f초 대기", delay)
    time.sleep(delay)


def _retry_on_error(
    func: Callable,
    max_retries: int,
    base_wait: int,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """429/연결 에러 시 지수 백오프 재시도

    Args:
        func: 실행할 함수
        max_retries: 최대 재시도 횟수
        base_wait: 기본 대기 시간(초)

    Returns:
        func의 반환값

    Raises:
        마지막 재시도 실패 시 원래 예외를 다시 발생
    """
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except (
            instaloader.exceptions.QueryReturnedBadRequestException,
            instaloader.exceptions.ConnectionException,
            ConnectionError,
            TimeoutError,
        ) as e:
            if attempt == max_retries:
                logger.error("최대 재시도(%d회) 초과 — 포기: %s", max_retries, e)
                raise
            wait = base_wait * (2 ** attempt)
            logger.warning(
                "요청 에러 (시도 %d/%d): %s — %d초 후 재시도",
                attempt + 1,
                max_retries,
                e,
                wait,
            )
            time.sleep(wait)
    return None  # 도달 불가


# ──────────────────────────────────────────────
# Instaloader 인스턴스 생성
# ──────────────────────────────────────────────


def _create_loader(config: dict) -> instaloader.Instaloader:
    """Instaloader 인스턴스 생성 및 세션 로드

    다운로드 기능은 모두 비활성화 — 수동으로 처리함
    """
    loader = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
    )

    ig_config = config["instagram"]
    username = ig_config.get("username", "")
    session_file = ig_config.get("session_file", "")

    if username and session_file:
        session_path = Path(session_file)
        if session_path.exists():
            try:
                loader.load_session_from_file(username, str(session_path))
                logger.info("세션 로드 완료 — 사용자: %s", username)
            except Exception as e:
                logger.warning("세션 로드 실패: %s — 비로그인으로 진행", e)
        else:
            logger.warning("세션 파일 없음: %s — 비로그인으로 진행", session_path)
    else:
        logger.info("로그인 정보 미설정 — 비로그인으로 수집 진행")

    return loader


# ──────────────────────────────────────────────
# 프로필 수집
# ──────────────────────────────────────────────


def _collect_profile(profile: instaloader.Profile, raw_dir: Path) -> dict:
    """프로필 메타데이터 수집 → raw/profile.json 저장

    Args:
        profile: Instaloader Profile 객체
        raw_dir: raw/ 디렉토리 경로

    Returns:
        프로필 데이터 딕셔너리
    """
    profile_data = {
        "username": profile.username,
        "full_name": profile.full_name,
        "followers": profile.followers,
        "followees": profile.followees,
        "biography": profile.biography,
        "external_url": profile.external_url or "",
        "mediacount": profile.mediacount,
        "profile_pic_url": profile.profile_pic_url,
        "is_verified": profile.is_verified,
        "business_category_name": getattr(profile, "business_category_name", "") or "",
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }

    output_path = raw_dir / "profile.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(profile_data, f, ensure_ascii=False, indent=2)

    logger.info(
        "프로필 수집 완료 — @%s (팔로워: %s, 게시물: %s)",
        profile.username,
        f"{profile.followers:,}",
        f"{profile.mediacount:,}",
    )
    return profile_data


# ──────────────────────────────────────────────
# 게시물 수집
# ──────────────────────────────────────────────


def _collect_posts(
    profile: instaloader.Profile,
    raw_dir: Path,
    max_posts: int,
    delay_min: float,
    delay_max: float,
) -> list[dict]:
    """게시물 데이터 수집 → raw/posts.csv 저장

    Args:
        profile: Instaloader Profile 객체
        raw_dir: raw/ 디렉토리 경로
        max_posts: 최대 수집 게시물 수
        delay_min: 요청 간 최소 딜레이(초)
        delay_max: 요청 간 최대 딜레이(초)

    Returns:
        수집된 게시물 딕셔너리 리스트
    """
    posts_data: list[dict] = []
    logger.info("게시물 수집 시작 (최대 %d개)", max_posts)

    try:
        for i, post in enumerate(profile.get_posts()):
            if i >= max_posts:
                break

            try:
                post_dict = {
                    "shortcode": post.shortcode,
                    "date_utc": post.date_utc.isoformat(),
                    "caption": post.caption or "",
                    "likes": post.likes,
                    "comments": post.comments,
                    "typename": post.typename,
                    "caption_hashtags": ",".join(post.caption_hashtags),
                    "caption_mentions": ",".join(post.caption_mentions),
                    "url": post.url,
                    "mediacount": post.mediacount,
                }
                posts_data.append(post_dict)

                if (i + 1) % 10 == 0:
                    logger.info("게시물 수집 진행: %d/%d", i + 1, max_posts)

            except Exception as e:
                logger.warning("게시물 수집 실패 (인덱스 %d): %s — 건너뜀", i, e)

            # 매 요청 사이 랜덤 딜레이 (절대 생략 금지)
            _random_delay(delay_min, delay_max)

    except KeyboardInterrupt:
        logger.warning("사용자 중단 — 현재까지 수집된 %d개 게시물 저장", len(posts_data))
    except Exception as e:
        logger.error("게시물 반복 중 에러: %s — 현재까지 수집된 데이터 저장", e)
    finally:
        # 수집된 데이터가 있으면 항상 저장
        if posts_data:
            df = pd.DataFrame(posts_data)
            output_path = raw_dir / "posts.csv"
            df.to_csv(output_path, index=False, encoding="utf-8")
            logger.info("게시물 저장 완료: %d개 → %s", len(posts_data), output_path)

    return posts_data


# ──────────────────────────────────────────────
# 댓글 수집
# ──────────────────────────────────────────────


def _collect_comments(
    loader: instaloader.Instaloader,
    posts: list[dict],
    raw_dir: Path,
    delay_min: float,
    delay_max: float,
) -> None:
    """게시물별 댓글 수집 → raw/comments.csv 저장

    Args:
        loader: Instaloader 인스턴스
        posts: 게시물 딕셔너리 리스트 (_collect_posts에서 반환)
        raw_dir: raw/ 디렉토리 경로
        delay_min: 댓글 페이지네이션 추가 딜레이 최소(초)
        delay_max: 댓글 페이지네이션 추가 딜레이 최대(초)
    """
    all_comments: list[dict] = []
    total_posts = len(posts)
    logger.info("댓글 수집 시작 — %d개 게시물 대상", total_posts)

    try:
        for idx, post_data in enumerate(posts):
            shortcode = post_data["shortcode"]
            comment_count = post_data["comments"]

            # 댓글이 없는 게시물은 건너뜀
            if comment_count == 0:
                continue

            try:
                post = instaloader.Post.from_shortcode(loader.context, shortcode)

                for comment in post.get_comments():
                    try:
                        comment_dict = {
                            "post_shortcode": shortcode,
                            "comment_id": comment.id,
                            "text": comment.text,
                            "owner_username": comment.owner.username,
                            "created_at_utc": comment.created_at_utc.isoformat(),
                        }
                        all_comments.append(comment_dict)
                    except Exception as e:
                        logger.debug("개별 댓글 파싱 실패 (%s): %s", shortcode, e)

                    # 댓글 페이지네이션 딜레이
                    _random_delay(delay_min, delay_max)

            except Exception as e:
                logger.warning(
                    "댓글 수집 실패 — 게시물 %s (%d/%d): %s — 건너뜀",
                    shortcode,
                    idx + 1,
                    total_posts,
                    e,
                )

            if (idx + 1) % 10 == 0:
                logger.info(
                    "댓글 수집 진행: %d/%d 게시물 완료 (누적 댓글 %d개)",
                    idx + 1,
                    total_posts,
                    len(all_comments),
                )

            # 게시물 간 딜레이
            _random_delay(delay_min, delay_max)

    except KeyboardInterrupt:
        logger.warning("사용자 중단 — 현재까지 수집된 %d개 댓글 저장", len(all_comments))
    finally:
        # 댓글 데이터 저장 (비어있어도 빈 CSV 생성)
        df = pd.DataFrame(all_comments)
        output_path = raw_dir / "comments.csv"
        df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info("댓글 저장 완료: %d개 → %s", len(all_comments), output_path)


# ──────────────────────────────────────────────
# 이미지 다운로드
# ──────────────────────────────────────────────


def _download_images(
    loader: instaloader.Instaloader,
    posts: list[dict],
    raw_dir: Path,
    delay_min: float,
    delay_max: float,
) -> None:
    """게시물 이미지 다운로드 → raw/images/{shortcode}.jpg

    - 단일 이미지(GraphImage): {shortcode}.jpg
    - 캐러셀(GraphSidecar): {shortcode}_1.jpg, {shortcode}_2.jpg, ...
    - 동영상(GraphVideo): 썸네일을 {shortcode}.jpg로 저장

    Args:
        loader: Instaloader 인스턴스
        posts: 게시물 딕셔너리 리스트
        raw_dir: raw/ 디렉토리 경로
        delay_min: 요청 간 최소 딜레이(초)
        delay_max: 요청 간 최대 딜레이(초)
    """
    images_dir = raw_dir / "images"
    total = len(posts)
    downloaded = 0
    failed = 0

    logger.info("이미지 다운로드 시작 — %d개 게시물 대상", total)

    for idx, post_data in enumerate(posts):
        shortcode = post_data["shortcode"]
        typename = post_data["typename"]
        url = post_data["url"]

        try:
            if typename == "GraphSidecar":
                # 캐러셀: 각 슬라이드 다운로드
                try:
                    post = instaloader.Post.from_shortcode(loader.context, shortcode)
                    for slide_idx, node in enumerate(post.get_sidecar_nodes(), start=1):
                        img_path = images_dir / f"{shortcode}_{slide_idx}.jpg"
                        _download_single_image(node.display_url, img_path)
                        downloaded += 1
                        _random_delay(delay_min, delay_max)
                except Exception as e:
                    # 캐러셀 순회 실패 시 대표 이미지만 다운로드
                    logger.debug("캐러셀 슬라이드 접근 실패 (%s): %s — 대표 이미지만 저장", shortcode, e)
                    img_path = images_dir / f"{shortcode}.jpg"
                    _download_single_image(url, img_path)
                    downloaded += 1
            else:
                # 단일 이미지 또는 동영상 썸네일
                img_path = images_dir / f"{shortcode}.jpg"
                _download_single_image(url, img_path)
                downloaded += 1

        except Exception as e:
            logger.warning("이미지 다운로드 실패 — %s: %s — 건너뜀", shortcode, e)
            failed += 1

        if (idx + 1) % 20 == 0:
            logger.info("이미지 다운로드 진행: %d/%d 게시물 처리", idx + 1, total)

        _random_delay(delay_min, delay_max)

    logger.info("이미지 다운로드 완료 — 성공: %d, 실패: %d", downloaded, failed)


def _download_single_image(url: str, save_path: Path) -> None:
    """단일 이미지 URL을 파일로 저장

    Args:
        url: 이미지 URL
        save_path: 저장 경로
    """
    urllib.request.urlretrieve(url, str(save_path))
    logger.debug("이미지 저장: %s", save_path.name)


# ──────────────────────────────────────────────
# 메인 수집 함수
# ──────────────────────────────────────────────


def collect(channel: str, data_dir: Path) -> bool:
    """인스타그램 채널 데이터 수집 — 메인 진입점

    프로필, 게시물, 댓글, 이미지를 순차적으로 수집하여
    data/{channel}/raw/ 디렉토리에 저장합니다.

    Args:
        channel: 인스타그램 채널명 (@없이)
        data_dir: data/{channel}/ 경로

    Returns:
        True: 수집 성공 (일부 실패 포함), False: 치명적 에러 (프로필 접근 불가 등)
    """
    raw_dir = data_dir / "raw"
    config = _load_config()
    ig = config["instagram"]

    # Instaloader 인스턴스 생성
    loader = _create_loader(config)

    # 프로필 로드
    logger.info("프로필 접근 시도 — @%s", channel)
    try:
        profile = _retry_on_error(
            instaloader.Profile.from_username,
            ig["retry_on_429"],
            ig["retry_wait_base"],
            loader.context,
            channel,
        )
    except instaloader.exceptions.ProfileNotExistsException:
        logger.error("프로필을 찾을 수 없음 — @%s", channel)
        return False
    except Exception as e:
        logger.error("프로필 접근 실패 — @%s: %s", channel, e)
        return False

    # 비공개 계정 확인
    if profile.is_private:
        logger.error("비공개 계정 — @%s (공개 계정만 분석 가능)", channel)
        return False

    # 1. 프로필 메타데이터 수집
    _collect_profile(profile, raw_dir)

    # 2. 게시물 수집
    posts = _collect_posts(
        profile,
        raw_dir,
        max_posts=ig["max_posts"],
        delay_min=ig["delay_min"],
        delay_max=ig["delay_max"],
    )

    if not posts:
        logger.warning("수집된 게시물이 없음 — @%s", channel)
        return True  # 프로필은 수집됐으므로 성공으로 처리

    # 3. 댓글 수집
    _collect_comments(
        loader,
        posts,
        raw_dir,
        delay_min=ig["comment_delay_min"],
        delay_max=ig["comment_delay_max"],
    )

    # 4. 이미지 다운로드
    _download_images(
        loader,
        posts,
        raw_dir,
        delay_min=ig["delay_min"],
        delay_max=ig["delay_max"],
    )

    logger.info("전체 수집 완료 — @%s (게시물 %d개)", channel, len(posts))
    return True
