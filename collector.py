"""
인스타그램 데이터 수집 모듈 — instagrapi 기반

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
import re
import shutil
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yaml
from instagrapi import Client
from instagrapi.exceptions import (
    ChallengeRequired,
    LoginRequired,
    PleaseWaitFewMinutes,
    RateLimitError,
)

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 설정 기본값
# ──────────────────────────────────────────────
_DEFAULT_CONFIG: dict = {
    "instagram": {
        "username": "",
        "password": "",
        "session_file": "",
        "max_posts": 20,
        "delay_min": 2,
        "delay_max": 4,
        "comment_delay_min": 1,
        "comment_delay_max": 2,
        "retry_on_429": 3,
        "retry_wait_base": 60,
    }
}

# instagrapi 미디어 타입 → 보고서용 이름 매핑
_MEDIA_TYPE_MAP: dict[int, str] = {
    1: "GraphImage",      # 단일 이미지
    2: "GraphVideo",      # 동영상/릴스
    8: "GraphSidecar",    # 캐러셀 (앨범)
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
            RateLimitError,
            PleaseWaitFewMinutes,
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
# instagrapi 클라이언트 생성
# ──────────────────────────────────────────────


def _create_client(config: dict) -> Client:
    """instagrapi Client 생성 및 로그인

    세션 파일이 있으면 로드, 없으면 username/password로 로그인
    """
    cl = Client()
    # 요청 간 딜레이 설정 (instagrapi 내부)
    cl.delay_range = [
        config["instagram"]["delay_min"],
        config["instagram"]["delay_max"],
    ]

    ig_config = config["instagram"]
    username = ig_config.get("username", "")
    password = ig_config.get("password", "")
    session_file = ig_config.get("session_file", "")

    # 1순위: 세션 파일로 로그인
    if session_file:
        session_path = Path(session_file)
        if session_path.exists():
            try:
                cl.load_settings(str(session_path))
                cl.login(username, password) if username and password else None
                logger.info("세션 로드 완료 — %s", session_path)
                return cl
            except Exception as e:
                logger.warning("세션 로드 실패: %s — 직접 로그인 시도", e)

    # 2순위: username/password로 로그인
    if username and password:
        try:
            cl.login(username, password)
            # 세션 저장 (다음 실행 시 재사용)
            if session_file:
                cl.dump_settings(str(session_file))
                logger.info("세션 저장 완료 — %s", session_file)
            logger.info("로그인 완료 — 사용자: %s", username)
            return cl
        except ChallengeRequired:
            logger.error("Instagram 챌린지(인증) 필요 — 브라우저에서 먼저 로그인 후 재시도")
            raise
        except Exception as e:
            logger.error("로그인 실패: %s", e)
            raise

    logger.warning("로그인 정보 미설정 — 비로그인으로 수집 진행 (제한적)")
    return cl


# ──────────────────────────────────────────────
# 프로필 수집
# ──────────────────────────────────────────────


def _collect_profile(cl: Client, channel: str, raw_dir: Path) -> dict:
    """프로필 메타데이터 수집 → raw/profile.json 저장

    Args:
        cl: instagrapi Client
        channel: 채널명
        raw_dir: raw/ 디렉토리 경로

    Returns:
        프로필 데이터 딕셔너리
    """
    user = cl.user_info_by_username(channel)

    profile_data = {
        "username": user.username,
        "full_name": user.full_name,
        "followers": user.follower_count,
        "followees": user.following_count,
        "biography": user.biography or "",
        "external_url": str(user.external_url) if user.external_url else "",
        "mediacount": user.media_count,
        "profile_pic_url": str(user.profile_pic_url) if user.profile_pic_url else "",
        "is_verified": user.is_verified,
        "business_category_name": user.business_category_name or "",
        "is_private": user.is_private,
        "pk": str(user.pk),
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }

    output_path = raw_dir / "profile.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(profile_data, f, ensure_ascii=False, indent=2)

    logger.info(
        "프로필 수집 완료 — @%s (팔로워: %s, 게시물: %s)",
        user.username,
        f"{user.follower_count:,}",
        f"{user.media_count:,}",
    )
    return profile_data


# ──────────────────────────────────────────────
# 게시물 수집
# ──────────────────────────────────────────────


def _collect_posts(
    cl: Client,
    user_id: str,
    raw_dir: Path,
    max_posts: int,
    delay_min: float,
    delay_max: float,
) -> list[dict]:
    """게시물 데이터 수집 → raw/posts.csv 저장

    Args:
        cl: instagrapi Client
        user_id: 사용자 PK (숫자 문자열)
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
        medias = cl.user_medias(int(user_id), amount=max_posts)

        for i, media in enumerate(medias):
            try:
                caption = media.caption_text or ""
                # 캡션에서 해시태그/멘션 추출
                hashtags = re.findall(r"#(\w+)", caption)
                mentions = re.findall(r"@(\w+)", caption)

                typename = _MEDIA_TYPE_MAP.get(media.media_type, "Unknown")

                post_dict = {
                    "shortcode": media.code,
                    "pk": str(media.pk),
                    "date_utc": media.taken_at.isoformat() if media.taken_at else "",
                    "caption": caption,
                    "likes": media.like_count,
                    "comments": media.comment_count,
                    "typename": typename,
                    "caption_hashtags": ",".join(hashtags),
                    "caption_mentions": ",".join(mentions),
                    "url": f"https://www.instagram.com/p/{media.code}/",
                    "mediacount": len(media.resources) if media.resources else 1,
                    "thumbnail_url": str(media.thumbnail_url) if media.thumbnail_url else "",
                }
                posts_data.append(post_dict)

                if (i + 1) % 10 == 0:
                    logger.info("게시물 수집 진행: %d/%d", i + 1, len(medias))

            except Exception as e:
                logger.warning("게시물 수집 실패 (인덱스 %d): %s — 건너뜀", i, e)

            # 매 요청 사이 랜덤 딜레이 (절대 생략 금지)
            _random_delay(delay_min, delay_max)

    except KeyboardInterrupt:
        logger.warning("사용자 중단 — 현재까지 수집된 %d개 게시물 저장", len(posts_data))
    except Exception as e:
        logger.error("게시물 수집 중 에러: %s — 현재까지 수집된 데이터 저장", e)
    finally:
        # 수집된 데이터가 있으면 항상 저장
        if posts_data:
            df = pd.DataFrame(posts_data)
            output_path = raw_dir / "posts.csv"
            df.to_csv(output_path, index=False, encoding="utf-8-sig")
            logger.info("게시물 저장 완료: %d개 → %s", len(posts_data), output_path)

    return posts_data


# ──────────────────────────────────────────────
# 댓글 수집
# ──────────────────────────────────────────────


def _collect_comments(
    cl: Client,
    posts: list[dict],
    raw_dir: Path,
    delay_min: float,
    delay_max: float,
) -> None:
    """게시물별 댓글 수집 → raw/comments.csv 저장

    Args:
        cl: instagrapi Client
        posts: 게시물 딕셔너리 리스트
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
            media_pk = post_data["pk"]
            comment_count = post_data["comments"]

            # 댓글이 없는 게시물은 건너뜀
            if comment_count == 0:
                continue

            try:
                comments = cl.media_comments(media_pk, amount=0)

                for comment in comments:
                    try:
                        comment_dict = {
                            "post_shortcode": shortcode,
                            "comment_id": str(comment.pk),
                            "text": comment.text,
                            "owner_username": comment.user.username,
                            "created_at_utc": comment.created_at.isoformat() if comment.created_at else "",
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
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logger.info("댓글 저장 완료: %d개 → %s", len(all_comments), output_path)


# ──────────────────────────────────────────────
# 이미지 다운로드
# ──────────────────────────────────────────────


def _download_images(
    cl: Client,
    posts: list[dict],
    raw_dir: Path,
    delay_min: float,
    delay_max: float,
) -> None:
    """게시물 이미지 다운로드 → raw/images/{shortcode}.jpg

    - 단일 이미지(GraphImage): {shortcode}.jpg
    - 캐러셀(GraphSidecar): {shortcode}_1.jpg ~ {shortcode}_5.jpg (최대 5장)
    - 동영상(GraphVideo): 썸네일을 {shortcode}.jpg로 저장

    Args:
        cl: instagrapi Client
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
        media_pk = post_data["pk"]
        thumbnail_url = post_data.get("thumbnail_url", "")

        try:
            if typename == "GraphSidecar":
                # 캐러셀: 최대 5장까지만 다운로드 (비용 절감)
                try:
                    paths = cl.album_download(int(media_pk), folder=str(images_dir))
                    max_slides = 5
                    for slide_idx, src_path in enumerate(paths, start=1):
                        if slide_idx <= max_slides:
                            dst_path = images_dir / f"{shortcode}_{slide_idx}.jpg"
                            shutil.move(str(src_path), str(dst_path))
                            downloaded += 1
                        else:
                            # 초과분 임시 파일 삭제
                            Path(src_path).unlink(missing_ok=True)
                except Exception as e:
                    logger.debug("캐러셀 다운로드 실패 (%s): %s — 썸네일만 저장", shortcode, e)
                    if thumbnail_url:
                        img_path = images_dir / f"{shortcode}.jpg"
                        _download_url(thumbnail_url, img_path)
                        downloaded += 1

            elif typename == "GraphVideo":
                # 동영상: 썸네일만 저장 (영상 자체는 불필요)
                if thumbnail_url:
                    img_path = images_dir / f"{shortcode}.jpg"
                    _download_url(thumbnail_url, img_path)
                    downloaded += 1

            else:
                # 단일 이미지
                try:
                    src_path = cl.photo_download(int(media_pk), folder=str(images_dir))
                    dst_path = images_dir / f"{shortcode}.jpg"
                    shutil.move(str(src_path), str(dst_path))
                    downloaded += 1
                except Exception as e:
                    logger.debug("이미지 API 다운로드 실패 (%s): %s — URL로 시도", shortcode, e)
                    if thumbnail_url:
                        img_path = images_dir / f"{shortcode}.jpg"
                        _download_url(thumbnail_url, img_path)
                        downloaded += 1

        except Exception as e:
            logger.warning("이미지 다운로드 실패 — %s: %s — 건너뜀", shortcode, e)
            failed += 1

        if (idx + 1) % 20 == 0:
            logger.info("이미지 다운로드 진행: %d/%d 게시물 처리", idx + 1, total)

        _random_delay(delay_min, delay_max)

    # 다운로드 후 instagrapi가 남긴 임시 파일 정리
    _cleanup_temp_files(images_dir)

    logger.info("이미지 다운로드 완료 — 성공: %d, 실패: %d", downloaded, failed)


def _download_url(url: str, save_path: Path) -> None:
    """URL에서 파일 직접 다운로드

    Args:
        url: 다운로드 URL
        save_path: 저장 경로
    """
    urllib.request.urlretrieve(url, str(save_path))
    logger.debug("이미지 저장: %s", save_path.name)


def _cleanup_temp_files(images_dir: Path) -> None:
    """instagrapi가 남긴 임시 파일(.json 등) 정리"""
    for f in images_dir.iterdir():
        if f.suffix in (".json", ".mp4") or f.name.endswith(".json.xz"):
            f.unlink(missing_ok=True)


# ──────────────────────────────────────────────
# 메인 수집 함수
# ──────────────────────────────────────────────


def collect(channel: str, data_dir: Path, with_comments: bool = False) -> bool:
    """인스타그램 채널 데이터 수집 — 메인 진입점

    프로필, 게시물, 이미지를 순차적으로 수집하여
    data/{channel}/raw/ 디렉토리에 저장합니다.
    댓글 수집은 기본 비활성화 (--with-comments 플래그로 활성화).

    Args:
        channel: 인스타그램 채널명 (@없이)
        data_dir: data/{channel}/ 경로
        with_comments: True면 댓글도 수집 (기본 False — Instagram API 제한으로 비활성화)

    Returns:
        True: 수집 성공 (일부 실패 포함), False: 치명적 에러 (프로필 접근 불가 등)
    """
    raw_dir = data_dir / "raw"
    config = _load_config()
    ig = config["instagram"]

    # instagrapi 클라이언트 생성
    try:
        cl = _create_client(config)
    except Exception as e:
        logger.error("Instagram 클라이언트 생성 실패: %s", e)
        return False

    # 프로필 수집
    logger.info("프로필 접근 시도 — @%s", channel)
    try:
        profile_data = _retry_on_error(
            _collect_profile,
            ig["retry_on_429"],
            ig["retry_wait_base"],
            cl,
            channel,
            raw_dir,
        )
    except Exception as e:
        logger.error("프로필 접근 실패 — @%s: %s", channel, e)
        return False

    # 비공개 계정 확인
    if profile_data.get("is_private"):
        logger.error("비공개 계정 — @%s (공개 계정만 분석 가능)", channel)
        return False

    user_id = profile_data["pk"]

    # 게시물 수집
    posts = _collect_posts(
        cl,
        user_id,
        raw_dir,
        max_posts=ig["max_posts"],
        delay_min=ig["delay_min"],
        delay_max=ig["delay_max"],
    )

    if not posts:
        logger.warning("수집된 게시물이 없음 — @%s", channel)
        return True  # 프로필은 수집됐으므로 성공으로 처리

    # 댓글 수집 (기본 비활성화 — --with-comments 플래그로 활성화)
    if with_comments:
        _collect_comments(
            cl,
            posts,
            raw_dir,
            delay_min=ig["comment_delay_min"],
            delay_max=ig["comment_delay_max"],
        )
    else:
        logger.info("댓글 수집 건너뜀 (활성화: --with-comments 플래그 사용)")

    # 이미지 다운로드
    _download_images(
        cl,
        posts,
        raw_dir,
        delay_min=ig["delay_min"],
        delay_max=ig["delay_max"],
    )

    logger.info("전체 수집 완료 — @%s (게시물 %d개)", channel, len(posts))
    return True
