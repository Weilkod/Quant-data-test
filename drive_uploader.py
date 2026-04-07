"""
Google Drive 업로드 모듈 — 분석 결과 영구 저장 전담

CLAUDE.md 규칙:
    - 단일 책임: 업로드만 담당
    - Retry: 3회 지수 백오프 (2/4/8초)
    - 인증: 서비스 계정 JSON 키 (config/credentials.json)
    - config.yaml에서 설정 로드
    - logging만 사용, print() 금지

Google Drive 폴더 구조:
    Instagram Channel Analysis/
      {channel}/
        raw/        ← profile.json, posts.csv, comments.csv, images/
        analysis/   ← categories.json, caption_style.json, ...
        report/     ← report.pptx, charts/
"""

import logging
import time
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 설정 로드
# ──────────────────────────────────────────────
_CONFIG_PATH = Path(__file__).parent / "config" / "config.yaml"


def _load_drive_config() -> dict:
    """config.yaml에서 Drive 설정 로드

    Returns:
        drive 설정 dict (credentials_file, folder_name)
    """
    defaults = {
        "credentials_file": "config/credentials.json",
        "folder_name": "Instagram Channel Analysis",
    }
    if not _CONFIG_PATH.exists():
        logger.warning("config.yaml 없음 — 기본 Drive 설정 사용")
        return defaults

    with open(_CONFIG_PATH, encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    drive_config = config.get("drive", {})
    return {
        "credentials_file": drive_config.get("credentials_file", defaults["credentials_file"]),
        "folder_name": drive_config.get("folder_name", defaults["folder_name"]),
    }


# ──────────────────────────────────────────────
# Google Drive 인증
# ──────────────────────────────────────────────
def _authenticate(credentials_file: str):
    """서비스 계정으로 Google Drive API 인증

    Args:
        credentials_file: 서비스 계정 JSON 키 파일 경로

    Returns:
        Google Drive API 서비스 객체

    Raises:
        FileNotFoundError: credentials.json 없음
        Exception: 인증 실패
    """
    cred_path = Path(credentials_file)
    if not cred_path.exists():
        raise FileNotFoundError(
            f"Google Drive 인증 파일 없음: {cred_path}\n"
            "Google Cloud Console에서 서비스 계정 키를 다운로드하여 "
            f"{cred_path}에 저장하세요."
        )

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/drive"]
    credentials = service_account.Credentials.from_service_account_file(
        str(cred_path), scopes=scopes
    )
    service = build("drive", "v3", credentials=credentials)
    logger.info("Google Drive 인증 성공")
    return service


# ──────────────────────────────────────────────
# 폴더 관리
# ──────────────────────────────────────────────
def _find_folder(service, name: str, parent_id: str | None = None) -> str | None:
    """Drive에서 폴더 검색

    Args:
        service: Drive API 서비스
        name: 폴더명
        parent_id: 부모 폴더 ID (None이면 전체 검색)

    Returns:
        폴더 ID 또는 None
    """
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    results = service.files().list(
        q=query, spaces="drive", fields="files(id, name)", pageSize=1
    ).execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


def _create_folder(service, name: str, parent_id: str | None = None) -> str:
    """Drive에 폴더 생성

    Args:
        service: Drive API 서비스
        name: 폴더명
        parent_id: 부모 폴더 ID

    Returns:
        생성된 폴더 ID
    """
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        metadata["parents"] = [parent_id]

    folder = service.files().create(body=metadata, fields="id").execute()
    folder_id = folder["id"]
    logger.info("Drive 폴더 생성: %s (ID: %s)", name, folder_id)
    return folder_id


def _ensure_folder(service, name: str, parent_id: str | None = None) -> str:
    """폴더가 없으면 생성, 있으면 ID 반환"""
    folder_id = _find_folder(service, name, parent_id)
    if folder_id:
        logger.debug("기존 폴더 사용: %s (ID: %s)", name, folder_id)
        return folder_id
    return _create_folder(service, name, parent_id)


def _ensure_folder_path(service, root_name: str, channel: str) -> dict[str, str]:
    """채널별 전체 폴더 구조 생성

    Returns:
        {"root": id, "channel": id, "raw": id, "analysis": id, "report": id}
    """
    root_id = _ensure_folder(service, root_name)
    channel_id = _ensure_folder(service, channel, root_id)
    raw_id = _ensure_folder(service, "raw", channel_id)
    analysis_id = _ensure_folder(service, "analysis", channel_id)
    report_id = _ensure_folder(service, "report", channel_id)

    return {
        "root": root_id,
        "channel": channel_id,
        "raw": raw_id,
        "analysis": analysis_id,
        "report": report_id,
    }


# ──────────────────────────────────────────────
# 파일 업로드
# ──────────────────────────────────────────────
_RETRY_MAX = 3
_RETRY_DELAYS = [2, 4, 8]  # 지수 백오프 (초)


def _upload_file(service, local_path: Path, parent_id: str) -> str | None:
    """단일 파일 업로드 (재시도 포함)

    Args:
        service: Drive API 서비스
        local_path: 업로드할 로컬 파일 경로
        parent_id: 업로드할 Drive 폴더 ID

    Returns:
        업로드된 파일 ID 또는 None (실패 시)
    """
    from googleapiclient.http import MediaFileUpload

    metadata = {
        "name": local_path.name,
        "parents": [parent_id],
    }

    # 기존 파일 덮어쓰기 (같은 이름 파일 검색)
    existing = service.files().list(
        q=f"name='{local_path.name}' and '{parent_id}' in parents and trashed=false",
        spaces="drive", fields="files(id)", pageSize=1,
    ).execute().get("files", [])

    # MIME 타입 추정
    mime_map = {
        ".json": "application/json",
        ".csv": "text/csv",
        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".html": "text/html",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
    }
    mime_type = mime_map.get(local_path.suffix.lower(), "application/octet-stream")

    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)

    for attempt in range(_RETRY_MAX):
        try:
            if existing:
                # 기존 파일 업데이트
                file_id = existing[0]["id"]
                service.files().update(
                    fileId=file_id, media_body=media
                ).execute()
                logger.debug("파일 업데이트: %s", local_path.name)
                return file_id
            else:
                # 새 파일 생성
                result = service.files().create(
                    body=metadata, media_body=media, fields="id"
                ).execute()
                logger.debug("파일 업로드: %s", local_path.name)
                return result["id"]
        except Exception as e:
            if attempt < _RETRY_MAX - 1:
                delay = _RETRY_DELAYS[attempt]
                logger.warning(
                    "업로드 실패 (%s), %d초 후 재시도 (%d/%d): %s",
                    local_path.name, delay, attempt + 1, _RETRY_MAX, e
                )
                time.sleep(delay)
            else:
                logger.error("업로드 최종 실패: %s — %s", local_path.name, e)
                return None


def _upload_directory(service, local_dir: Path, parent_id: str) -> int:
    """디렉토리 내 파일을 재귀적으로 업로드

    Args:
        service: Drive API 서비스
        local_dir: 업로드할 로컬 디렉토리
        parent_id: 업로드할 Drive 폴더 ID

    Returns:
        성공적으로 업로드된 파일 수
    """
    if not local_dir.exists():
        logger.debug("디렉토리 없음 (건너뜀): %s", local_dir)
        return 0

    uploaded = 0
    for item in sorted(local_dir.iterdir()):
        if item.name.startswith("."):
            continue
        if item.is_dir():
            sub_folder_id = _ensure_folder(service, item.name, parent_id)
            uploaded += _upload_directory(service, item, sub_folder_id)
        elif item.is_file():
            result = _upload_file(service, item, parent_id)
            if result:
                uploaded += 1

    return uploaded


# ──────────────────────────────────────────────
# 메인 진입점
# ──────────────────────────────────────────────
def upload_to_drive(channel: str, data_dir: Path) -> bool:
    """채널 분석 결과를 Google Drive에 업로드

    Args:
        channel: 채널명 (@없이)
        data_dir: data/{channel}/ 경로

    Returns:
        True: 업로드 성공, False: 실패
    """
    logger.info("Google Drive 업로드 시작 — @%s", channel)

    # 설정 로드
    config = _load_drive_config()

    # 인증
    try:
        service = _authenticate(config["credentials_file"])
    except FileNotFoundError as e:
        logger.error(str(e))
        return False
    except Exception as e:
        logger.error("Google Drive 인증 실패: %s", e)
        return False

    # 폴더 구조 생성
    try:
        folders = _ensure_folder_path(service, config["folder_name"], channel)
    except Exception as e:
        logger.error("Drive 폴더 생성 실패: %s", e)
        return False

    # 각 디렉토리 업로드
    total_uploaded = 0

    # raw/ 업로드
    raw_count = _upload_directory(service, data_dir / "raw", folders["raw"])
    total_uploaded += raw_count
    logger.info("raw/ 업로드 완료: %d개 파일", raw_count)

    # analysis/ 업로드
    analysis_count = _upload_directory(service, data_dir / "analysis", folders["analysis"])
    total_uploaded += analysis_count
    logger.info("analysis/ 업로드 완료: %d개 파일", analysis_count)

    # report/ 업로드
    report_count = _upload_directory(service, data_dir / "report", folders["report"])
    total_uploaded += report_count
    logger.info("report/ 업로드 완료: %d개 파일", report_count)

    logger.info(
        "Google Drive 업로드 완료 — @%s, 총 %d개 파일",
        channel, total_uploaded
    )
    return True
