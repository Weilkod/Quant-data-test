# 프로젝트 진행 상황

## 완료된 단계
- [x] collector.py — instagrapi 수집, 세션 관리, 429 retry
- [x] estimator.py — 순수 함수, coefficients.yaml 기반 추정
- [x] analyzer.py — Claude API (Haiku/Sonnet), 캐싱, 배치 처리
- [x] reporter.py — PPT 19슬라이드, matplotlib 차트 7종, 3모드 대응
- [x] main.py 통합 (collector → estimator → analyzer → reporter 연결)

## 진행중
- [x] drive_uploader.py — 서비스 계정 인증, 3회 지수 백오프, 폴더 자동 생성
- [x] app.py — Streamlit UI (채널 입력, 업종 선택, 옵션, 진행 바, 다운로드)
- [x] presets/beauty.yaml — B01~B10 카테고리, 저장/공유 보정 계수
- [x] presets/fashion.yaml — S01~S10 카테고리, 저장/공유 보정 계수
- [x] MD 파일 정합성 수정 — CLAUDE.md 참조 경로, INSTA.md D-5 구조도 실제에 맞게 수정
- [x] 429 rate limit fix — 프록시 지원, RetryError 포착, 프로필 fallback, request_timeout

## 테스트 현황 (168개 전부 통과)
- [x] test_estimator.py — 45개: 포맷 매핑, 팔로워 티어 경계값, 추정 계수, enrichment
- [x] test_analyzer.py — 38개: 프롬프트 로딩, 프리셋 로딩, 토큰 추정, 캐싱, 모델 배정
- [x] test_collector.py — 30개: 설정 로딩, 지수 백오프, 비공개 계정 거부, 댓글 플래그, 프록시, RetryError
- [x] test_reporter.py — 21개: JSON/CSV 안전 로딩, 빈 데이터 처리, PPT 전체 생성
- [x] test_drive_uploader.py — 15개: 폴더 관리, 재시도 설정, 인증 실패 처리
- [x] test_main.py — 19개: CLI 인자 파싱, 채널명 정규화, 디렉토리 생성, 로깅

## 브랜치 현황
모든 작업 main에 머지 완료. 피처 브랜치 정리됨.

## 주요 결정 사항
- 댓글 수집 기본 비활성화 (`--with-comments` 플래그로만 활성화)
- 캐러셀 이미지 게시물당 최대 5장
- Vision 이미지 ≤1024px 리사이즈 후 base64 인코딩
- Claude 모델: 텍스트 분류 → Haiku, Vision/서사 → Sonnet
- `--force-reanalyze` 플래그로 캐시된 분석 결과 무시 가능
- 프로젝트 파일 구조: 루트 레벨 (src/ 폴더 미사용)
- 프롬프트 외부 파일 관리: `prompts/*.txt` (인라인 문자열 금지)
- 업종 프리셋: food, beauty, fashion + auto 모드 지원

## PROGRESS.md 업데이트 방법
작업 완료 시 해당 항목 `[ ]` → `[x]` 로 변경하고 커밋.
