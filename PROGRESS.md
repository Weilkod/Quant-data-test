# 프로젝트 진행 상황

## 완료된 단계
- [x] collector.py — instagrapi 수집, 세션 관리, 429 retry
- [x] estimator.py — 순수 함수, coefficients.yaml 기반 추정
- [x] analyzer.py — Claude API (Haiku/Sonnet), 캐싱, 배치 처리

## 진행중
- [ ] reporter.py — PPT/HTML 리포트 생성 (claude/setup-collector-no-ai-UlkrO 세션)
- [ ] drive_uploader.py
- [ ] main.py 통합 (모든 플래그 연결)
- [ ] app.py (Streamlit UI)

## 브랜치 현황
| 브랜치 | 작업 내용 |
|--------|-----------|
| claude/setup-collector-no-ai-UlkrO | reporter.py 작성 중 |
| claude/auto-session-logging-vkIZr | SessionStart hook 설정 |

## 주요 결정 사항
- 댓글 수집 기본 비활성화 (`--with-comments` 플래그로만 활성화)
- 캐러셀 이미지 게시물당 최대 5장
- Vision 이미지 ≤1024px 리사이즈 후 base64 인코딩
- Claude 모델: 텍스트 분류 → Haiku, Vision/서사 → Sonnet

## PROGRESS.md 업데이트 방법
작업 완료 시 해당 항목 `[ ]` → `[x]` 로 변경하고 커밋.
