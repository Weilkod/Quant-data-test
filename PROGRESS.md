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

## 브랜치 현황
모든 작업 main에 머지 완료. 피처 브랜치 정리됨.

## 주요 결정 사항
- 댓글 수집 기본 비활성화 (`--with-comments` 플래그로만 활성화)
- 캐러셀 이미지 게시물당 최대 5장
- Vision 이미지 ≤1024px 리사이즈 후 base64 인코딩
- Claude 모델: 텍스트 분류 → Haiku, Vision/서사 → Sonnet

## PROGRESS.md 업데이트 방법
작업 완료 시 해당 항목 `[ ]` → `[x]` 로 변경하고 커밋.
