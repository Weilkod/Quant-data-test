#!/usr/bin/env bash
# SessionStart hook — 세션 시작 시 프로젝트 컨텍스트 출력

PROJECT_DIR="/home/user/Insta_analyse"
PROGRESS_FILE="$PROJECT_DIR/PROGRESS.md"

echo "=== Insta_analyse 프로젝트 컨텍스트 ==="
echo ""

# 현재 브랜치 및 최근 커밋
echo "[ Git 상태 ]"
cd "$PROJECT_DIR" && git branch --show-current 2>/dev/null | xargs -I{} echo "브랜치: {}"
echo "최근 커밋:"
git log --oneline -3 2>/dev/null | sed 's/^/  /'
echo ""

# PROGRESS.md 내용 출력
if [ -f "$PROGRESS_FILE" ]; then
  echo "[ 진행 상황 ]"
  cat "$PROGRESS_FILE"
else
  echo "[ PROGRESS.md 없음 ]"
fi

echo ""
echo "======================================="
