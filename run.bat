@echo off
chcp 65001 >nul
title 인스타그램 채널 분석기

echo ========================================
echo   인스타그램 채널 분석기 - 자동 업데이트
echo ========================================
echo.

echo [1/3] 최신 코드 업데이트 중...
git pull origin main
echo.

echo [2/3] 패키지 확인 중...
pip install -r requirements.txt --quiet
echo.

echo [3/3] 앱 실행 중...
python desktop_app.py

if %errorlevel% neq 0 (
    echo.
    echo [오류] 앱 실행에 실패했습니다.
    pause
)
