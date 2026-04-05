import os
import requests
import csv
import time
from datetime import datetime

# ==========================================
# 1. 설정 및 API 키 로드
# ==========================================
FMP_API_KEY = os.environ.get("Wz4nOOqLm8sTMyindByujmGybLmFRyY6")
FRED_API_KEY = os.environ.get("ae2279093d5c518c3d15904012a2146b")

START_DATE = "2013-06-01"
END_DATE = "2024-12-31"

TICKERS = [
    "NVDA", "ASML", "AMZN", "CRWD", "V", "LLY", "GE", "XOM", "KO", "VZ",
    "AMD", "AMAT", "AAPL", "PANW", "MA", "ABBV", "TSLA", "CVX", "PEP", "AMT",
    "AVGO", "MSFT", "GOOGL", "JPM", "BRK-B", "PFE", "HD", "NEE", "COST", "PLD",
    "QCOM", "CRM", "META", "BAC", "UNH", "CAT", "NKE", "LIN", "WMT", "TXN",
    "TSM", "ORCL", "NFLX", "GS", "JNJ", "HON", "MCD", "PG", "T", "INTC"
]

FRED_SERIES = {
    "VIXCLS": "vix.csv",
    "T10Y2Y": "yield_spread.csv",
    "ICSA": "claims.csv",
    "CPIAUCSL": "cpi.csv",
    "DCOILWTICO": "wti.csv",
    "GOLDAMGBD228NLBM": "gold.csv",
    "PCOPPUSDM": "copper.csv",
    "DTWEXBGS": "dxy.csv"
}

# ==========================================
# 2. 폴더 구조 생성
# ==========================================
def create_directories():
    dirs = [
        "backtest_data/prices",
        "backtest_data/macro",
        "backtest_data/benchmark",
        "backtest_data/fundamentals"
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
    print("✅ 작업 폴더 구조 생성 완료")

# ==========================================
# 3. 데이터 수집 함수
# ==========================================
def fetch_fmp_prices():
    print("\n[1/3] 종목별 일별 주가 수집 시작 (FMP API)...")
    if not FMP_API_KEY:
        print("❌ FMP_API_KEY가 설정되지 않아 주가 수집을 건너뜁니다.")
        return

    for ticker in TICKERS:
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={START_DATE}&to={END_DATE}&apikey={FMP_API_KEY}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if "historical" in data:
                filepath = f"backtest_data/prices/{ticker}.csv"
                with open(filepath, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
                    
                    # FMP 데이터는 최신순이므로 과거순으로 정렬이 필요할 수 있으나, 명세서 형식 유지를 위해 그대로 작성
                    for row in data["historical"]:
                        writer.writerow([
                            row.get("date"), row.get("open"), row.get("high"), 
                            row.get("low"), row.get("close"), row.get("adjClose"), row.get("volume")
                        ])
                print(f"  - {ticker}.csv 저장 완료")
            else:
                print(f"  - {ticker} 데이터 없음 또는 한도 초과")
        else:
            print(f"  - {ticker} API 요청 실패 ({response.status_code})")
        
        # 무료 API 한도(초당 호출 제한 등)를 고려한 대기
        time.sleep(0.5)

def fetch_fred_macro():
    print("\n[2/3] FRED 매크로 경제지표 수집 시작...")
    if not FRED_API_KEY:
        print("❌ FRED_API_KEY가 설정되지 않아 매크로 수집을 건너뜁니다.")
        return

    for series_id, filename in FRED_SERIES.items():
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&observation_start={START_DATE}&observation_end={END_DATE}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if "observations" in data:
                filepath = f"backtest_data/macro/{filename}"
                with open(filepath, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(["Date", "Value"])
                    for obs in data["observations"]:
                        writer.writerow([obs.get("date"), obs.get("value")])
                print(f"  - {filename} 저장 완료")
        else:
            print(f"  - {series_id} API 요청 실패 ({response.status_code})")
        
        time.sleep(0.5)

def fetch_fmp_fundamentals():
    print("\n[3/3] 종목별 연간 재무제표 수집 시작 (선택 사항, FMP API)...")
    if not FMP_API_KEY:
        print("❌ FMP_API_KEY가 설정되지 않아 재무제표 수집을 건너뜁니다.")
        return

    # 참고: 명세서의 macrotrends 항목들을 FMP Income Statement, Balance Sheet, Cash Flow 에서 조합해야 합니다.
    # 이 스크립트는 FMP의 Income Statement를 예시로 기본 데이터를 가져옵니다.
    for ticker in TICKERS:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=12&apikey={FMP_API_KEY}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                filepath = f"backtest_data/fundamentals/{ticker}.csv"
                with open(filepath, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    # 명세서 기준 컬럼 최소화 매핑 (실제 데이터는 Income, Balance, Cashflow 3개 엔드포인트 병합 필요)
                    writer.writerow(["year", "revenue", "operating_income", "net_income", "eps"])
                    
                    for row in data:
                        year = row.get("calendarYear")
                        if year and 2013 <= int(year) <= 2024:
                            writer.writerow([
                                year,
                                row.get("revenue"),
                                row.get("operatingIncome"),
                                row.get("netIncome"),
                                row.get("eps")
                            ])
                print(f"  - {ticker}.csv (재무) 저장 완료")
        
        time.sleep(0.5)

# ==========================================
# 4. 메인 실행부
# ==========================================
if __name__ == "__main__":
    print("🚀 백테스트 데이터 수집 스크립트 시작")
    create_directories()
    fetch_fmp_prices()
    fetch_fred_macro()
    fetch_fmp_fundamentals()
    print("\n✅ 모든 자동 수집 프로세스가 완료되었습니다.")
    print("참고: S&P 500 벤치마크(항목 3)는 명세서 안내에 따라 Yahoo Finance에서 수동 다운로드하여 backtest_data/benchmark/SP500.csv 로 저장해 주세요.")
