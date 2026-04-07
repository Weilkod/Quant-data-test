"""
Mock 데이터 기반 파이프라인 테스트 스크립트

사용법:
    python run_mock_test.py

결과:
    data/mock_food/report/report.pptx  ← PPT 보고서
    data/mock_food/report/charts/*.png ← 차트 이미지
"""

import json
import random
import re
import datetime
from pathlib import Path

import pandas as pd


def create_mock_data() -> Path:
    """mock 데이터 생성 → data/mock_food/"""
    channel = "mock_food"
    base = Path("data") / channel

    for d in [
        base / "raw" / "images",
        base / "analysis",
        base / "report" / "charts",
        base / "report" / "assets",
    ]:
        d.mkdir(parents=True, exist_ok=True)

    # ── profile.json ──
    profile = {
        "username": "mock_food",
        "full_name": "모의 푸드 채널",
        "followers": 85000,
        "followees": 320,
        "biography": "맛집 큐레이션 | 전국 맛집 리뷰\n문의: DM\n#맛집 #푸드",
        "external_url": "https://linktr.ee/mock_food",
        "mediacount": 450,
        "profile_pic_url": "",
        "is_verified": False,
        "business_category_name": "Food & Beverage",
        "is_private": False,
        "pk": "12345678",
        "collected_at": "2026-04-07T12:00:00+00:00",
    }
    (base / "raw" / "profile.json").write_text(
        json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # ── posts.csv (30개 게시물) ──
    captions_pool = [
        ("강남역 숨은 맛집 TOP5 🍜 #맛집 #강남 #추천", "GraphSidecar"),
        ("편의점 신상 꿀조합 3가지! #편의점 #GS25 #꿀팁", "GraphImage"),
        ("이 집 진짜 미쳤다 ㄹㅇ 🔥 #먹방 #맛집추천", "GraphVideo"),
        ("홍대 카페 디저트 투어 ☕ #카페 #디저트 #마카롱", "GraphSidecar"),
        ("프랜차이즈 신메뉴 솔직 리뷰 #신메뉴 #출시 #리뷰", "GraphImage"),
        ("오늘의 꿀조합 레시피 🍳 #레시피 #꿀팁 #만들기", "GraphVideo"),
        ("여름 한정 빙수 맛집 BEST 🍧 #빙수 #여름 #트렌드", "GraphSidecar"),
        ("광고) 새로운 맛을 만나다 #광고 #협찬 #제공", "GraphImage"),
        ("팔로워 제보 맛집! DM 감사 🙏 #제보 #DM #팔로워추천", "GraphImage"),
        ("할인 이벤트 진행중! 🎉 #이벤트 #할인 #프로모션", "GraphVideo"),
        ("을지로 숨은 맛집 3곳 소개합니다 #을지로 #맛집 #추천", "GraphSidecar"),
        ("이마트 간편식 추천 TOP3 #이마트 #간편식 #신상", "GraphImage"),
        ("제주도 맛집 로드 여행기 🏝️ #제주 #맛집 #여행", "GraphSidecar"),
        ("CU 편의점 디저트 신상 후기 #CU #편의점 #디저트", "GraphImage"),
        ("성수동 브런치 카페 추천 ☀️ #성수 #브런치 #카페", "GraphSidecar"),
        ("먹방 챌린지 도전! 대왕 짜장면 🍜 #먹방 #챌린지 #ASMR", "GraphVideo"),
        ("가성비 최고 한식 뷔페 #한식 #뷔페 #가성비", "GraphImage"),
        ("요즘 핫한 트렌드 음식 모음 #트렌드 #핫한 #올해", "GraphSidecar"),
        ("집에서 만드는 파스타 레시피 🍝 #레시피 #파스타 #꿀팁", "GraphVideo"),
        ("주말 데이트 맛집 코스 추천 💕 #데이트 #맛집 #추천", "GraphSidecar"),
        ("연남동 베이커리 투어 🥐 #베이커리 #빵 #연남동", "GraphImage"),
        ("명동 길거리 음식 먹방 🍢 #명동 #길거리 #먹방", "GraphVideo"),
        ("겨울 한정 핫초코 맛집 ☕ #겨울 #핫초코 #카페", "GraphImage"),
        ("치킨 프랜차이즈 비교 리뷰 🍗 #치킨 #신메뉴 #비교", "GraphSidecar"),
        ("오늘 뭐 먹지? 점심 추천 #점심 #추천 #맛집", "GraphImage"),
        ("이 조합 진짜 꿀맛! #꿀조합 #조합 #이렇게먹어", "GraphVideo"),
        ("광고) 프리미엄 한우 세트 #광고 #협찬 #한우", "GraphImage"),
        ("DM으로 온 제보 맛집 후기 #제보 #DM #리그램", "GraphSidecar"),
        ("연말 기프티콘 증정 이벤트 🎁 #이벤트 #증정 #무료", "GraphImage"),
        ("서울 3대 떡볶이 맛집 비교 🌶️ #떡볶이 #맛집 #TOP", "GraphVideo"),
    ]

    rows = []
    base_date = datetime.datetime(2026, 1, 15, 12, 0, 0)
    random.seed(42)

    for i, (caption, typename) in enumerate(captions_pool):
        date = base_date - datetime.timedelta(days=i * 3, hours=random.randint(0, 12))
        hashtags = re.findall(r"#(\w+)", caption)
        mentions = re.findall(r"@(\w+)", caption)
        rows.append({
            "shortcode": f"MOCK{i:03d}",
            "pk": str(1000000 + i),
            "date_utc": date.isoformat(),
            "caption": caption,
            "likes": random.randint(800, 15000),
            "comments": random.randint(10, 500),
            "typename": typename,
            "caption_hashtags": ",".join(hashtags),
            "caption_mentions": ",".join(mentions),
            "url": f"https://www.instagram.com/p/MOCK{i:03d}/",
            "mediacount": random.choice([1, 3, 5]) if typename == "GraphSidecar" else 1,
            "thumbnail_url": "",
        })

    pd.DataFrame(rows).to_csv(base / "raw" / "posts.csv", index=False, encoding="utf-8-sig")

    # ── AI 분석 결과 mock ──
    categories = ["F02", "F05", "F04", "F06", "F01", "F03", "F07", "F08", "F09", "F10"]
    cat_names = [
        "맛집/음식점 추천 큐레이션", "편의점/간편식 추천", "먹방/푸드 엔터테인먼트",
        "카페/디저트 소개", "프랜차이즈 신메뉴 리뷰", "꿀조합/레시피 팁",
        "시즌/트렌드 음식", "브랜드 협찬/광고", "UGC/팔로워 제보", "이벤트/프로모션",
    ]
    classifications = []
    for i in range(30):
        idx = i % 10
        classifications.append({
            "shortcode": f"MOCK{i:03d}",
            "category": categories[idx],
            "category_name": cat_names[idx],
        })

    analysis_data = {
        "categories.json": {"classifications": classifications},
        "caption_style.json": {
            "tone": "캐주얼/친근",
            "avg_length": 45,
            "cta_types": {"질문형": 35, "태그 유도": 25, "링크 유도": 15, "없음": 25},
            "emoji_usage": "높음 (게시물 80%에서 사용)",
            "hashtag_avg": 3.2,
        },
        "sentiment.json": {
            "positive": 72, "neutral": 20, "negative": 8,
            "top_positive_keywords": ["맛있다", "추천", "갈래", "최고", "대박"],
            "top_negative_keywords": ["비싸다", "웨이팅", "별로"],
        },
        "insights.json": {
            "sections": {
                "executive_summary": "mock_food 채널은 팔로워 85,000명 규모의 푸드 미디어 채널로, 맛집 큐레이션과 편의점 신상 리뷰가 핵심 콘텐츠입니다.",
                "content_strategy": "맛집 큐레이션(F02)이 가장 높은 비중을 차지하며, 정보성 콘텐츠가 엔터테인먼트 대비 2배 높은 저장률을 보입니다.",
                "visual_tone": "밝고 따뜻한 색감 위주의 피드로, 음식 클로즈업 사진이 주를 이룹니다.",
                "top_posts_common": "인기 게시물의 공통점: 캐러셀 포맷, 지역 기반 큐레이션, 질문형 CTA 포함.",
                "closing": "1. 캐러셀 포맷 비중 확대 권장\n2. 교육/정보형 콘텐츠 강화\n3. 주말 오전 게시 최적화",
            }
        },
    }

    for name, data in analysis_data.items():
        (base / "analysis" / name).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    print("✅ Mock 데이터 생성 완료")
    return base


def run_pipeline(data_dir: Path) -> None:
    """estimator + reporter 파이프라인 실행"""
    from estimator import load_coefficients, enrich_posts, aggregate_by_format
    from reporter import generate_report

    # 추정치 산출
    coeffs = load_coefficients()
    with open(data_dir / "raw" / "profile.json", encoding="utf-8") as f:
        profile = json.load(f)
    posts_df = pd.read_csv(data_dir / "raw" / "posts.csv")

    enriched = enrich_posts(posts_df, profile["followers"], coeffs)
    enriched.to_csv(
        data_dir / "analysis" / "posts_enriched.csv",
        index=False, encoding="utf-8-sig",
    )
    format_stats = aggregate_by_format(enriched)
    format_stats.to_csv(
        data_dir / "analysis" / "format_stats.csv",
        index=False, encoding="utf-8-sig",
    )
    print(f"✅ 추정치 산출 완료 — 게시물 {len(enriched)}개")

    # 보고서 생성
    report_path = generate_report("mock_food", data_dir)
    print(f"✅ 보고서 생성 완료 → {report_path}")
    print(f"   파일 크기: {report_path.stat().st_size / 1024:.1f} KB")

    # 차트 목록
    charts = sorted((data_dir / "report" / "charts").glob("*.png"))
    print(f"✅ 차트 {len(charts)}개 생성:")
    for c in charts:
        print(f"   - {c.name}")


if __name__ == "__main__":
    data_dir = create_mock_data()
    run_pipeline(data_dir)
    print(f"\n📂 결과 위치: {data_dir / 'report'}")
