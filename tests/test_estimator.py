"""estimator.py 단위 테스트

CLAUDE.md 요구사항:
    - unit test coefficients
    - Follower tier mapping must handle boundary values correctly
"""

import pytest
import pandas as pd

from estimator import (
    _format_key,
    _get_follower_tier,
    aggregate_by_format,
    enrich_posts,
    estimate_engagement_rate,
    estimate_full_engagement_rate,
    estimate_saves,
    estimate_shares,
    estimate_views,
    load_coefficients,
)


@pytest.fixture
def coeffs():
    """실제 coefficients.yaml 로드"""
    return load_coefficients()


# ──────────────────────────────────────────────
# _format_key 매핑
# ──────────────────────────────────────────────


class TestFormatKey:
    def test_graph_image(self):
        assert _format_key("GraphImage") == "image"

    def test_graph_video(self):
        assert _format_key("GraphVideo") == "reels"

    def test_graph_sidecar(self):
        assert _format_key("GraphSidecar") == "carousel"

    def test_unknown_falls_back_to_image(self):
        assert _format_key("Unknown") == "image"

    def test_empty_string_falls_back_to_image(self):
        assert _format_key("") == "image"


# ──────────────────────────────────────────────
# _get_follower_tier 경계값 테스트
# ──────────────────────────────────────────────


class TestFollowerTier:
    def test_below_first_tier(self, coeffs):
        tiers = coeffs["views_by_tier"]
        tier = _get_follower_tier(100, tiers)
        assert tier["max_followers"] == 5000

    def test_exact_boundary_5000(self, coeffs):
        tiers = coeffs["views_by_tier"]
        tier = _get_follower_tier(5000, tiers)
        assert tier["max_followers"] == 5000

    def test_just_above_5000(self, coeffs):
        tiers = coeffs["views_by_tier"]
        tier = _get_follower_tier(5001, tiers)
        assert tier["max_followers"] == 10000

    def test_exact_boundary_10000(self, coeffs):
        tiers = coeffs["views_by_tier"]
        tier = _get_follower_tier(10000, tiers)
        assert tier["max_followers"] == 10000

    def test_exact_boundary_50000(self, coeffs):
        tiers = coeffs["views_by_tier"]
        tier = _get_follower_tier(50000, tiers)
        assert tier["max_followers"] == 50000

    def test_exact_boundary_100000(self, coeffs):
        tiers = coeffs["views_by_tier"]
        tier = _get_follower_tier(100000, tiers)
        assert tier["max_followers"] == 100000

    def test_exact_boundary_1000000(self, coeffs):
        tiers = coeffs["views_by_tier"]
        tier = _get_follower_tier(1000000, tiers)
        assert tier["max_followers"] == 1000000

    def test_over_1m_uses_last_tier(self, coeffs):
        tiers = coeffs["views_by_tier"]
        tier = _get_follower_tier(2000000, tiers)
        assert tier["max_followers"] == 1000000  # 마지막 tier 반환

    def test_zero_followers(self, coeffs):
        tiers = coeffs["views_by_tier"]
        tier = _get_follower_tier(0, tiers)
        assert tier["max_followers"] == 5000


# ──────────────────────────────────────────────
# estimate_saves
# ──────────────────────────────────────────────


class TestEstimateSaves:
    def test_reels(self, coeffs):
        result = estimate_saves(1000, "GraphVideo", coeffs)
        assert result == pytest.approx(1000 * 0.082, rel=1e-3)

    def test_carousel(self, coeffs):
        result = estimate_saves(1000, "GraphSidecar", coeffs)
        assert result == pytest.approx(1000 * 0.055, rel=1e-3)

    def test_image(self, coeffs):
        result = estimate_saves(1000, "GraphImage", coeffs)
        assert result == pytest.approx(1000 * 0.028, rel=1e-3)

    def test_educational_modifier(self, coeffs):
        result = estimate_saves(1000, "GraphVideo", coeffs, category="educational")
        assert result == pytest.approx(1000 * 0.082 * 1.24, rel=1e-3)

    def test_sponsored_modifier(self, coeffs):
        result = estimate_saves(1000, "GraphImage", coeffs, category="sponsored")
        assert result == pytest.approx(1000 * 0.028 * 0.70, rel=1e-3)

    def test_negative_likes_clamped(self, coeffs):
        result = estimate_saves(-100, "GraphImage", coeffs)
        assert result == 0.0

    def test_zero_likes(self, coeffs):
        result = estimate_saves(0, "GraphVideo", coeffs)
        assert result == 0.0


# ──────────────────────────────────────────────
# estimate_shares
# ──────────────────────────────────────────────


class TestEstimateShares:
    def test_reels(self, coeffs):
        result = estimate_shares(1000, "GraphVideo", coeffs)
        assert result == pytest.approx(1000 * 0.189, rel=1e-3)

    def test_carousel(self, coeffs):
        result = estimate_shares(1000, "GraphSidecar", coeffs)
        assert result == pytest.approx(1000 * 0.08, rel=1e-3)

    def test_image(self, coeffs):
        result = estimate_shares(1000, "GraphImage", coeffs)
        assert result == pytest.approx(1000 * 0.05, rel=1e-3)

    def test_entertainment_modifier(self, coeffs):
        result = estimate_shares(1000, "GraphVideo", coeffs, category="entertainment")
        assert result == pytest.approx(1000 * 0.189 * 1.12, rel=1e-3)

    def test_negative_likes_clamped(self, coeffs):
        result = estimate_shares(-50, "GraphVideo", coeffs)
        assert result == 0.0


# ──────────────────────────────────────────────
# estimate_views
# ──────────────────────────────────────────────


class TestEstimateViews:
    def test_small_account_reels(self, coeffs):
        result = estimate_views(3000, "GraphVideo", coeffs)
        assert result == 580.0

    def test_small_account_carousel(self, coeffs):
        result = estimate_views(3000, "GraphSidecar", coeffs)
        assert result == 993.0

    def test_medium_account(self, coeffs):
        result = estimate_views(30000, "GraphImage", coeffs)
        assert result == 2340.0

    def test_large_account(self, coeffs):
        result = estimate_views(500000, "GraphVideo", coeffs)
        assert result == 16035.0

    def test_over_1m(self, coeffs):
        result = estimate_views(2000000, "GraphSidecar", coeffs)
        assert result == 35370.0  # 마지막 tier 값


# ──────────────────────────────────────────────
# estimate_engagement_rate
# ──────────────────────────────────────────────


class TestEngagementRate:
    def test_normal(self):
        result = estimate_engagement_rate(100, 10, 10000)
        assert result == pytest.approx(1.1, rel=1e-3)

    def test_zero_followers(self):
        assert estimate_engagement_rate(100, 10, 0) == 0.0

    def test_negative_followers(self):
        assert estimate_engagement_rate(100, 10, -5) == 0.0

    def test_negative_likes_clamped(self):
        result = estimate_engagement_rate(-10, 5, 1000)
        assert result == pytest.approx(0.5, rel=1e-3)

    def test_negative_comments_clamped(self):
        result = estimate_engagement_rate(10, -5, 1000)
        assert result == pytest.approx(1.0, rel=1e-3)


# ──────────────────────────────────────────────
# estimate_full_engagement_rate
# ──────────────────────────────────────────────


class TestFullEngagementRate:
    def test_normal(self):
        result = estimate_full_engagement_rate(100, 10, 8.0, 19.0, 2460.0)
        expected = (100 + 10 + 8.0 + 19.0) / 2460.0 * 100
        assert result == pytest.approx(expected, rel=1e-3)

    def test_zero_views(self):
        assert estimate_full_engagement_rate(100, 10, 8.0, 19.0, 0.0) == 0.0

    def test_negative_views(self):
        assert estimate_full_engagement_rate(100, 10, 8.0, 19.0, -100.0) == 0.0


# ──────────────────────────────────────────────
# enrich_posts (DataFrame 통합)
# ──────────────────────────────────────────────


class TestEnrichPosts:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            "shortcode": ["AAA", "BBB", "CCC"],
            "likes": [500, 1000, 200],
            "comments": [10, 50, 5],
            "typename": ["GraphImage", "GraphVideo", "GraphSidecar"],
        })

    def test_adds_five_columns(self, sample_df, coeffs):
        result = enrich_posts(sample_df, 30000, coeffs)
        expected_cols = ["est_saves", "est_shares", "est_views", "est_eng_rate", "est_full_eng_rate"]
        for col in expected_cols:
            assert col in result.columns

    def test_does_not_modify_original(self, sample_df, coeffs):
        original_cols = list(sample_df.columns)
        enrich_posts(sample_df, 30000, coeffs)
        assert list(sample_df.columns) == original_cols

    def test_correct_saves_for_image(self, sample_df, coeffs):
        result = enrich_posts(sample_df, 30000, coeffs)
        expected = round(500 * 0.028, 1)
        assert result.iloc[0]["est_saves"] == expected

    def test_correct_views_for_tier(self, sample_df, coeffs):
        result = enrich_posts(sample_df, 30000, coeffs)
        # 30K followers → 10~50K tier → image=2340
        assert result.iloc[0]["est_views"] == 2340


# ──────────────────────────────────────────────
# aggregate_by_format
# ──────────────────────────────────────────────


class TestAggregateByFormat:
    def test_counts_and_ratios(self, coeffs):
        df = pd.DataFrame({
            "shortcode": ["A", "B", "C", "D"],
            "likes": [100, 200, 300, 400],
            "comments": [5, 10, 15, 20],
            "typename": ["GraphImage", "GraphImage", "GraphVideo", "GraphSidecar"],
        })
        enriched = enrich_posts(df, 10000, coeffs)
        agg = aggregate_by_format(enriched)

        assert len(agg) == 3  # 3 formats
        image_row = agg[agg["typename"] == "GraphImage"].iloc[0]
        assert image_row["count"] == 2
        assert image_row["ratio_pct"] == 50.0

    def test_empty_dataframe(self, coeffs):
        df = pd.DataFrame({
            "shortcode": [],
            "likes": [],
            "comments": [],
            "typename": [],
        })
        enriched = enrich_posts(df, 10000, coeffs)
        agg = aggregate_by_format(enriched)
        assert len(agg) == 0
