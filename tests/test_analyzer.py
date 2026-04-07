"""analyzer.py 단위 테스트

API 호출은 mock으로 대체. 순수 로직과 캐싱/프리셋/프롬프트 로딩만 테스트.
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from analyzer import (
    MODEL_HAIKU,
    MODEL_SONNET,
    _estimate_cost,
    _estimate_tokens,
    _load_cache,
    _save_cache,
    load_preset,
    load_prompt,
    resize_and_encode_image,
    reset_cumulative_cost,
)


# ──────────────────────────────────────────────
# 프롬프트 로딩
# ──────────────────────────────────────────────


class TestLoadPrompt:
    def test_load_existing_prompt(self):
        prompt = load_prompt("categorize_captions")
        assert "category" in prompt.lower() or "카테고리" in prompt

    def test_load_all_prompts(self):
        """모든 프롬프트 파일이 로드 가능한지 확인"""
        prompt_names = [
            "categorize_captions",
            "analyze_caption_style",
            "sentiment_analysis",
            "age_estimation",
            "visual_analysis",
            "top_posts_insights",
            "report_narrative",
            "auto_categories",
        ]
        for name in prompt_names:
            prompt = load_prompt(name)
            assert len(prompt) > 50, f"프롬프트 '{name}'이 너무 짧음"

    def test_missing_prompt_raises(self):
        with pytest.raises(FileNotFoundError):
            load_prompt("nonexistent_prompt")


# ──────────────────────────────────────────────
# 프리셋 로딩
# ──────────────────────────────────────────────


class TestLoadPreset:
    def test_load_food_preset(self):
        preset = load_preset("food")
        assert preset is not None
        assert preset["industry"] == "food"
        assert len(preset["categories"]) == 10

    def test_food_preset_categories(self):
        preset = load_preset("food")
        codes = [c["code"] for c in preset["categories"]]
        assert codes == [f"F{i:02d}" for i in range(1, 11)]

    def test_food_preset_has_modifiers(self):
        preset = load_preset("food")
        for cat in preset["categories"]:
            assert "save_modifier" in cat
            assert "share_modifier" in cat
            assert isinstance(cat["save_modifier"], (int, float))
            assert isinstance(cat["share_modifier"], (int, float))

    def test_food_preset_competitors(self):
        preset = load_preset("food")
        assert "competitors" in preset
        assert len(preset["competitors"]) > 0

    def test_auto_returns_none(self):
        result = load_preset("auto")
        assert result is None

    def test_none_returns_none(self):
        result = load_preset(None)
        assert result is None

    def test_unknown_preset_returns_none(self):
        result = load_preset("nonexistent_industry")
        assert result is None

    def test_custom_path(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("industry: test\nname: Test\ncategories: []\n")
            f.flush()
            preset = load_preset(f"custom:{f.name}")
            assert preset["industry"] == "test"
        os.unlink(f.name)

    def test_custom_missing_raises(self):
        with pytest.raises(FileNotFoundError):
            load_preset("custom:/nonexistent/path.yaml")


# ──────────────────────────────────────────────
# 토큰/비용 추정
# ──────────────────────────────────────────────


class TestEstimation:
    def test_estimate_tokens(self):
        tokens = _estimate_tokens("Hello world 안녕하세요")
        assert tokens > 0
        assert isinstance(tokens, int)

    def test_estimate_tokens_empty(self):
        tokens = _estimate_tokens("")
        assert tokens >= 1  # 최소 1

    def test_estimate_cost_haiku(self):
        cost = _estimate_cost(MODEL_HAIKU, 50000, 10000)
        # 50K * $1/1M + 10K * $5/1M = $0.05 + $0.05 = $0.10
        assert cost == pytest.approx(0.10, rel=1e-2)

    def test_estimate_cost_sonnet(self):
        cost = _estimate_cost(MODEL_SONNET, 100000, 10000)
        # 100K * $3/1M + 10K * $15/1M = $0.30 + $0.15 = $0.45
        assert cost == pytest.approx(0.45, rel=1e-2)

    def test_reset_cumulative_cost(self):
        reset_cumulative_cost()
        from analyzer import get_cumulative_cost
        assert get_cumulative_cost() == 0.0


# ──────────────────────────────────────────────
# 캐싱
# ──────────────────────────────────────────────


class TestCaching:
    def test_save_and_load_cache(self, tmp_path):
        data = {"key": "value", "number": 42}
        _save_cache(tmp_path, "test.json", data)
        loaded = _load_cache(tmp_path, "test.json")
        assert loaded == data

    def test_load_missing_cache(self, tmp_path):
        result = _load_cache(tmp_path, "nonexistent.json")
        assert result is None

    def test_cache_unicode(self, tmp_path):
        data = {"한국어": "테스트", "emoji": "🍕"}
        _save_cache(tmp_path, "unicode.json", data)
        loaded = _load_cache(tmp_path, "unicode.json")
        assert loaded == data


# ──────────────────────────────────────────────
# 이미지 리사이즈
# ──────────────────────────────────────────────


class TestImageResize:
    def test_resize_and_encode(self, tmp_path):
        """테스트용 이미지 생성 후 리사이즈/인코딩"""
        from PIL import Image
        import base64

        # 2000x1500 테스트 이미지 생성
        img = Image.new("RGB", (2000, 1500), color="red")
        img_path = tmp_path / "test.jpg"
        img.save(img_path)

        b64 = resize_and_encode_image(img_path, max_size=1024)

        # base64 디코딩 가능한지 확인
        decoded = base64.b64decode(b64)
        assert len(decoded) > 0

        # 리사이즈 확인
        import io
        resized = Image.open(io.BytesIO(decoded))
        assert max(resized.size) <= 1024

    def test_small_image_not_upscaled(self, tmp_path):
        """작은 이미지는 확대하지 않음"""
        from PIL import Image
        import base64, io

        img = Image.new("RGB", (500, 300), color="blue")
        img_path = tmp_path / "small.jpg"
        img.save(img_path)

        b64 = resize_and_encode_image(img_path, max_size=1024)
        decoded = base64.b64decode(b64)
        resized = Image.open(io.BytesIO(decoded))
        assert resized.size[0] <= 500
        assert resized.size[1] <= 300

    def test_rgba_to_rgb(self, tmp_path):
        """RGBA 이미지가 RGB로 변환되는지 확인"""
        from PIL import Image

        img = Image.new("RGBA", (100, 100), color=(255, 0, 0, 128))
        img_path = tmp_path / "rgba.png"
        img.save(img_path)

        b64 = resize_and_encode_image(img_path, max_size=1024)
        assert len(b64) > 0  # 에러 없이 인코딩 성공


# ──────────────────────────────────────────────
# call_claude 모킹 테스트
# ──────────────────────────────────────────────


class TestCallClaude:
    @patch("analyzer._create_client")
    def test_call_claude_success(self, mock_create):
        from analyzer import call_claude

        reset_cumulative_cost()

        # Mock 응답 구성
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='{"result": "success"}')]
        mock_response.usage = MagicMock(input_tokens=100, output_tokens=50)

        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response
        mock_create.return_value = mock_client

        result = call_claude(
            model=MODEL_HAIKU,
            system_prompt="Test",
            user_content="Test input",
        )

        assert result == {"result": "success"}
        mock_client.messages.create.assert_called_once()

    @patch("analyzer._create_client")
    def test_call_claude_json_in_code_block(self, mock_create):
        """```json ... ``` 감싸진 응답 처리"""
        from analyzer import call_claude

        reset_cumulative_cost()

        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='```json\n{"key": "value"}\n```')]
        mock_response.usage = MagicMock(input_tokens=100, output_tokens=50)

        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response
        mock_create.return_value = mock_client

        result = call_claude(MODEL_HAIKU, "Test", "Test input")
        assert result == {"key": "value"}

    @patch("analyzer._create_client")
    def test_call_claude_retry_on_error(self, mock_create):
        """API 오류 시 재시도 확인"""
        from analyzer import call_claude

        reset_cumulative_cost()

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = [
            Exception("API Error"),
            MagicMock(
                content=[MagicMock(text='{"retried": true}')],
                usage=MagicMock(input_tokens=100, output_tokens=50),
            ),
        ]
        mock_create.return_value = mock_client

        result = call_claude(
            MODEL_HAIKU, "Test", "Test input", wait_time=0
        )
        assert result == {"retried": True}
        assert mock_client.messages.create.call_count == 2

    @patch("analyzer._create_client")
    def test_call_claude_all_retries_fail(self, mock_create):
        """모든 재시도 실패 시 RuntimeError"""
        from analyzer import call_claude

        reset_cumulative_cost()

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("Persistent Error")
        mock_create.return_value = mock_client

        with pytest.raises(RuntimeError, match="Claude API 호출 실패"):
            call_claude(MODEL_HAIKU, "Test", "Test input", wait_time=0)


# ──────────────────────────────────────────────
# 개별 분석 태스크 (mock API)
# ──────────────────────────────────────────────


class TestAnalyzeCategories:
    @patch("analyzer.call_claude")
    def test_categories_with_preset(self, mock_call, tmp_path):
        from analyzer import analyze_categories

        mock_result = {
            "classifications": [
                {"shortcode": "AAA", "category_code": "F01", "confidence": 0.9, "reason": "테스트"},
            ]
        }
        mock_call.return_value = mock_result

        posts_df = pd.DataFrame({
            "shortcode": ["AAA"],
            "caption": ["맛집 추천 TOP 10"],
        })
        preset = load_preset("food")

        result = analyze_categories(posts_df, preset, tmp_path, force=True)
        assert "classifications" in result
        assert result["classifications"][0]["category_code"] == "F01"
        mock_call.assert_called_once()

    @patch("analyzer.call_claude")
    def test_categories_cache_hit(self, mock_call, tmp_path):
        """캐시가 있으면 API 호출하지 않음"""
        from analyzer import analyze_categories

        cached = {"classifications": [{"shortcode": "BBB", "category_code": "F02"}]}
        _save_cache(tmp_path, "categories.json", cached)

        posts_df = pd.DataFrame({"shortcode": ["BBB"], "caption": ["test"]})
        result = analyze_categories(posts_df, None, tmp_path, force=False)

        assert result == cached
        mock_call.assert_not_called()


class TestAnalyzeCaptionStyle:
    @patch("analyzer.call_claude")
    def test_caption_style(self, mock_call, tmp_path):
        from analyzer import analyze_caption_style

        mock_call.return_value = {
            "tone": "casual",
            "avg_length_chars": 200,
            "summary": "테스트 요약",
        }

        posts_df = pd.DataFrame({
            "shortcode": ["A", "B"],
            "caption": ["캡션1 🍕", "캡션2 맛집 추천"],
        })

        result = analyze_caption_style(posts_df, tmp_path, force=True)
        assert result["tone"] == "casual"


class TestAnalyzeSentiment:
    @patch("analyzer.call_claude")
    def test_sentiment_with_comments(self, mock_call, tmp_path):
        from analyzer import analyze_sentiment

        mock_call.return_value = {
            "overall": {"positive_pct": 70, "neutral_pct": 20, "negative_pct": 10},
        }

        posts_df = pd.DataFrame({
            "shortcode": ["A", "B"],
            "likes": [100, 50],
        })
        comments_df = pd.DataFrame({
            "shortcode": ["A", "A", "B"],
            "text": ["맛있다!", "별로..", "좋아요"],
        })

        result = analyze_sentiment(posts_df, comments_df, tmp_path, force=True)
        assert "overall" in result


class TestAnalyzeVisual:
    @patch("analyzer.call_claude")
    def test_visual_no_images(self, mock_call, tmp_path):
        """이미지 없으면 건너뜀"""
        from analyzer import analyze_visual

        data_dir = tmp_path / "data"
        (data_dir / "raw" / "images").mkdir(parents=True)
        analysis_dir = tmp_path / "analysis"
        analysis_dir.mkdir()

        posts_df = pd.DataFrame({
            "shortcode": ["AAA"],
            "likes": [100],
        })

        result = analyze_visual(data_dir, posts_df, analysis_dir, force=True)
        assert result.get("skipped") is True
        mock_call.assert_not_called()


class TestRunAnalysis:
    @patch("analyzer.call_claude")
    def test_run_analysis_no_comments(self, mock_call, tmp_path):
        """댓글 없는 경우 감성/연령대 분석 건너뜀"""
        from analyzer import run_analysis

        # 데이터 구조 생성
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "images").mkdir()
        (tmp_path / "analysis").mkdir()

        profile = {"username": "test", "followers": 10000, "biography": "테스트"}
        with open(raw_dir / "profile.json", "w", encoding="utf-8") as f:
            json.dump(profile, f)

        posts_df = pd.DataFrame({
            "shortcode": ["A", "B"],
            "caption": ["캡션1", "캡션2"],
            "likes": [100, 200],
            "comments": [5, 10],
            "typename": ["GraphImage", "GraphVideo"],
            "date_utc": ["2025-01-01", "2025-01-02"],
            "caption_hashtags": ["#맛집", "#추천"],
        })
        posts_df.to_csv(raw_dir / "posts.csv", index=False)

        # Mock: 각 호출마다 다른 결과 반환
        mock_call.side_effect = [
            {"classifications": []},  # categories
            {"tone": "casual"},  # caption_style
            # sentiment & age skipped (no comments)
            # visual skipped (no images → handled internally)
            {"common_patterns": {}},  # top_posts
            {"executive_summary": "테스트"},  # narrative
        ]

        results = run_analysis("test", tmp_path, text_only=True, force=True)

        assert results["sentiment"] is None
        assert results["audience_age"] is None
        assert results["visual"] is None  # text_only=True


# ──────────────────────────────────────────────
# 모델 배정 확인
# ──────────────────────────────────────────────


class TestModelAssignment:
    """CLAUDE.md 모델 배정 규칙 검증"""

    def test_haiku_model_id(self):
        assert MODEL_HAIKU == "claude-haiku-4-5-20251001"

    def test_sonnet_model_id(self):
        assert MODEL_SONNET == "claude-sonnet-4-6"

    @patch("analyzer.call_claude")
    def test_categories_uses_haiku(self, mock_call, tmp_path):
        from analyzer import analyze_categories

        mock_call.return_value = {"classifications": []}
        posts_df = pd.DataFrame({"shortcode": ["A"], "caption": ["test"]})
        analyze_categories(posts_df, None, tmp_path, force=True)

        assert mock_call.call_args.kwargs.get("model") == MODEL_HAIKU

    @patch("analyzer.call_claude")
    def test_caption_style_uses_haiku(self, mock_call, tmp_path):
        from analyzer import analyze_caption_style

        mock_call.return_value = {"tone": "casual"}
        posts_df = pd.DataFrame({"shortcode": ["A"], "caption": ["test"]})
        analyze_caption_style(posts_df, tmp_path, force=True)

        assert mock_call.call_args.kwargs.get("model") == MODEL_HAIKU

    @patch("analyzer.call_claude")
    def test_top_posts_uses_sonnet(self, mock_call, tmp_path):
        from analyzer import analyze_top_posts

        mock_call.return_value = {"common_patterns": {}}
        posts_df = pd.DataFrame({
            "shortcode": ["A"] * 20,
            "likes": list(range(20)),
            "comments": [1] * 20,
            "typename": ["GraphImage"] * 20,
            "date_utc": ["2025-01-01"] * 20,
            "caption": ["test"] * 20,
            "caption_hashtags": [""] * 20,
        })
        analyze_top_posts(posts_df, None, tmp_path, force=True)

        assert mock_call.call_args.kwargs.get("model") == MODEL_SONNET
