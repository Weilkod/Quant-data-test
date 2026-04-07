# CLAUDE.md — Instagram Channel Analyzer

## Project Overview
CLI pipeline: `python main.py @channel [--no-ai|--ai-text-only|--skip-collect|--no-upload|--with-comments]`
Flow: instagrapi → pandas → Claude API → python-pptx/Jinja2 → Google Drive

## Architecture
- **5 modules, single responsibility each**: collector (scrape only), analyzer (Claude API only), estimator (pure math, no I/O), reporter (PPT/HTML only), drive_uploader (upload only)
- `main.py` orchestrates — no business logic inside it
- Modules communicate via files (`/data/{channel}/raw/`, `analysis/`, `report/`), never direct imports between siblings
- Each pipeline stage checks previous stage's output files exist before running; exit(1) with clear message if missing

## instagrapi Rules (collector.py)
- **Random delay 2–4s between every request** — never skip
- **댓글 수집 기본 비활성화** — `--with-comments` 플래그로 활성화 (Instagram API 제한으로 401 에러 빈발)
- Comment pagination (활성화 시): additional 1–2s delay per page
- Max 200 posts per run; reuse session via `Client.load_settings()` / `Client.login()`
- On 429: wait 60s, retry up to 3x with exponential backoff
- **Individual post/comment failures → skip & log, never abort pipeline**
- Save: `raw/profile.json`, `raw/posts.csv`, `raw/images/{shortcode}.jpg` (댓글 활성화 시 `raw/comments.csv` 추가)

## Claude API Rules (analyzer.py)
**Model assignment (strict):**
| Task | Model |
|------|-------|
| Category classify, caption style, sentiment | `claude-haiku-4-5-20251001` |
| Age estimation, vision, top-post insights, narrative | `claude-sonnet-4-6` |

- All calls go through single `call_claude()` wrapper with retry (2x, 30s wait)
- System prompt must specify JSON output schema — never parse free text with regex
- Batch captions as JSON array in 1 call, not per-post calls
- Prompts live in `prompts/*.txt`, not inline strings
- **Vision: resize to ≤1024px before base64 encoding. Max 20 images per call.**
- **캐러셀 이미지: 게시물당 최대 5장만 수집·분석 (비용 절감)**
- Cache results to `analysis/{task}.json`; skip re-analysis if file exists unless `--force-reanalyze`
- Log estimated token count before each call; warn if total pipeline cost > $5

## Estimator Rules (estimator.py)
- **Pure functions only** — no API calls, no file I/O
- All coefficients (save/share/reach) in `config/coefficients.yaml` with source comments — no magic numbers
- Follower tier mapping must handle boundary values correctly (write edge-case tests)
- Clamp negative likes/comments to 0; filter None/empty captions before analysis

## Reporter Rules (reporter.py)
- Access PPT placeholders by name, not index
- Charts → save as PNG to `report/charts/` first, then insert into PPT
- **Matplotlib Korean font**: always set `NanumGothic` + `axes.unicode_minus = False`
- Empty data → render "데이터 없음" in report section, not an error

## Common Rules
- **Logging**: `logging` module only, never `print()`. File + stderr handlers. Log to `data/{channel}/pipeline.log`
- **Dirs**: call `ensure_dirs(channel)` at pipeline start — create all subdirs with `mkdir(parents=True, exist_ok=True)`
- **Type hints** on all function signatures
- **Channel name**: normalize with `channel.lstrip("@")`
- **Retry policy**: instagrapi 3x exponential (5/15/45s), Claude API 2x (30s fixed), Drive 3x exponential (2/4/8s)

## Forbidden
- instagrapi loop without delay
- Hardcoded API keys or secrets
- `except: pass` or bare except
- Magic numbers for estimation coefficients
- Unresized images to Vision API
- Committing `/data/`, `.env`, `config/credentials.json`, `config/config.yaml` to git
- Regex parsing of Claude API responses

## Progress Tracking
- **코드 변경 커밋 시 `PROGRESS.md`도 반드시 함께 업데이트** — 테스트 개수, 완료 항목, 새 기능 등 반영
- 테스트 추가/삭제 시 해당 모듈의 테스트 개수와 총 개수를 갱신
- 새 기능·버그 수정 완료 시 "완료된 단계" 또는 "진행중" 섹션에 항목 추가

## Config
All runtime config in `config/config.yaml` (gitignored). Provide `config.yaml.example` in repo.
Coefficients in `config/coefficients.yaml` (committed, with source citations).

## Dev Order (follow strictly)
1. collector.py → test with `--no-ai` (zero API cost)
2. estimator.py → unit test coefficients
3. analyzer.py → test one channel end-to-end
4. reporter.py → verify PPT output
5. drive_uploader.py
6. main.py → integrate all flags
7. app.py → Streamlit UI wrapping main.py modules

## Streamlit (app.py)
- Reuse all modules from steps 1-6
- UI: channel input, industry dropdown, checkbox options, progress bar, download buttons
- Wireframe: see INSTA.md D-1

## Industry Presets
- `presets/{industry}.yaml` — category definitions + save/share modifiers + competitor list
- `--industry food|beauty|fashion|auto`
- `auto`: Claude Sonnet analyzes 50 captions → generates category YAML → saves to `presets/auto_{channel}.yaml`
- Preset YAML schema: see INSTA.md B-6
