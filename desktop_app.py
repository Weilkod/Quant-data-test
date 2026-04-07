"""
인스타그램 채널 분석기 — CustomTkinter 데스크톱 앱

CLAUDE.md 규칙:
    - steps 1-6 모듈 재사용 (collector, estimator, analyzer, reporter, drive_uploader)
    - UI: 채널 입력, 업종 드롭다운, 체크박스 옵션, 진행 바, 다운로드 버튼
    - Wireframe: INSTA.md D-1

실행: python desktop_app.py
"""

import json
import os
import platform
import queue
import shutil
import subprocess
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox

import customtkinter as ctk
from PIL import Image

from desktop_worker import PipelineWorker, apply_credentials

# ──────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────
INDUSTRY_OPTIONS: dict[str, str | None] = {
    "선택 안 함": None,
    "푸드": "food",
    "뷰티": "beauty",
    "패션": "fashion",
    "자동 감지": "auto",
}

WINDOW_WIDTH = 1100
WINDOW_HEIGHT = 720
SIDEBAR_WIDTH = 280
FONT_FAMILY = "NanumGothic"
COLOR_PRIMARY = "#1B3A5C"
COLOR_ACCENT = "#E85D26"


# ──────────────────────────────────────────────
# 메인 앱
# ──────────────────────────────────────────────
class DesktopApp(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()

        self.title("인스타그램 채널 분석기")
        self.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")
        self.minsize(900, 600)

        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")

        self.progress_queue: queue.Queue = queue.Queue()
        self.worker: PipelineWorker | None = None
        self.report_path: Path | None = None

        # 레이아웃: 사이드바 + 메인
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self._build_sidebar()
        self._build_main_area()
        self._show_existing_channels()

    # ──────────────────────────────────────────
    # 사이드바 (왼쪽 패널)
    # ──────────────────────────────────────────
    def _build_sidebar(self) -> None:
        sidebar = ctk.CTkScrollableFrame(self, width=SIDEBAR_WIDTH, corner_radius=0)
        sidebar.grid(row=0, column=0, sticky="nsew")
        sidebar.grid_columnconfigure(0, weight=1)

        row = 0

        # 제목
        ctk.CTkLabel(
            sidebar, text="분석 설정", font=ctk.CTkFont(family=FONT_FAMILY, size=18, weight="bold"),
        ).grid(row=row, column=0, padx=16, pady=(16, 8), sticky="w")
        row += 1

        # 채널명
        ctk.CTkLabel(sidebar, text="채널명", font=ctk.CTkFont(family=FONT_FAMILY, size=13)).grid(
            row=row, column=0, padx=16, pady=(8, 2), sticky="w",
        )
        row += 1
        self.channel_entry = ctk.CTkEntry(sidebar, placeholder_text="@omuk_food")
        self.channel_entry.grid(row=row, column=0, padx=16, pady=(0, 8), sticky="ew")
        row += 1

        # 업종
        ctk.CTkLabel(sidebar, text="업종", font=ctk.CTkFont(family=FONT_FAMILY, size=13)).grid(
            row=row, column=0, padx=16, pady=(8, 2), sticky="w",
        )
        row += 1
        self.industry_var = ctk.StringVar(value="선택 안 함")
        self.industry_menu = ctk.CTkOptionMenu(
            sidebar, variable=self.industry_var, values=list(INDUSTRY_OPTIONS.keys()),
        )
        self.industry_menu.grid(row=row, column=0, padx=16, pady=(0, 8), sticky="ew")
        row += 1

        # API 설정
        ctk.CTkLabel(
            sidebar, text="API 설정", font=ctk.CTkFont(family=FONT_FAMILY, size=15, weight="bold"),
        ).grid(row=row, column=0, padx=16, pady=(16, 4), sticky="w")
        row += 1

        ctk.CTkLabel(sidebar, text="Anthropic API Key", font=ctk.CTkFont(family=FONT_FAMILY, size=12)).grid(
            row=row, column=0, padx=16, pady=(4, 2), sticky="w",
        )
        row += 1
        self.api_key_entry = ctk.CTkEntry(sidebar, show="*")
        self.api_key_entry.grid(row=row, column=0, padx=16, pady=(0, 4), sticky="ew")
        row += 1

        ctk.CTkLabel(sidebar, text="Instagram ID", font=ctk.CTkFont(family=FONT_FAMILY, size=12)).grid(
            row=row, column=0, padx=16, pady=(4, 2), sticky="w",
        )
        row += 1
        self.ig_user_entry = ctk.CTkEntry(sidebar)
        self.ig_user_entry.grid(row=row, column=0, padx=16, pady=(0, 4), sticky="ew")
        row += 1

        ctk.CTkLabel(sidebar, text="Instagram Password", font=ctk.CTkFont(family=FONT_FAMILY, size=12)).grid(
            row=row, column=0, padx=16, pady=(4, 2), sticky="w",
        )
        row += 1
        self.ig_pass_entry = ctk.CTkEntry(sidebar, show="*")
        self.ig_pass_entry.grid(row=row, column=0, padx=16, pady=(0, 4), sticky="ew")
        row += 1

        self.cred_button = ctk.CTkButton(
            sidebar, text="입력 완료", command=self._on_save_credentials,
            fg_color="#2ECC71", hover_color="#27AE60",
        )
        self.cred_button.grid(row=row, column=0, padx=16, pady=(4, 8), sticky="ew")
        row += 1

        self.cred_status_label = ctk.CTkLabel(
            sidebar, text="", font=ctk.CTkFont(family=FONT_FAMILY, size=11),
            text_color="#888888",
        )
        self.cred_status_label.grid(row=row, column=0, padx=16, pady=(0, 8), sticky="w")
        row += 1

        # 옵션
        ctk.CTkLabel(
            sidebar, text="옵션", font=ctk.CTkFont(family=FONT_FAMILY, size=15, weight="bold"),
        ).grid(row=row, column=0, padx=16, pady=(12, 4), sticky="w")
        row += 1

        self.use_ai_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(sidebar, text="AI 분석 포함", variable=self.use_ai_var,
                         command=self._on_ai_toggle).grid(
            row=row, column=0, padx=16, pady=2, sticky="w",
        )
        row += 1

        self.use_vision_var = ctk.BooleanVar(value=True)
        self.vision_cb = ctk.CTkCheckBox(sidebar, text="Vision (이미지 분석)", variable=self.use_vision_var)
        self.vision_cb.grid(row=row, column=0, padx=16, pady=2, sticky="w")
        row += 1

        self.with_comments_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(sidebar, text="댓글 수집", variable=self.with_comments_var).grid(
            row=row, column=0, padx=16, pady=2, sticky="w",
        )
        row += 1

        self.upload_drive_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(sidebar, text="Google Drive 업로드", variable=self.upload_drive_var).grid(
            row=row, column=0, padx=16, pady=2, sticky="w",
        )
        row += 1

        self.force_reanalyze_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(sidebar, text="분석 캐시 무시", variable=self.force_reanalyze_var).grid(
            row=row, column=0, padx=16, pady=(2, 12), sticky="w",
        )
        row += 1

        # 시작 버튼
        self.start_button = ctk.CTkButton(
            sidebar, text="분석 시작", command=self._on_start,
            font=ctk.CTkFont(family=FONT_FAMILY, size=15, weight="bold"),
            fg_color=COLOR_ACCENT, hover_color="#C94E1F", height=42,
        )
        self.start_button.grid(row=row, column=0, padx=16, pady=(4, 16), sticky="ew")

    # ──────────────────────────────────────────
    # 메인 영역 (오른쪽 패널)
    # ──────────────────────────────────────────
    def _build_main_area(self) -> None:
        main = ctk.CTkFrame(self, corner_radius=0, fg_color="transparent")
        main.grid(row=0, column=1, sticky="nsew", padx=0, pady=0)
        main.grid_columnconfigure(0, weight=1)
        main.grid_rowconfigure(2, weight=1)  # 로그 영역이 확장됨

        # 제목
        ctk.CTkLabel(
            main, text="인스타그램 채널 분석기",
            font=ctk.CTkFont(family=FONT_FAMILY, size=22, weight="bold"),
            text_color=COLOR_PRIMARY,
        ).grid(row=0, column=0, padx=20, pady=(16, 4), sticky="w")

        ctk.CTkLabel(
            main, text="공개 데이터 기반 경쟁 채널 분석 보고서 자동 생성",
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            text_color="#888888",
        ).grid(row=1, column=0, padx=20, pady=(0, 12), sticky="w")

        # 프로그레스 바
        self.progress_frame = ctk.CTkFrame(main, fg_color="transparent")
        self.progress_frame.grid(row=2, column=0, padx=20, pady=(0, 4), sticky="new")
        self.progress_frame.grid_columnconfigure(0, weight=1)

        self.progress_bar = ctk.CTkProgressBar(self.progress_frame, height=16)
        self.progress_bar.grid(row=0, column=0, sticky="ew", pady=(0, 2))
        self.progress_bar.set(0)

        self.progress_label = ctk.CTkLabel(
            self.progress_frame, text="대기 중",
            font=ctk.CTkFont(family=FONT_FAMILY, size=11), text_color="#666666",
        )
        self.progress_label.grid(row=1, column=0, sticky="w")

        # 로그 텍스트박스
        self.log_box = ctk.CTkTextbox(
            main, font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            state="disabled", height=200,
        )
        self.log_box.grid(row=3, column=0, padx=20, pady=(8, 8), sticky="nsew")
        main.grid_rowconfigure(3, weight=1)

        # 결과 버튼 프레임
        self.result_frame = ctk.CTkFrame(main, fg_color="transparent")
        self.result_frame.grid(row=4, column=0, padx=20, pady=(4, 4), sticky="ew")

        self.open_btn = ctk.CTkButton(
            self.result_frame, text="보고서 열기", command=self._open_report,
            fg_color=COLOR_PRIMARY, width=140,
        )
        self.save_btn = ctk.CTkButton(
            self.result_frame, text="다른 이름으로 저장", command=self._save_report_as,
            fg_color="#555555", width=160,
        )
        # 버튼은 결과가 나온 후에만 표시
        self.open_btn.grid(row=0, column=0, padx=(0, 8), pady=4)
        self.save_btn.grid(row=0, column=1, padx=(0, 8), pady=4)
        self.open_btn.grid_remove()
        self.save_btn.grid_remove()

        # 차트 미리보기 프레임
        self.charts_frame = ctk.CTkFrame(main, fg_color="transparent")
        self.charts_frame.grid(row=5, column=0, padx=20, pady=(4, 4), sticky="ew")

        # 기존 분석 결과 프레임
        self.existing_frame = ctk.CTkFrame(main, fg_color="transparent")
        self.existing_frame.grid(row=6, column=0, padx=20, pady=(8, 16), sticky="ew")

    # ──────────────────────────────────────────
    # 이벤트 핸들러
    # ──────────────────────────────────────────
    def _on_ai_toggle(self) -> None:
        """AI 분석 비활성화 시 Vision도 비활성화"""
        if not self.use_ai_var.get():
            self.use_vision_var.set(False)
            self.vision_cb.configure(state="disabled")
        else:
            self.vision_cb.configure(state="normal")

    def _on_save_credentials(self) -> None:
        api_key = self.api_key_entry.get().strip()
        ig_user = self.ig_user_entry.get().strip()
        ig_pass = self.ig_pass_entry.get().strip()

        if not api_key and not (ig_user and ig_pass):
            messagebox.showwarning("입력 필요", "API Key 또는 Instagram 계정 정보를 입력해주세요.")
            return

        apply_credentials(api_key, ig_user, ig_pass)
        self.cred_status_label.configure(text="자격증명 저장 완료", text_color="#2ECC71")

    def _on_start(self) -> None:
        channel_raw = self.channel_entry.get().strip()
        if not channel_raw:
            messagebox.showwarning("입력 필요", "채널명을 입력해주세요.")
            return

        channel = channel_raw.lstrip("@").strip()
        if not channel:
            messagebox.showwarning("입력 오류", "유효한 채널명을 입력해주세요.")
            return

        # 자격증명 적용
        apply_credentials(
            self.api_key_entry.get().strip(),
            self.ig_user_entry.get().strip(),
            self.ig_pass_entry.get().strip(),
        )

        industry_label = self.industry_var.get()
        industry = INDUSTRY_OPTIONS.get(industry_label)

        config = {
            "channel": channel,
            "industry": industry,
            "use_ai": self.use_ai_var.get(),
            "use_vision": self.use_vision_var.get() if self.use_ai_var.get() else False,
            "with_comments": self.with_comments_var.get(),
            "upload_drive": self.upload_drive_var.get(),
            "force_reanalyze": self.force_reanalyze_var.get(),
        }

        # UI 초기화
        self.start_button.configure(state="disabled", text="분석 중...")
        self.progress_bar.set(0)
        self.progress_label.configure(text="파이프라인 시작...")
        self._clear_log()
        self._append_log(f"@{channel} 분석을 시작합니다.\n")
        self.open_btn.grid_remove()
        self.save_btn.grid_remove()
        self._clear_charts()
        self.report_path = None

        # 워커 시작
        self.progress_queue = queue.Queue()
        self.worker = PipelineWorker(config, self.progress_queue)
        self.worker.start()
        self.after(100, self._poll_progress)

    def _poll_progress(self) -> None:
        """큐에서 진행 메시지를 읽어 UI 업데이트"""
        try:
            while True:
                stage, percent, message = self.progress_queue.get_nowait()

                if stage == "done":
                    self.progress_bar.set(1.0)
                    self.progress_label.configure(text="완료!")
                    self._append_log("파이프라인 완료!\n")

                    if message:  # report_path
                        self.report_path = Path(message)
                        self.open_btn.grid()
                        self.save_btn.grid()
                        self._show_charts(self.report_path.parent / "charts")

                    self.start_button.configure(state="normal", text="분석 시작")
                    self._show_existing_channels()
                    return

                elif stage == "error":
                    self.progress_label.configure(text="오류 발생")
                    self._append_log(f"[오류] {message}\n")
                    self.start_button.configure(state="normal", text="분석 시작")
                    return

                else:
                    if percent >= 0:
                        self.progress_bar.set(percent / 100.0)
                    self.progress_label.configure(text=message)
                    self._append_log(f"{message}\n")

        except queue.Empty:
            pass

        # 워커가 아직 동작 중이면 다시 폴링
        if self.worker and self.worker.is_alive():
            self.after(100, self._poll_progress)
        else:
            # 워커가 죽었는데 done/error 메시지 없이 종료된 경우
            self.start_button.configure(state="normal", text="분석 시작")

    # ──────────────────────────────────────────
    # 로그 유틸
    # ──────────────────────────────────────────
    def _append_log(self, text: str) -> None:
        self.log_box.configure(state="normal")
        self.log_box.insert("end", text)
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _clear_log(self) -> None:
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")

    # ──────────────────────────────────────────
    # 결과 액션
    # ──────────────────────────────────────────
    def _open_report(self) -> None:
        if not self.report_path or not self.report_path.exists():
            messagebox.showinfo("알림", "보고서 파일을 찾을 수 없습니다.")
            return

        system = platform.system()
        try:
            if system == "Darwin":
                subprocess.Popen(["open", str(self.report_path)])
            elif system == "Windows":
                os.startfile(str(self.report_path))
            else:
                subprocess.Popen(["xdg-open", str(self.report_path)])
        except Exception as e:
            messagebox.showerror("오류", f"파일을 열 수 없습니다: {e}")

    def _save_report_as(self) -> None:
        if not self.report_path or not self.report_path.exists():
            messagebox.showinfo("알림", "보고서 파일을 찾을 수 없습니다.")
            return

        dest = filedialog.asksaveasfilename(
            defaultextension=".pptx",
            filetypes=[("PowerPoint", "*.pptx"), ("모든 파일", "*.*")],
            initialfile=self.report_path.name,
        )
        if dest:
            shutil.copy2(self.report_path, dest)
            messagebox.showinfo("저장 완료", f"보고서가 저장되었습니다:\n{dest}")

    # ──────────────────────────────────────────
    # 차트 미리보기
    # ──────────────────────────────────────────
    def _clear_charts(self) -> None:
        for widget in self.charts_frame.winfo_children():
            widget.destroy()

    def _show_charts(self, charts_dir: Path) -> None:
        self._clear_charts()
        if not charts_dir.exists():
            return

        chart_files = sorted(charts_dir.glob("*.png"))
        if not chart_files:
            return

        ctk.CTkLabel(
            self.charts_frame, text="생성된 차트",
            font=ctk.CTkFont(family=FONT_FAMILY, size=14, weight="bold"),
        ).grid(row=0, column=0, columnspan=3, padx=0, pady=(4, 4), sticky="w")

        max_display = 6
        thumb_size = (240, 160)

        for i, chart_path in enumerate(chart_files[:max_display]):
            try:
                pil_img = Image.open(chart_path)
                pil_img.thumbnail(thumb_size, Image.LANCZOS)
                ctk_img = ctk.CTkImage(light_image=pil_img, dark_image=pil_img,
                                        size=pil_img.size)
                col = i % 3
                row = 1 + (i // 3)
                label = ctk.CTkLabel(self.charts_frame, image=ctk_img, text=chart_path.stem,
                                      compound="top", font=ctk.CTkFont(size=10))
                label.grid(row=row, column=col, padx=4, pady=4)
            except Exception:
                pass

    # ──────────────────────────────────────────
    # 기존 분석 결과 표시
    # ──────────────────────────────────────────
    def _show_existing_channels(self) -> None:
        for widget in self.existing_frame.winfo_children():
            widget.destroy()

        data_dir = Path("data")
        if not data_dir.exists():
            return

        channels = []
        for d in sorted(data_dir.iterdir()):
            profile_path = d / "raw" / "profile.json"
            if d.is_dir() and profile_path.exists():
                try:
                    with open(profile_path, encoding="utf-8") as f:
                        profile = json.load(f)
                    channels.append((d.name, profile))
                except Exception:
                    continue

        if not channels:
            return

        ctk.CTkLabel(
            self.existing_frame, text="기존 분석 결과",
            font=ctk.CTkFont(family=FONT_FAMILY, size=14, weight="bold"),
        ).grid(row=0, column=0, columnspan=3, padx=0, pady=(8, 4), sticky="w")

        for i, (ch_name, profile) in enumerate(channels):
            followers = profile.get("followers", 0)
            report_exists = (data_dir / ch_name / "report" / "report.pptx").exists()
            status_text = "보고서 있음" if report_exists else "보고서 없음"
            status_color = "#2ECC71" if report_exists else "#999999"

            row = i + 1
            ctk.CTkLabel(
                self.existing_frame,
                text=f"@{ch_name}  —  팔로워 {followers:,}명",
                font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            ).grid(row=row, column=0, padx=(0, 12), pady=2, sticky="w")

            ctk.CTkLabel(
                self.existing_frame, text=status_text,
                font=ctk.CTkFont(family=FONT_FAMILY, size=11), text_color=status_color,
            ).grid(row=row, column=1, padx=(0, 8), pady=2, sticky="w")

            if report_exists:
                report_path = data_dir / ch_name / "report" / "report.pptx"
                btn = ctk.CTkButton(
                    self.existing_frame, text="열기", width=60, height=28,
                    font=ctk.CTkFont(size=11),
                    command=lambda p=report_path: self._open_existing_report(p),
                )
                btn.grid(row=row, column=2, padx=0, pady=2)

    def _open_existing_report(self, path: Path) -> None:
        self.report_path = path
        self._open_report()


# ──────────────────────────────────────────────
# 엔트리 포인트
# ──────────────────────────────────────────────
def main() -> None:
    app = DesktopApp()
    app.mainloop()


if __name__ == "__main__":
    main()
