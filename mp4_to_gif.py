#!/usr/bin/env python3
"""Convert an MP4 video file into an animated GIF.

Backends (auto-detected):
  - ffmpeg binary on PATH or bundled via imageio-ffmpeg (palettegen/paletteuse)
  - imageio + Pillow fallback (pure-Python)

Features:
  - Default 10 FPS for balanced animation and reasonable file size
  - Automatic size limiting (--max-size-mb, default 10.0 MB) that optimizes
    resolution and framerate to guarantee the output GIF stays under the limit
  - High quality palette generation with rectangular diff optimization
  - Batch mode (--batch <dir>) for processing entire directories

Examples:
  # Single file
  python mp4_to_gif.py video.mp4
  python mp4_to_gif.py video.mp4 -o out.gif --fps 10 --width 480 --max-size 10

  # Batch mode (processes all videos in a directory)
  python mp4_to_gif.py --batch ./my_clips/
  python mp4_to_gif.py --batch ./my_clips/ -o "recap_%name%.gif" --fps 12
"""

from __future__ import annotations

import argparse
import math
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# ---- Defaults ----
DEFAULT_FPS: int = 10
DEFAULT_WIDTH: int = 480
DEFAULT_MAX_SIZE_MB: float = 10.0


class ConversionError(Exception):
    """Raised when a video cannot be converted."""


def find_ffmpeg() -> str | None:
    """Find ffmpeg on system PATH or via imageio-ffmpeg bundle."""
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg

        bundled = imageio_ffmpeg.get_ffmpeg_exe()
        if bundled and Path(bundled).is_file():
            return bundled
    except Exception:
        pass
    return None


def convert_with_ffmpeg(
    input_path: Path,
    output_path: Path,
    ffmpeg: str,
    fps: int,
    width: int,
    start: float,
    end: float | None,
    loop: int,
    max_colors: int = 256,
) -> None:
    vf = f"fps={fps},scale={width}:-2:flags=lanczos"
    seek = ["-ss", f"{start:.3f}"] if start > 0 else []
    dur = ["-t", f"{end - start:.3f}"] if end is not None else []
    colors = min(max(16, max_colors), 256)

    with tempfile.TemporaryDirectory(prefix="gif_palette_") as tmp:
        palette = Path(tmp) / "palette.png"
        gen = [
            ffmpeg,
            "-y",
            *seek,
            *dur,
            "-i",
            str(input_path),
            "-vf",
            f"{vf},palettegen=stats_mode=diff:max_colors={colors}",
            str(palette),
        ]
        use = [
            ffmpeg,
            "-y",
            *seek,
            *dur,
            "-i",
            str(input_path),
            "-i",
            str(palette),
            "-lavfi",
            f"{vf} [x]; [x][1:v] paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle",
            "-loop",
            str(loop),
            str(output_path),
        ]
        subprocess.run(
            gen, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        subprocess.run(
            use, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )


def convert_with_imageio(
    input_path: Path,
    output_path: Path,
    fps: int,
    width: int,
    start: float,
    end: float | None,
    loop: int,
    max_colors: int = 256,
) -> None:
    import imageio
    from PIL import Image

    reader = imageio.get_reader(str(input_path))
    meta = reader.get_meta_data()
    src_fps = float(meta.get("fps") or 25.0)
    duration = float(meta.get("duration") or 0.0)
    if duration == math.inf or duration <= 0:
        duration = 0.0
    if end is None:
        end = duration if duration > 0 else math.inf
    start = max(start, 0.0)

    # Calculate timestamps to sample matching the target fps
    step = src_fps / max(1, fps)
    pil_frames: list = []
    target_frame = start * src_fps
    stop_idx = math.inf if math.isinf(end) else end * src_fps

    for i, data in enumerate(reader):
        if i >= stop_idx:
            break
        if i < start * src_fps:
            continue
        if i >= target_frame:
            img = Image.fromarray(data).convert("RGB")
            if img.width != width:
                ratio = width / img.width
                new_h = max(2, round(img.height * ratio) // 2 * 2)
                img = img.resize((width, new_h), Image.LANCZOS)
            pil_frames.append(img)
            target_frame += step

    reader.close()

    if not pil_frames:
        raise ConversionError("no frames read from the video (check --start/--end)")
    if len(pil_frames) > 2000:
        print(
            f"Warning: {len(pil_frames)} frames is a lot for a GIF; "
            "consider --start/--end or lower --fps to shorten it.",
            file=sys.stderr,
        )

    # Build one global palette from representative frames
    colors = min(max(16, max_colors), 256)
    palette = pil_frames[0].quantize(colors=colors, dither=Image.Dither.NONE)
    step_sample = max(1, len(pil_frames) // 12)
    for img in pil_frames[::step_sample]:
        palette = img.quantize(palette=palette, dither=Image.Dither.NONE)

    gif_frames = [
        img.quantize(palette=palette, dither=Image.Dither.FLOYDSTEINBERG)
        for img in pil_frames
    ]
    duration_ms = max(10, round(1000 / fps))
    gif_frames[0].save(
        str(output_path),
        save_all=True,
        append_images=gif_frames[1:],
        duration=duration_ms,
        loop=loop,
        disposal=2,
        optimize=True,
    )


def compute_next_parameters(
    width: int,
    fps: int,
    colors: int,
    current_bytes: int,
    target_bytes: int,
) -> tuple[int, int, int]:
    """Compute reduced parameters to fit within the target byte limit."""
    ratio = target_bytes / max(1, current_bytes)
    # Area scales quadratically with width; add a 10% safety margin
    w_scale = math.sqrt(ratio) * 0.90
    new_width = max(180, int(round(width * w_scale / 10) * 10))
    new_fps = fps
    new_colors = colors

    # If width is dropping below 320, start reducing FPS to preserve clarity
    if new_width < 320 and fps > 6:
        new_fps = max(5, int(round(fps * 0.8)))
        fps_ratio = fps / new_fps
        new_width = max(
            220, int(round(width * math.sqrt(ratio * fps_ratio) * 0.92 / 10) * 10)
        )
    elif new_width < 260 and colors > 128:
        new_colors = 128

    # Ensure monotonic reduction to avoid infinite loops
    if new_width >= width and new_fps >= fps and new_colors >= colors:
        if width > 200:
            new_width = max(180, int(width * 0.8 / 10) * 10)
        elif fps > 5:
            new_fps = max(5, fps - 2)
        elif colors > 64:
            new_colors = max(64, colors // 2)

    return new_width, new_fps, new_colors


def convert_mp4_to_gif(
    input_path: Path,
    output_path: Path,
    backend: str,
    ffmpeg: str | None,
    fps: int,
    width: int,
    start: float,
    end: float | None,
    loop: int,
    max_size_mb: float,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    curr_fps = fps
    curr_width = width
    curr_colors = 256
    max_bytes = int(max_size_mb * 1024 * 1024) if max_size_mb > 0 else 0

    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        if backend == "ffmpeg":
            assert ffmpeg is not None
            convert_with_ffmpeg(
                input_path,
                output_path,
                ffmpeg,
                curr_fps,
                curr_width,
                start,
                end,
                loop,
                curr_colors,
            )
        else:
            convert_with_imageio(
                input_path,
                output_path,
                curr_fps,
                curr_width,
                start,
                end,
                loop,
                curr_colors,
            )

        size_bytes = output_path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)

        if max_bytes <= 0 or size_bytes <= max_bytes or attempt == max_attempts:
            if max_bytes > 0 and size_bytes > max_bytes:
                print(
                    f"Warning: Could not reduce GIF below {max_size_mb:.1f} MB "
                    f"(final size: {size_mb:.2f} MB after {attempt} attempts).",
                    file=sys.stderr,
                )
            break

        new_width, new_fps, new_colors = compute_next_parameters(
            curr_width, curr_fps, curr_colors, size_bytes, max_bytes
        )

        if (new_width, new_fps, new_colors) == (curr_width, curr_fps, curr_colors):
            break

        print(
            f"Output size ({size_mb:.2f} MB) exceeded {max_size_mb:.1f} MB target limit. "
            f"Optimizing -> width={new_width}, fps={new_fps}, colors={new_colors}..."
        )
        curr_width, curr_fps, curr_colors = new_width, new_fps, new_colors


# ---- Batch Helpers ----


def find_videos(directory: Path, suffixes: set[str]) -> list[Path]:
    """Return sorted regular files under directory with a matching suffix."""
    return sorted(
        (
            path
            for path in directory.rglob("*")
            if path.is_file() and path.suffix.lower() in suffixes
        ),
        key=lambda path: str(path.relative_to(directory)).lower(),
    )


def make_output(template: str | None, video: Path, counter: int) -> Path:
    """Resolve an output path, honoring the %name%/%extname%/%counter%/%index% placeholders."""
    if not template:
        return video.with_suffix(".gif")

    resolved = template
    if "%name%" in resolved or "%extname%" in resolved:
        resolved = resolved.replace("%name%", video.stem).replace(
            "%extname%", video.suffix.lstrip(".").lower()
        )
    if "%counter%" in resolved or "%i%" in resolved or "%index%" in resolved:
        resolved = (
            resolved.replace("%counter%", str(counter))
            .replace("%i%", str(counter))
            .replace("%index%", str(counter).zfill(4))
        )

    resolved = Path(resolved)
    return (
        resolved if resolved.suffix.lower() == ".gif" else resolved.with_suffix(".gif")
    )


# ---- CLI ----


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert an MP4 video to an animated GIF under a target size limit."
    )

    # Batch Mode / Output Template
    parser.add_argument(
        "--batch",
        type=Path,
        default=None,
        help="Directory to scan for videos (enables batch mode)",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="template",
        type=str,
        default=None,
        help="Output filename template. Works for both single and batch: for single it overrides the output path, e.g '-o out.gif'; for batch use '%%name%%', '%%index%%' (e.g. 'recap_%%index%%_%%name%%.gif').",
    )
    parser.add_argument(
        "-p",
        "--pattern-ext",
        dest="extensions",
        nargs="*",
        type=str,
        default=None,
        help="restrict to these file extensions (batch only), e.g. '.mp4' '.mov'",
    )

    # Core File Arg
    parser.add_argument(
        "input", type=Path, default=None, nargs="?", help="path to the input .mp4 file"
    )

    parser.add_argument(
        "--fps",
        type=int,
        default=None,
        help=f"output frames per second (default: {DEFAULT_FPS})",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=None,
        help=f"output width in px, aspect ratio preserved (default: {DEFAULT_WIDTH})",
    )
    parser.add_argument(
        "--max-size",
        "--max-size-mb",
        dest="max_size_mb",
        type=float,
        default=None,
        help=f"maximum output GIF size in MB, set 0 to disable limit (default: {DEFAULT_MAX_SIZE_MB})",
    )
    parser.add_argument(
        "--start", type=float, default=None, help="start time in seconds"
    )
    parser.add_argument("--end", type=float, default=None, help="end time in seconds")
    parser.add_argument(
        "--loop", type=int, default=None, help="GIF loop count, 0 = infinite"
    )
    parser.add_argument(
        "--backend",
        choices=["auto", "ffmpeg", "imageio"],
        default=None,
        help="conversion backend",
    )

    args = parser.parse_args()

    # Apply defaults if not explicitly provided
    fps = args.fps if args.fps is not None else DEFAULT_FPS
    width = args.width if args.width is not None else DEFAULT_WIDTH
    max_size_mb = (
        args.max_size_mb if args.max_size_mb is not None else DEFAULT_MAX_SIZE_MB
    )
    start = args.start if args.start is not None else 0.0
    loop = args.loop if args.loop is not None else 0

    # --- Batch Mode ---
    if args.batch:
        if not args.batch.is_dir():
            parser.error(f"--batch directory not found: {args.batch}")

        suffixes: set[str] = {".mp4", ".mov", ".m4v", ".mkv", ".webm", ".avi"}
        if args.extensions:
            suffixes = {
                p.strip().lower()
                for p in args.extensions
                if p.strip().lower().startswith(".")
            }

        videos = find_videos(args.batch, suffixes)
        if not videos:
            sys.exit(f"No matching video files under {args.batch}.")

        backend = args.backend if args.backend else "auto"
        ffmpeg_bin = None
        if backend in ("ffmpeg", "auto"):
            ffmpeg_bin = find_ffmpeg()
        if backend == "ffmpeg" and not ffmpeg_bin:
            sys.exit("Error: no ffmpeg found (PATH or imageio-ffmpeg).")
        if backend == "auto":
            backend = "ffmpeg" if ffmpeg_bin else "imageio"

        print(f"Batch Mode: Found {len(videos)} video file(s) in {args.batch}")
        parts = [f"backend={backend}"]
        if args.fps:
            parts.append(f"fps={fps}")
        if args.width:
            parts.append(f"width={width}")
        print(f"Using: {', '.join(parts)}")

        ok = 0
        failed = 0
        for counter, video in enumerate(videos, start=1):
            out_path = make_output(args.template, video, counter)
            try:
                print(f"  [{counter}/{len(videos)}] {video.name} -> {out_path.name}")

                convert_mp4_to_gif(
                    input_path=video,
                    output_path=out_path,
                    backend=backend,
                    ffmpeg=ffmpeg_bin,
                    fps=fps,
                    width=width,
                    start=start,
                    end=args.end,
                    loop=loop,
                    max_size_mb=max_size_mb,
                )
                print(f"      [ok] {out_path.name}")
                ok += 1
            except Exception as e:
                print(f"      [fail] {video.name}: {e}", file=sys.stderr)
                failed += 1

        print(f"\nBatch done. {ok} converted, {failed} failed.")
        if failed:
            raise SystemExit(1)
        return

    # --- Single File Mode ---
    if not args.input:
        parser.error("please provide an input .mp4 file or --batch <directory>")

    if not args.input.is_file():
        parser.error(f"input file not found: {args.input}")
    if args.end is not None and args.end <= args.start:
        parser.error("--end must be greater than --start")

    output = Path(args.template) if args.template else args.input.with_suffix(".gif")

    ffmpeg_bin = find_ffmpeg()
    backend_arg = args.backend if args.backend else "auto"

    if backend_arg == "ffmpeg" and not ffmpeg_bin:
        sys.exit("Error: --backend ffmpeg requested but no ffmpeg binary found.")

    backend = (
        "imageio"
        if backend_arg == "imageio"
        else ("ffmpeg" if ffmpeg_bin else "imageio")
    )

    max_size_str = f", max_size={max_size_mb:.1f}MB" if max_size_mb > 0 else ""
    print(
        f"Converting {args.input} -> {output} (backend={backend}, fps={fps}, width={width}{max_size_str})"
    )

    if backend != "ffmpeg":
        try:
            import imageio  # noqa: F401
        except ImportError:
            sys.exit("Error: imageio missing. Run: uv pip install imageio[pillow]")

    try:
        convert_mp4_to_gif(
            input_path=args.input,
            output_path=output,
            backend=backend,
            ffmpeg=ffmpeg_bin,
            fps=fps,
            width=width,
            start=start,
            end=args.end,
            loop=loop,
            max_size_mb=max_size_mb,
        )
    except ConversionError as error:
        sys.exit(f"Error: {error}")

    final_size_mb = output.stat().st_size / (1024 * 1024)
    print(f"Done: {output} ({final_size_mb:.2f} MB)")


if __name__ == "__main__":
    main()
