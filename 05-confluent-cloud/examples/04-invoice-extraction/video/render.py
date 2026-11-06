"""Render video.html frame by frame and mux the voiceover.

    uv run --no-project --with playwright python render.py            # → ex04-docling-confluent.mp4
    uv run --no-project --with playwright python render.py --stills 3,12,25,40,50,57
"""
import argparse
import pathlib
import subprocess
import sys

from playwright.sync_api import sync_playwright

HERE = pathlib.Path(__file__).parent
FPS = 30

ap = argparse.ArgumentParser()
ap.add_argument("--stills")
ap.add_argument("--out", default=str(HERE / "ex04-docling-confluent.mp4"))
args = ap.parse_args()

with sync_playwright() as pw:
    browser = pw.chromium.launch(channel="chrome")
    page = browser.new_page(viewport={"width": 1920, "height": 1080})
    page.goto((HERE / "video.html").as_uri())
    page.wait_for_load_state("networkidle")
    page.evaluate("document.fonts.ready")
    if args.stills:
        for s in args.stills.split(","):
            page.evaluate(f"render({float(s)})")
            page.screenshot(path=str(HERE / f"still-{s}.png"))
        sys.exit()
    total = page.evaluate("TOTAL")
    VO_AT = page.evaluate("VO_AT")   # where each voiceover clip starts, as video.html lays the scenes out
    silent = HERE / "_silent.mp4"
    ff = subprocess.Popen(["ffmpeg", "-y", "-loglevel", "error", "-f", "image2pipe", "-framerate", str(FPS), "-c:v", "mjpeg",
                           "-i", "-", "-c:v", "libx264", "-preset", "slow", "-crf", "18", "-pix_fmt", "yuv420p", str(silent)],
                          stdin=subprocess.PIPE)
    n = int(total * FPS)
    for i in range(n):
        page.evaluate(f"render({i / FPS})")
        ff.stdin.write(page.screenshot(type="jpeg", quality=95))
        if i % 150 == 0:
            print(f"frame {i}/{n}", flush=True)
    ff.stdin.close(); ff.wait()
    browser.close()

inputs, filt = [], []
for k, at in enumerate(VO_AT, 1):
    inputs += ["-i", str(HERE / "vo" / f"s{k}.wav")]
    ms = int(at * 1000)
    filt.append(f"[{k}:a]aresample=48000,adelay={ms}|{ms}[a{k}]")
mix = "".join(f"[a{k}]" for k in range(1, len(VO_AT) + 1))
filt.append(f"{mix}amix=inputs={len(VO_AT)}:normalize=0,loudnorm=I=-16:TP=-1.5:LRA=11,aresample=48000,apad[aout]")
subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(silent), *inputs, "-filter_complex", ";".join(filt),
                "-map", "0:v", "-map", "[aout]", "-c:v", "copy", "-c:a", "aac", "-b:a", "192k", "-shortest",
                "-movflags", "+faststart", args.out], check=True)
silent.unlink()
print("wrote", args.out)
