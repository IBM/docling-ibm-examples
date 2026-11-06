"""Voiceover with Kokoro (open-weights neural TTS, runs locally): vo/s<N>.wav per line of vo/script.txt,
and assets/timing.js with their durations, which video.html and render.py lay the scenes out from.

    uv run --no-project --python 3.12 --with kokoro-onnx --with soundfile python tts.py [--voice af_heart] [--speed 1.05]

Model files (kokoro-v1.0.onnx, voices-v1.0.bin) from github.com/thewh1teagle/kokoro-onnx/releases, in $KOKORO_DIR.
"""
import argparse
import json
import os
import pathlib

import soundfile as sf
from kokoro_onnx import Kokoro

HERE = pathlib.Path(__file__).parent
ap = argparse.ArgumentParser()
ap.add_argument("--voice", default="af_heart")
ap.add_argument("--speed", type=float, default=1.05)
args = ap.parse_args()

d = pathlib.Path(os.environ["KOKORO_DIR"])
tts = Kokoro(str(d / "kokoro-v1.0.onnx"), str(d / "voices-v1.0.bin"))
durs = []
for i, line in enumerate((HERE / "vo" / "script.txt").read_text().splitlines(), 1):
    audio, sr = tts.create(line, voice=args.voice, speed=args.speed, lang="en-us")
    sf.write(HERE / "vo" / f"s{i}.wav", audio, sr)
    durs.append(round(len(audio) / sr, 3))
    print(f"s{i} {durs[-1]:.2f}s")
(HERE / "assets" / "timing.js").write_text(f"window.VO = {json.dumps(durs)};\n")
print("total voice", round(sum(durs), 2))
