#!/usr/bin/env python3
"""
One-shot setup for this project.

    py -3.10 setup.py

Creates the .venv, installs requirements.txt into it, builds the
winutils-free Hadoop filesystem shim (Windows only), and downloads the NLTK
corpora the dashboard needs. Re-running it is safe.

Training the model and starting Kafka/MongoDB are deliberately left as separate
steps; see the README.
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV = ROOT / ".venv"
PY = VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")

# PySpark 3.5 does not support Python 3.12+ workers.
MIN_VERSION, MAX_VERSION = (3, 10), (3, 11)


def run(cmd, **kwargs):
    print(f"\n$ {' '.join(str(c) for c in cmd)}")
    return subprocess.run(cmd, check=True, **kwargs)


def main():
    print("=" * 64)
    print("Real-Time Twitter Sentiment Analysis - setup")
    print("=" * 64)

    version = sys.version_info[:2]
    if not (MIN_VERSION <= version <= MAX_VERSION):
        print(
            f"[FAIL] Run this with Python 3.10 or 3.11 (you used {version[0]}.{version[1]}).\n"
            f"       PySpark 3.5 cannot use newer interpreters as Spark workers.\n"
            f"       Try:  py -3.10 setup.py"
        )
        return 1

    if not PY.exists():
        print(f"\n[1/4] Creating virtual environment at {VENV}")
        run([sys.executable, "-m", "venv", str(VENV)])
    else:
        print(f"\n[1/4] Virtual environment already present at {VENV}")

    print("\n[2/4] Installing Python dependencies")
    run([str(PY), "-m", "pip", "install", "--upgrade", "pip", "--quiet"])
    run([str(PY), "-m", "pip", "install", "-r", str(ROOT / "requirements.txt")])

    print("\n[3/4] Building the local-filesystem shim (lets Spark save on Windows)")
    if os.name == "nt":
        run([str(PY), str(ROOT / "tools" / "build_barelocalfs.py")])
    else:
        print("      Skipped: only needed on Windows.")

    print("\n[4/4] Downloading NLTK corpora")
    run([str(PY), "-c",
         "import nltk;[nltk.download(p, quiet=True) for p in ('punkt','punkt_tab','stopwords')];"
         "print('      punkt, punkt_tab, stopwords ready')"])

    activate = ".venv\\Scripts\\activate" if os.name == "nt" else "source .venv/bin/activate"
    print("\n" + "=" * 64)
    print("Setup complete. Next:")
    print("=" * 64)
    print(f"  1. Activate the venv:   {activate}")
    print("  2. Train the model:     python train_model.py")
    print("  3. Start the services:  docker compose -f zk-single-kafka-single.yml up -d")
    print("  4. Run the pipeline:    python Kafka-PySpark/producer-validation-tweets.py")
    print("                          python Kafka-PySpark/consumer-pyspark.py")
    print("  5. Run the dashboard:   cd Django-Dashboard && python manage.py runserver")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as exc:
        print(f"\n[FAIL] Command failed with exit code {exc.returncode}")
        sys.exit(exc.returncode)
