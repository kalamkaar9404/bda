#!/usr/bin/env python3
"""
Build tools/barelocalfs/bare-local-fs.jar — the winutils-free local Hadoop
FileSystem that lets Spark SAVE files on Windows.

Compiles against the Hadoop jars that ship inside the installed pyspark, using
the same JDK that spark_env picks for Spark. Run once after installing
requirements:

    python tools/build_barelocalfs.py
"""

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from spark_env import configure_spark_env  # noqa: E402

HERE = Path(__file__).resolve().parent / "barelocalfs"
SRC_DIR = HERE / "src" / "org" / "bigdataproject" / "fs"
CLASSES = HERE / "classes"
JAR = HERE / "bare-local-fs.jar"


def main():
    configure_spark_env()

    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        print("[FAIL] No JDK found. Install Temurin 17: https://adoptium.net")
        return 1

    jdk = Path(java_home)
    exe = ".exe" if os.name == "nt" else ""
    javac, jar_tool = jdk / "bin" / f"javac{exe}", jdk / "bin" / f"jar{exe}"
    if not javac.exists():
        print(f"[FAIL] {javac} not found — JAVA_HOME points at a JRE, not a JDK.")
        return 1

    import pyspark
    jars = Path(pyspark.__file__).parent / "jars"
    hadoop = sorted(jars.glob("hadoop-client-api-*.jar"))
    if not hadoop:
        print(f"[FAIL] No hadoop-client-api jar under {jars}")
        return 1

    print(f"[..] Compiling with {javac}")
    CLASSES.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [str(javac), "-nowarn", "-cp", str(hadoop[0]), "-d", str(CLASSES)] + [str(f) for f in sorted(SRC_DIR.glob("*.java"))],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print("[FAIL] Compilation failed:\n" + result.stdout + result.stderr)
        return 1

    print(f"[..] Packaging {JAR.name}")
    result = subprocess.run(
        [str(jar_tool), "--create", "--file", str(JAR), "-C", str(CLASSES), "."],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print("[FAIL] jar failed:\n" + result.stdout + result.stderr)
        return 1

    print(f"[OK] Built {JAR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
