"""
Shared environment bootstrap for every PySpark entry point in this project.

Spark is fussy about two things that differ from machine to machine, and getting
either one wrong produces errors that look nothing like their cause:

* **Java version.** Spark 3.5 runs on Java 8, 11 or 17 only. On a newer JDK it
  fails with reflective-access / ``UnsupportedClassVersionError`` style crashes.
* **Worker Python.** The Spark driver and its Python workers must be the *same*
  interpreter. If ``PYSPARK_PYTHON`` is unset, Spark launches whatever ``python``
  is first on ``PATH``; when that differs from the venv running the driver the
  worker dies and Spark only reports ``SocketException: Connection reset``.

Import and call :func:`configure_spark_env` *before* importing ``pyspark``.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

# Spark 3.5 supports these major Java versions.
SUPPORTED_JAVA_MAJORS = (8, 11, 17)

# Directories commonly holding JDK installations on Windows / Linux / macOS.
_JDK_SEARCH_DIRS = (
    r"C:\Program Files\Eclipse Adoptium",
    r"C:\Program Files\Java",
    r"C:\Program Files\Microsoft",
    r"C:\Program Files\Amazon Corretto",
    r"C:\Program Files\Zulu",
    "/usr/lib/jvm",
    "/Library/Java/JavaVirtualMachines",
)


def _java_major(java_home):
    """Return the major Java version of a JDK, or None if it can't be read."""
    java_exe = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
    if not java_exe.exists():
        return None
    try:
        out = subprocess.run(
            [str(java_exe), "-version"],
            capture_output=True, text=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    # `java -version` writes to stderr; the string looks like "17.0.13" or "1.8.0_392".
    match = re.search(r'version "(\d+)(?:\.(\d+))?', out.stderr + out.stdout)
    if not match:
        return None
    major = int(match.group(1))
    if major == 1:  # legacy "1.8.0_x" naming
        major = int(match.group(2) or 0)
    return major


def _find_supported_jdk():
    """Search well-known install locations for a JDK that Spark 3.5 accepts."""
    candidates = []
    for base in _JDK_SEARCH_DIRS:
        base_path = Path(base)
        if not base_path.is_dir():
            continue
        for entry in base_path.iterdir():
            if entry.is_dir() and (entry / "bin").is_dir():
                candidates.append(entry)

    # Prefer the newest supported version (17 over 11 over 8).
    found = []
    for candidate in candidates:
        major = _java_major(candidate)
        if major in SUPPORTED_JAVA_MAJORS:
            found.append((major, candidate))
    if not found:
        return None
    return str(max(found, key=lambda pair: pair[0])[1])


def _configure_java():
    """Point JAVA_HOME at a Spark-compatible JDK, warning if none is found."""
    current = os.environ.get("JAVA_HOME")
    if current and _java_major(current) in SUPPORTED_JAVA_MAJORS:
        return current  # already fine

    jdk = _find_supported_jdk()
    if jdk:
        if current:
            print(f"[spark_env] JAVA_HOME was an unsupported JDK -> using {jdk}")
        else:
            print(f"[spark_env] JAVA_HOME not set -> using {jdk}")
        os.environ["JAVA_HOME"] = jdk
        # Put its bin first so the JVM Spark launches is the one we picked.
        os.environ["PATH"] = str(Path(jdk) / "bin") + os.pathsep + os.environ.get("PATH", "")
        return jdk

    print(
        "[spark_env] WARNING: no Java 8/11/17 installation found. "
        "Spark 3.5 will not run on a newer JDK — install Temurin 17 "
        "(https://adoptium.net) and re-run."
    )
    return current


def _configure_python():
    """Make Spark's Python workers use the same interpreter as the driver."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def _configure_hadoop():
    """Register a winutils.exe install if one exists (Windows-only nicety)."""
    if os.name != "nt":
        return
    hadoop_home = os.environ.get("HADOOP_HOME")
    candidates = [hadoop_home] if hadoop_home else []
    candidates += [r"C:\hadoop", str(Path.home() / "hadoop")]
    for candidate in candidates:
        if candidate and (Path(candidate) / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = candidate
            os.environ["PATH"] = (
                str(Path(candidate) / "bin") + os.pathsep + os.environ.get("PATH", "")
            )
            return
    # Without winutils Spark still reads and writes local files; it only logs a
    # noisy warning, so there is nothing to fail on here.
    os.environ.pop("HADOOP_HOME", None)


def configure_spark_env():
    """Prepare the process environment for PySpark. Safe to call more than once."""
    _configure_java()
    _configure_python()
    _configure_hadoop()
    # Silence Spark's hostname-resolution warning on machines without a FQDN.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def bare_local_fs_jar():
    """Path to the compiled winutils-free local FileSystem shim, if built.

    Returns None when the jar is absent, in which case Spark falls back to
    Hadoop's own local FileSystem (fine for reading, but unable to write on
    Windows without winutils.exe). Build it with:
        python tools/build_barelocalfs.py
    """
    jar = Path(__file__).resolve().parent / "tools" / "barelocalfs" / "bare-local-fs.jar"
    return str(jar) if jar.exists() else None


def needs_local_fs_fix():
    """True when Spark would fail to write locally without the shim.

    Only Windows machines lacking winutils.exe need it; everywhere else Hadoop's
    own local filesystem is used unchanged.
    """
    return os.name == "nt" and not os.environ.get("HADOOP_HOME")


def apply_local_fs_fix(builder):
    """Register the bare local filesystem on a SparkSession.Builder.

    Returns the builder so it can be chained. A no-op when the fix is not needed
    or the jar has not been built.
    """
    if not needs_local_fs_fix():
        return builder

    jar = bare_local_fs_jar()
    if not jar:
        print(
            "[spark_env] NOTE: winutils.exe is not installed and "
            "tools/barelocalfs/bare-local-fs.jar has not been built, so Spark "
            "will not be able to SAVE files. Build it with:\n"
            "            python tools/build_barelocalfs.py"
        )
        return builder

    return (
        builder
        .config("spark.jars", jar)
        .config("spark.driver.extraClassPath", jar)
        .config("spark.executor.extraClassPath", jar)
        .config("spark.hadoop.fs.file.impl", "org.bigdataproject.fs.BareLocalFileSystem")
    )


def project_root():
    """Absolute path to the repository root (the directory holding this file)."""
    return Path(__file__).resolve().parent
