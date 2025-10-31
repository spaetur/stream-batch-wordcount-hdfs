#!/usr/bin/env python3
import os
import shlex
import subprocess
import yaml


def run(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def main() -> int:
    default_user = os.getenv("USER", "user")

    cfg = {}
    try:
        with open("config/beam-pipeline.yaml", "r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        cfg = {}

    hdfs_base = cfg.get("hdfs_base", f"/user/{default_user}")
    input_dir = cfg.get("input_dir", "input_dir")
    dataset = cfg.get("dataset", {})
    
    dataset_url = dataset.get("url") or (
        "https://github.com/FilipePires98/LargeText-WordCount/blob/main/datasets/"
        "AChristmasCarol_CharlesDickens/AChristmasCarol_CharlesDickens_English.txt?raw=1"
    )
    
    filename = dataset.get("filename", "input.txt")

    hdfs_input_dir = f"{hdfs_base.rstrip('/')}/{input_dir.strip('/')}"
    target = f"{hdfs_input_dir}/{filename}"

    # Ensure input dir exists
    run(f"hdfs dfs -mkdir -p {shlex.quote(hdfs_input_dir)}")

    # Stream HTTPS → HDFS
    cmd = f"curl -L {shlex.quote(dataset_url)} | hdfs dfs -put -f - {shlex.quote(target)}"
    out = run(cmd)
    
    if out.stderr:
        # print any warnings
        print(out.stderr, end="")
    
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


