import os
import shlex
import subprocess
import sys
import re
from typing import Dict, Optional

import yaml
from cryptography.fernet import Fernet


def run_cmd(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def load_yaml_config(path: str) -> Dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}


def allowed_charset_regex(allowed_charset: str) -> str:
    # Pattern for replacing non-allowed with space
    return f"[^ {allowed_charset}]+"


def get_or_create_fernet_key(hdfs_output_dir: str, key_path: Optional[str] = None) -> bytes:
    kp = key_path or f"{hdfs_output_dir}/_fernet.key"
    exists = subprocess.run(f"hdfs dfs -test -e {shlex.quote(kp)}", shell=True).returncode == 0
    if exists:
        out = run_cmd(f"hdfs dfs -cat {shlex.quote(kp)}")
        return out.stdout.strip().encode()
    key = Fernet.generate_key()
    run_cmd(f"printf '%s' '{key.decode()}' | hdfs dfs -put -f - {shlex.quote(kp)}")
    return key


def total_header(total_str: str, encrypt: bool, fernet_key: Optional[bytes]) -> str:
    if encrypt and fernet_key:
        token = Fernet(fernet_key).encrypt(total_str.encode()).decode()
        return f"total_words:{token}"
    return f"total_words:{total_str}"


def awk_norm_agg(allowed_charset: str) -> str:
    # Lowercase, strip non-allowed, aggregate by word
    return (
        f"awk 'BEGIN{{FS=\"\\t\"}} "
        f"{{w=tolower($1); gsub(/[^ {allowed_charset}]+/, \"\", w); if(w!=\"\"){{a[w]+=$2}}}} "
        f"END{{for(k in a) print k\"\\t\"a[k]}}'"
    )


def normalize_text(text: str, lowercase: bool, allowed_charset: str) -> str:
    """Normalize text by optional lowercasing and replacing non-allowed chars with spaces.
    allowed_charset is a character class fragment like "a-z0-9".
    """
    t = text.lower() if lowercase else text
    pattern = re.compile(fr"[^ {allowed_charset}]+")
    return pattern.sub(" ", t)


def print_hdfs_output(hdfs_output_path: str, encrypt_total: bool, fernet_key: Optional[bytes]) -> int:
    """Print HDFS output where the first line is total_words (possibly encrypted).
    Prints counts first, then a final total_words line in plaintext. Returns a process-like returncode (0 on success).
    """
    try:
        out = run_cmd(f"hdfs dfs -cat {shlex.quote(hdfs_output_path)}", check=True)
        content = out.stdout.splitlines()
        if content and content[0].startswith("total_words:"):
            enc = content[0].split(":", 1)[1].strip()
            if encrypt_total:
                total_plain = Fernet(fernet_key).decrypt(enc.encode()).decode()
            else:
                total_plain = enc
            sys.stdout.write("\n".join(content[1:]) + ("\n" if len(content) > 1 else ""))
            print(f"total_words: {total_plain}")
        else:
            sys.stdout.write(out.stdout)
        return 0
    except subprocess.CalledProcessError as e:
        sys.stderr.write(e.stderr or "")
        return e.returncode

