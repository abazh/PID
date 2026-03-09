#!/usr/bin/env python3
"""
Simple wait-for script: wait for one or more host:port pairs to accept TCP connections,
then exec the supplied command.

Usage:
  python wait_for.py host1:port [host2:port ...] -- cmd arg...

This script is intentionally small and dependency-free so it works in slim images.
"""
import os
import socket
import sys
import time
import subprocess


def wait_for(address, timeout=None, interval=1.0):
    host, port = address.split(":")
    port = int(port)
    start = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=3):
                return True
        except OSError:
            pass
        if timeout is not None and (time.time() - start) > timeout:
            return False
        time.sleep(interval)


def main():
    if "--" not in sys.argv:
        print("Usage: wait_for.py host:port [host2:port ...] -- cmd args...", file=sys.stderr)
        sys.exit(2)

    idx = sys.argv.index("--")
    targets = sys.argv[1:idx]
    cmd = sys.argv[idx + 1 :]
    if not targets or not cmd:
        print("Usage: wait_for.py host:port [host2:port ...] -- cmd args...", file=sys.stderr)
        sys.exit(2)

    timeout = None
    # optional env WAIT_TIMEOUT (seconds)
    if os.getenv("WAIT_TIMEOUT"):
        try:
            timeout = float(os.getenv("WAIT_TIMEOUT"))
        except Exception:
            timeout = None

    for t in targets:
        print(f"waiting for {t}...")
        ok = wait_for(t, timeout=timeout)
        if not ok:
            print(f"timeout waiting for {t}", file=sys.stderr)
            sys.exit(1)
        print(f"{t} is available")

    # exec the command
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    main()
