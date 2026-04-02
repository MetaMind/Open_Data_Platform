"""Compile all Python files in the metamind package to check for syntax errors."""
from __future__ import annotations

import os
import py_compile
import sys


def main() -> int:
    errors = 0
    root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    for dirpath, _dirs, files in os.walk(root):
        if "__pycache__" in dirpath or "node_modules" in dirpath:
            continue
        for fname in sorted(files):
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(dirpath, fname)
            try:
                py_compile.compile(fpath, doraise=True)
            except py_compile.PyCompileError as exc:
                print(f"COMPILE ERROR: {fpath}\n  {exc}")
                errors += 1
    if errors:
        print(f"\n{errors} compile error(s) found.")
    else:
        print("All Python files compiled successfully.")
    return errors


if __name__ == "__main__":
    sys.exit(main())
