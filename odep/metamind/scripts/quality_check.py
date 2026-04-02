#!/usr/bin/env python3
"""
Quality Check Script

Validates code quality gates:
- File size (< 500 lines)
- Type hints coverage
- No bare except clauses
"""

from __future__ import annotations

import ast
import os
import sys
from pathlib import Path
from typing import List, Tuple

MAX_LINES = 500
EXCLUDED_FILES = {"__init__.py", "test_*.py", "conftest.py"}


def should_check_file(file_path: Path) -> bool:
    """Check if file should be validated."""
    name = file_path.name
    
    for pattern in EXCLUDED_FILES:
        if pattern.endswith("*.py"):
            prefix = pattern[:-5]  # Remove "*.py"
            if name.startswith(prefix):
                return False
        elif name == pattern:
            return False
    
    return True


def check_file_size(file_path: Path) -> List[str]:
    """Check if file exceeds max lines."""
    violations = []
    
    with open(file_path, "r") as f:
        lines = f.readlines()
    
    if len(lines) > MAX_LINES:
        violations.append(
            f"{file_path}: File has {len(lines)} lines (max {MAX_LINES})"
        )
    
    return violations


def check_type_hints(file_path: Path) -> List[str]:
    """Check for type hints coverage."""
    violations = []
    
    try:
        with open(file_path, "r") as f:
            tree = ast.parse(f.read())
    except SyntaxError:
        return violations
    
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Skip private functions
            if node.name.startswith("_"):
                continue
            
            # Check return annotation
            if node.returns is None and node.name != "__init__":
                violations.append(
                    f"{file_path}:{node.lineno}: "
                    f"Function '{node.name}' missing return type hint"
                )
            
            # Check argument annotations
            for arg in node.args.args:
                if arg.annotation is None and arg.arg != "self":
                    violations.append(
                        f"{file_path}:{node.lineno}: "
                        f"Argument '{arg.arg}' in '{node.name}' missing type hint"
                    )
    
    return violations


def check_bare_except(file_path: Path) -> List[str]:
    """Check for bare except clauses."""
    violations = []
    
    try:
        with open(file_path, "r") as f:
            tree = ast.parse(f.read())
    except SyntaxError:
        return violations
    
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler):
            if node.type is None:
                violations.append(
                    f"{file_path}:{node.lineno}: Bare except clause found"
                )
    
    return violations


def main() -> int:
    """Main validation function."""
    project_root = Path(__file__).parent.parent
    metamind_dir = project_root / "metamind"
    
    if not metamind_dir.exists():
        print(f"Error: {metamind_dir} not found")
        return 1
    
    all_violations = []
    
    for py_file in metamind_dir.rglob("*.py"):
        if not should_check_file(py_file):
            continue
        
        all_violations.extend(check_file_size(py_file))
        all_violations.extend(check_type_hints(py_file))
        all_violations.extend(check_bare_except(py_file))
    
    if all_violations:
        print("Quality violations found:")
        for v in all_violations:
            print(f"  {v}")
        return 1
    else:
        print("All quality checks passed.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
