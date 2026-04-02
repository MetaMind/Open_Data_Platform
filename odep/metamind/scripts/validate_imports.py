#!/usr/bin/env python3
"""
Import Validation Script

Checks for circular imports and layer violations.
Dependencies should flow: api -> core -> execution -> storage
"""

from __future__ import annotations

import ast
import os
import sys
from pathlib import Path
from typing import Set, Dict, List, Tuple

# Layer definitions (higher = lower layer)
LAYERS = {
    "api": 4,
    "core": 3,
    "execution": 2,
    "storage": 1,
    "cache": 2,
    "ml": 2,
    "config": 0,  # Can be imported anywhere
    "observability": 1,
    "safety": 1,
    "security": 1,
}


def get_layer(module_path: str) -> int:
    """Get layer priority for a module path."""
    parts = module_path.split(".")
    for part in parts:
        if part in LAYERS:
            return LAYERS[part]
    return 0


def extract_imports(file_path: Path) -> List[Tuple[str, int]]:
    """Extract imports from a Python file."""
    imports = []
    
    try:
        with open(file_path, "r") as f:
            tree = ast.parse(f.read())
    except SyntaxError as e:
        print(f"Syntax error in {file_path}: {e}")
        return imports
    
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imports.append((module, node.lineno))
    
    return imports


def validate_file(file_path: Path, project_root: Path) -> List[str]:
    """Validate a single file for import violations."""
    violations = []
    
    # Get file's module path
    relative_path = file_path.relative_to(project_root / "metamind")
    module_parts = list(relative_path.with_suffix("").parts)
    file_module = "metamind." + ".".join(module_parts)
    file_layer = get_layer(file_module)
    
    # Extract imports
    imports = extract_imports(file_path)
    
    for import_path, line_no in imports:
        if not import_path.startswith("metamind."):
            continue
        
        import_layer = get_layer(import_path)
        
        # Check layer violation (importing higher layer from lower layer)
        if import_layer > file_layer:
            violations.append(
                f"{file_path}:{line_no}: Layer violation - "
                f"{file_module} (layer {file_layer}) imports "
                f"{import_path} (layer {import_layer})"
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
    
    # Scan all Python files
    for py_file in metamind_dir.rglob("*.py"):
        if py_file.name == "__init__.py":
            continue
        
        violations = validate_file(py_file, project_root)
        all_violations.extend(violations)
    
    if all_violations:
        print("Import violations found:")
        for v in all_violations:
            print(f"  {v}")
        return 1
    else:
        print("No import violations found.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
