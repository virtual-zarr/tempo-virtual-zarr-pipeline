import os
import sys

# exploration/ holds standalone PEP 723 scripts that are not on the default
# pytest path; add it so their pure helpers can be unit-tested (mirrors the
# tests/cdk conftest pattern).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "exploration"))
