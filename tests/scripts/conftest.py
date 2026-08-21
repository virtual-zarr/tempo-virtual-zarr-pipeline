import os
import sys

# scripts/ holds project utility scripts that are not on the default pytest
# path; add it so their pure helpers can be unit-tested (mirrors tests/exploration).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
