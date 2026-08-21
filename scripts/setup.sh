#!/bin/bash
set -e  # Exit on error

echo "📦 Syncing uv dependencies..."
uv sync --all-groups

echo "📦 Setting up Node.js environment..."
# An LTS release: jsii warns on every cdk command under non-LTS Node.
uv run nodeenv --node=22.18.0 -p

echo "📦 Installing AWS CDK..."
# Keep the CLI current with aws-cdk-lib in pyproject.toml: a CLI much older
# than the lib fails at deploy with a cloud assembly schema version mismatch.
uv run npm install -g aws-cdk@2.1135.1

echo "✅ Setup complete!"
