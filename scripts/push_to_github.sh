#!/usr/bin/env bash
# Usage: bash scripts/push_to_github.sh <github-username> [repo-name]
# Prerequisite: create an EMPTY repo on github.com first (no README / .gitignore).
set -euo pipefail

USER_NAME="${1:?Usage: $0 <github-username> [repo-name]}"
REPO="${2:-Day28-Real-Time-Booking-Project}"

cd "$(dirname "$0")/.."

[ -d .git ] || git init -b main
git add .
git diff --cached --quiet || git commit -m "Day 28: Real-time booking project (Scala + Spark Streaming, Pair RDD, windows, broadcast, Spark SQL)"

git remote remove origin 2>/dev/null || true
git remote add origin "https://github.com/${USER_NAME}/${REPO}.git"
git branch -M main
git push -u origin main
