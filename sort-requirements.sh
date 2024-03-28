#!/usr/bin/env bash
proj_root="$(git rev-parse --show-toplevel)"
echo "Sorting requirements files in-place via \`sort -fuo <file> <file>\`..."
find "$proj_root" -type f -name '*requirements.in' \
    -exec sort -fuC {} \; \
    -exec sort -fuo {} {} \; \
    -exec git status --porcelain=v1 {} \; \
    -exec git add {} \;

echo "Done\! Any changes have been staged."
echo "Please commit them along with your other work." 
