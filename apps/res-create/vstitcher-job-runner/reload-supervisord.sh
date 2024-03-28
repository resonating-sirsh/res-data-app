#!/usr/bin/env bash
set -euo pipefail

supervisorctl reread
supervisorctl update
