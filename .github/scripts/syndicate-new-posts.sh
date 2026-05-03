#!/usr/bin/env bash
set -euo pipefail

before_sha="${1:?before sha is required}"
current_sha="${2:?current sha is required}"
python_bin="${PYTHON_BIN:-python3}"
dry_run="${DRY_RUN:-false}"

is_truthy() {
    local lower_value

    lower_value="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
    case "$lower_value" in
        1|true|yes|on)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

args=("$before_sha" "$current_sha")
if is_truthy "$dry_run"; then
    args+=("--dry-run")
fi

exec "$python_bin" .github/scripts/publish_new_posts.py "${args[@]}"