#!/bin/sh
# Проверяет формат всех коммитов диапазона (по умолчанию origin/develop..HEAD)
# той же регуляркой, что и .githooks/commit-msg. Используется в CI на PR.
range=${1:-origin/develop..HEAD}
re='^(feat|fix|refactor|test|docs|chore|ci|build|perf)\((gateway|catalog|orders|internal|lab|deploy|repo)\)!?: [^A-Z[:space:]].{0,70}[^.[:space:]]$'

bad=$(git log --no-merges --format='%h %s' "$range" \
    | grep -Ev '^[0-9a-f]+ (Revert |fixup! |squash! )' \
    | grep -Ev "^[0-9a-f]+ ${re#^}" || true)

if [ -n "$bad" ]; then
    echo "коммиты не по формату <type>(<scope>): <описание>:"
    echo "$bad"
    echo
    echo "См. README «Ветки, коммиты, релизы». Чинить: git rebase -i"
    exit 1
fi
echo "все коммиты в $range по формату"
