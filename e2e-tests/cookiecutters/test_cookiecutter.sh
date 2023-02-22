#!/bin/bash
CC_BUILD_PATH=/tmp
TAP_TEMPLATE=$(realpath $1)
REPLAY_FILE=$(realpath $2)
SDK_DIR=$(realpath $1/../..)
CC_OUTPUT_DIR=$(basename $REPLAY_FILE .json) # name of replay file without .json
RUN_LINT=${3:-1}

usage() {
    echo "test_cookiecutter.sh [tap_template] [replay_file.json]"
    echo
    echo "Uses the tap template to build an empty cookiecutter, and runs the lint task on the created test project"
    echo ""
    if [[ $# -eq 1 ]]; then
        echo "ERROR: $1"
    fi
}

if [[ ! -d $TAP_TEMPLATE ]]; then
    usage "Tap template folder not found"
    exit 1
fi
if [[ ! -f $REPLAY_FILE ]]; then
    usage "Replay file not found"
    exit 1
fi

CC_TEST_OUTPUT=$CC_BUILD_PATH/$CC_OUTPUT_DIR
if [[ -d "$CC_TEST_OUTPUT" ]]; then
    rm -fr "$CC_TEST_OUTPUT"
fi

if [[ -f $GITHUB_ENV ]]; then
    echo CC_TEST_OUTPUT=$CC_BUILD_PATH/$CC_OUTPUT_DIR >> $GITHUB_ENV
fi

cookiecutter --replay-file $REPLAY_FILE $TAP_TEMPLATE -o $CC_BUILD_PATH &&
    cd $CC_TEST_OUTPUT &&
    pwd &&
    sed -i.bak "s|singer-sdk =.*|singer-sdk = \{ path = \"$SDK_DIR\", develop = true \}|" pyproject.toml &&
    poetry lock &&
    poetry install

if [[ $? -ne 0 ]]; then
    exit $?
fi

# before linting, auto-fix what can be autofixed
LIBRARY_NAME=$(ls * -d | egrep "tap|target")
poetry run black $LIBRARY_NAME &&
    poetry run isort $LIBRARY_NAME &&
    poetry run flake8 $LIBRARY_NAME &&
    poetry run mypy $LIBRARY_NAME
##

if [[ $RUN_LINT -eq 1 ]] && [[ $? -eq 0 ]]; then
    poetry run tox -e lint
fi
