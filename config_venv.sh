#!/usr/bin/env bash

# this script allows project dependencies to be managed by pip-tools
# a 'pyproject.toml' file is required in the root of the project

nc='\033[0m'
red='\033[0;31m'
green='\033[0;32m'
blue='\033[0;34m'
cyan='\033[0;36m'
pip_prefix="$green[config_venv]$nc"
python_version=$(python3 -c "import platform; print(platform.python_version())")
which_python=$(which python)

# verify that Python from local .venv is being used
if [[ $which_python != "$PWD/.venv"* ]]; then
    echo -e "$pip_prefix$red shell is using Python from ($which_python), needs to use project Python version in ($PWD/.venv)$nc"
    exit 1
fi

has_pip=0
if command -v pip 2>&1 >/dev/null; then
    if [[ $(which pip) = "$PWD/"* ]]; then
        has_pip=1
    fi
fi
if [ $has_pip -eq 0 ]; then
    echo -e "$pip_prefix$red ERROR: No pip installed via layout; add 'layout python3' to .envrc$nc"
    exit 1
fi

if ! command -v pip-compile 2>&1 >/dev/null; then
    echo -e "$pip_prefix pip-tools missing; installing..."
    pip install pip-tools
fi

if [ ! -f "pyproject.toml" ]; then
    echo -e "$pip_prefix$red ERROR: No 'pyproject.toml' file exists, please create one!$nc"
    exit 1
fi

# create requirements files
echo -e "$pip_prefix checking/updating/creating requirements.txt files..."
pip-compile --quiet --strip-extras -o requirements.txt
pip-compile --quiet --strip-extras --extra=dev -o requirements-dev.txt

echo -e "$pip_prefix syncing (dev)requirements to Python virtual environment ($VIRTUAL_ENV)..."
# install all pinned requirements to virtual environment
pip-sync requirements.txt requirements-dev.txt

# install local project from /src as editable, so code changes are reflected without re-install
echo -e "$pip_prefix installing project to Python virtual environment ($VIRTUAL_ENV)..."
pip install --editable .

echo -e "$pip_prefix Python virtual environment sucessfully created, updated and activated!"
echo -e "$pip_prefix Running $blue'which python'$nc should show $cyan'$which_python'$nc"
echo -e "$pip_prefix Running $blue'python --version'$nc should show $cyan'Python $python_version'$nc"
