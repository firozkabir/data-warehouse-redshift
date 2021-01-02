#!/bin/bash

app_dir=$(dirname "${BASH_SOURCE[0]}")

. ${app_dir}/venv/bin/activate

${app_dir}/dwh-main.py "$@"


deactivate
