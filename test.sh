#!/usr/bin/env bash

set -e
set -x

pytest --cov=src --cov-report=term-missing --cov-report=html tests "${@}"