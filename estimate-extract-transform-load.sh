#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")

racket -y ${dir}/estimate-extract.rkt -p "$1"
racket -y ${dir}/estimate-transform-load.rkt -p "$1"

7zr a /var/local/zacks/estimates/${today}.7z /var/local/zacks/estimates/${today}/*.html

racket -y ${dir}/dump-dolt-estimates.rkt -p "$1"
