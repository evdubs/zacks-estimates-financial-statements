#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")
current_year=$(date "+%Y")

racket -y ${dir}/dividend-calendar-extract.rkt
racket -y ${dir}/dividend-calendar-transform-load.rkt -p "$1"

7zr a /var/tmp/zacks/dividend-calendar/${current_year}.7z /var/tmp/zacks/dividend-calendar/${today}

# racket -y ${dir}/dump-dolt-dividend-calendar.rkt -p "$1"
