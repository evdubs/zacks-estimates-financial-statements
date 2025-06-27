#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")
current_year=$(date "+%Y")

racket -y ${dir}/earnings-calendar-extract.rkt
racket -y ${dir}/earnings-calendar-transform-load.rkt -p "$1"

7zr a /var/tmp/zacks/earnings-calendar/${current_year}.7z /var/tmp/zacks/earnings-calendar/${today}

racket -y ${dir}/dump-dolt-earnings-calendar.rkt -p "$1"
