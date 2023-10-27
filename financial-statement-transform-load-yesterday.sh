#!/usr/bin/env bash

yesterday=$(date -d "-1 day" "+%F")
dir=$(dirname "$0")

racket -y ${dir}/balance-sheet-transform-load.rkt -d ${yesterday} -p "$1"
racket -y ${dir}/cash-flow-statement-transform-load.rkt -d ${yesterday} -p "$1"
racket -y ${dir}/income-statement-transform-load.rkt -d ${yesterday} -p "$1"

7zr a /var/tmp/zacks/balance-sheet/${yesterday}.7z /var/tmp/zacks/balance-sheet/${yesterday}/*.html
7zr a /var/tmp/zacks/cash-flow-statement/${yesterday}.7z /var/tmp/zacks/cash-flow-statement/${yesterday}/*.html
7zr a /var/tmp/zacks/income-statement/${yesterday}.7z /var/tmp/zacks/income-statement/${yesterday}/*.html

racket -y ${dir}/dump-dolt-statements.rkt -p "$1"
