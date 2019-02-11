#lang racket/base

(require db
         racket/cmdline
         racket/vector
         srfi/19) ; Time Data Types and Procedures

(define start-date (make-parameter (date->string (current-date) "~1")))

(define end-date (make-parameter (date->string (current-date) "~1")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dump-dat.rkt"
 #:once-each
 [("-e" "--end-date") end
                      "Final date for history retrieval. Defaults to today"
                      (end-date end)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-s" "--start-date") start
                        "Earliest date for history retrieval. Defaults to today"
                        (start-date start)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define (vector->csv-line vec)
  (if (= 1 (vector-length vec))
      (vector-ref vec 0)
      (string-append (vector-ref vec 0) "," (vector->csv-line (vector-drop vec 1)))))

(for-each (λ (date)
            (call-with-output-file (string-append "/var/tmp/dat/zacks/eps-estimate/" date ".csv")
              (λ (out)
                (displayln "act_symbol,date,period,period_end_date,consensus,recent,count,high,low,year_ago" out)
                (for-each (λ (row)
                            (displayln (vector->csv-line row) out))
                          (query-rows dbc "
select
  act_symbol::text,
  date::text,
  period::text,
  period_end_date::text,
  consensus::text,
  recent::text,
  count::text,
  high::text,
  low::text,
  year_ago::text
from
  zacks.eps_estimate
where
  date = $1::text::date and
  consensus is not null and
  recent is not null and
  count is not null and
  high is not null and
  low is not null and
  year_ago is not null
order by
  act_symbol, date, period, period_end_date
"
                                      date)))
              #:exists 'replace))
          (query-list dbc "
select distinct
  date::text
from
  zacks.eps_estimate
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

(for-each (λ (date)
            (call-with-output-file (string-append "/var/tmp/dat/zacks/sales-estimate/" date ".csv")
              (λ (out)
                (displayln "act_symbol,date,period,period_end_date,consensus,count,high,low,year_ago" out)
                (for-each (λ (row)
                            (displayln (vector->csv-line row) out))
                          (query-rows dbc "
select
  act_symbol::text,
  date::text,
  period::text,
  period_end_date::text,
  consensus::text,
  count::text,
  high::text,
  low::text,
  year_ago::text
from
  zacks.sales_estimate
where
  date = $1::text::date and
  consensus is not null and
  count is not null and
  high is not null and
  low is not null and
  year_ago is not null
order by
  act_symbol, date, period, period_end_date
"
                                      date)))
              #:exists 'replace))
          (query-list dbc "
select distinct
  date::text
from
  zacks.sales_estimate
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

