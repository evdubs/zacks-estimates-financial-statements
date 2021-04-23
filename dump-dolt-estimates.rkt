#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/tmp/dolt/earnings"))

(define start-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define end-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dump-dolt-estimates.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Base dolt folder. Defaults to /var/tmp/dolt/earnings"
                         (base-folder folder)]
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

; rank-score
(for-each (λ (date)
            (define rank-score-file (string-append (base-folder) "/rank-score-" date ".csv"))
            (call-with-output-file rank-score-file
              (λ (out)
                (displayln "date,act_symbol,rank,value,growth,momentum,vgm" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  date::text,
  act_symbol::text,
  rank::text,
  value::text,
  growth::text,
  momentum::text,
  vgm::text
from
  zacks.rank_score
where
  date = $1::text::date
order by
  act_symbol, date;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u rank_score rank-score-" date ".csv")))
          (query-list dbc "
select distinct
  date::text
from
  zacks.rank_score
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add rank_score; "
                       "/usr/local/bin/dolt commit -m 'rank_score " (end-date) " update'; /usr/local/bin/dolt push"))

; eps-estimate
(for-each (λ (date)
            (define eps-estimate-file (string-append (base-folder) "/eps-estimate-" date ".csv"))
            (call-with-output-file eps-estimate-file
              (λ (out)
                (displayln "date,act_symbol,period,period_end_date,consensus,recent,count,high,low,year_ago" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  date::text,
  act_symbol::text,
  period::text,
  period_end_date::text,
  coalesce(consensus::text, ''),
  coalesce(recent::text, ''),
  coalesce(count::text, ''),
  coalesce(high::text, ''),
  coalesce(low::text, ''),
  coalesce(year_ago::text, '')
from
  zacks.eps_estimate
where
  date = $1::text::date
order by
  act_symbol, date, period;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u eps_estimate eps-estimate-" date ".csv")))
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

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add eps_estimate; "
                       "/usr/local/bin/dolt commit -m 'eps_estimate " (end-date) " update'; /usr/local/bin/dolt push"))

; sales-estimate
(for-each (λ (date)
            (define sales-estimate-file (string-append (base-folder) "/sales-estimate-" date ".csv"))
            (call-with-output-file sales-estimate-file
              (λ (out)
                (displayln "date,act_symbol,period,period_end_date,consensus,count,high,low,year_ago" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  date::text,
  act_symbol::text,
  period::text,
  period_end_date::text,
  coalesce(consensus::text, ''),
  coalesce(count::text, ''),
  coalesce(high::text, ''),
  coalesce(low::text, ''),
  coalesce(year_ago::text, '')
from
  zacks.sales_estimate
where
  date = $1::text::date
order by
  act_symbol, date, period;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u sales_estimate sales-estimate-" date ".csv")))
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

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add sales_estimate; "
                       "/usr/local/bin/dolt commit -m 'sales_estimate " (end-date) " update'; /usr/local/bin/dolt push"))
