#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/tmp/dolt/earnings"))

(define as-of-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dump-dolt-calendar.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Base dolt folder. Defaults to /var/tmp/dolt/earnings"
                         (base-folder folder)]
 [("-d" "--date") date
                  "Final date for history retrieval. Defaults to today"
                  (as-of-date date)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

; earnings-calendar
(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt sql -q \"delete from earnings_calendar where date >= date_sub('" (as-of-date) "', interval 7 day)\";"))

(define earnings-calendar-file (string-append (base-folder) "/earnings-calendar-" (as-of-date) ".csv"))

(call-with-output-file* earnings-calendar-file
  (λ (out)
    (displayln "act_symbol,date,when" out)
    (for-each (λ (row)
                (displayln (string-join (vector->list row) ",") out))
              (query-rows dbc "
select
  act_symbol::text,
  date::text,
  coalesce(\"when\"::text, '')
from
  zacks.earnings_calendar
where
  date >= $1::text::date - '7 days'::interval;
"
                          (as-of-date))))
  #:exists 'replace)

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u earnings_calendar earnings-calendar-" (as-of-date) ".csv"))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add earnings_calendar; "
                       "/usr/local/bin/dolt commit -m 'earnings_calendar " (as-of-date) " update'; /usr/local/bin/dolt push --silent"))
