#lang racket/base

(require db
         net/url
         racket/cmdline
         racket/file
         racket/list
         racket/port
         srfi/19 ; Time Data Types and Procedures
         tasks
         threading)

(define (download-income-statement symbol)
  (make-directory* (string-append "/var/tmp/zacks/income-statement/" (date->string (current-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/zacks/income-statement/" (date->string (current-date) "~1") "/" symbol ".income-statement.html")
    (λ (out) (~> (string-append "https://www.zacks.com/stock/quote/" symbol "/income-statement")
                 (string->url _)
                 (get-pure-port _)
                 (copy-port _ out)))
    #:exists 'replace))

(define (download-balance-sheet symbol)
  (make-directory* (string-append "/var/tmp/zacks/balance-sheet/" (date->string (current-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/zacks/balance-sheet/" (date->string (current-date) "~1") "/" symbol ".balance-sheet.html")
    (λ (out) (~> (string-append "https://www.zacks.com/stock/quote/" symbol "/balance-sheet")
                 (string->url _)
                 (get-pure-port _)
                 (copy-port _ out)))
    #:exists 'replace))

(define (download-cash-flow-statement symbol)
  (make-directory* (string-append "/var/tmp/zacks/cash-flow-statement/" (date->string (current-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/zacks/cash-flow-statement/" (date->string (current-date) "~1") "/" symbol ".cash-flow-statement.html")
    (λ (out) (~> (string-append "https://www.zacks.com/stock/quote/" symbol "/cash-flow-statements")
                 (string->url _)
                 (get-pure-port _)
                 (copy-port _ out)))
    #:exists 'replace))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(define first-symbol (make-parameter ""))

(define last-symbol (make-parameter ""))

(command-line
 #:program "racket financial-statement-extract.rkt"
 #:once-each
 [("-f" "--first-symbol") first
                          "First symbol to query. Defaults to nothing"
                          (first-symbol first)]
 [("-l" "--last-symbol") last
                         "Last symbol to query. Defaults to nothing"
                         (last-symbol last)]
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

(define symbols (query-list dbc "
select
  act_symbol
from
  nasdaq.symbol
where
  is_etf = false and
  is_test_issue = false and
  is_next_shares = false and
  security_name !~ 'ETN' and
  nasdaq_symbol !~ '[-\\$\\+\\*#!@%\\^=~]' and
  case when nasdaq_symbol ~ '[A-Z]{4}[L-Z]'
    then security_name !~ '(Note|Preferred|Right|Unit|Warrant)'
    else true
  end and
  last_seen = (select max(last_seen) from nasdaq.symbol) and
  case when $1 != ''
    then act_symbol >= $1
    else true
  end and
  case when $2 != ''
    then act_symbol <= $2
    else true
  end
order by
  act_symbol;
"
                            (first-symbol)
                            (last-symbol)))

(disconnect dbc)

(define delay-interval 15)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-income-statement (first l)))
                                                          (second l))
                              (schedule-delayed-task (λ () (download-balance-sheet (first l)))
                                                     (+ 5 (second l)))
                              (schedule-delayed-task (λ () (download-cash-flow-statement (first l)))
                                                     (+ 10 (second l))))
                            (map list symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
