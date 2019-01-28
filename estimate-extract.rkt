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

(define (download-estimates symbol)
  (make-directory* (string-append "/var/tmp/zacks/estimates/" (date->string (current-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/zacks/estimates/" (date->string (current-date) "~1") "/" symbol ".detailed-estimates.html")
    (λ (out) (with-handlers ([exn:fail:network
                              (λ (errno error)
                                (displayln (string-append "Encountered network error for " symbol))
                                (displayln ((error-value->string-handler) error 1000)))])
               (~> (string-append "https://www.zacks.com/stock/quote/" symbol "/detailed-estimates")
                   (string->url _)
                   (get-pure-port _)
                   (copy-port _ out))))
    #:exists 'replace))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket estimate-extract.rkt"
 #:once-each
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
  last_seen = (select max(last_seen) from nasdaq.symbol)
order by
  act_symbol;
"))

(disconnect dbc)

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-estimates (first l)))
                                                          (second l)))
                            (map list symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
