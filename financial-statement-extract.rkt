#lang racket

(require db)
(require net/url)
(require srfi/19) ; Time Data Types and Procedures
(require tasks)
(require threading)

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

(display "db user [user]: ")
(flush-output)
(define db-user
  (let ([db-user-input (read-line)])
    (if (equal? "" db-user-input) "user"
        db-user-input)))

(display "db name [local]: ")
(flush-output)
(define db-name
  (let ([db-name-input (read-line)])
    (if (equal? "" db-name-input) "local"
        db-name-input)))

(display "db pass []: ")
(flush-output)
(define db-pass (read-line))

(define dbc (postgresql-connect #:user db-user #:database db-name #:password db-pass))

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
  end
order by
  act_symbol;
"))

(disconnect dbc)

(define delay-interval 20)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-income-statement (first l))
                                                            (download-balance-sheet (first l))
                                                            (download-cash-flow-statement (first l)))
                                                          (second l)))
                            (map list symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
