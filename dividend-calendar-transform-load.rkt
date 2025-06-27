#lang racket/base

(require db
         gregor
         json
         racket/cmdline
         racket/list
         racket/port
         racket/sequence
         racket/string
         threading)

(define base-folder (make-parameter "/var/tmp/zacks/dividend-calendar"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Dividend Calendar base folder. Defaults to /var/tmp/zacks/dividend-calendar"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "Dividend Calendar folder date. Defaults to today"
                         (folder-date (iso8601->date date))]
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

; we clean up the future part of the table in case dividend dates have been shifted
(query-exec dbc "
delete from
  zacks.dividend_calendar
where
  ex_date >= $1::text::date;
"
            (~t (folder-date) "yyyy-MM-dd"))

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".json")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [date-of-dividend (string-replace (string-replace file-name (path->string (current-directory)) "") ".json" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to parse "
                                                                      file-name
                                                                      " for date "
                                                                      date-of-dividend))
                                       (displayln e))])
            (~> (port->string in)
                (regexp-replace* #rx"<.*?>" _ "")
                (regexp-replace* #rx"[A-Z\\.]+ Quick Quote" _ "")
                (string-replace _ "window.app_data = " "")
                (string->jsexpr _)
                (hash-ref _ 'data)
                (for-each (λ (ticker-div-list)
                            (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to insert "
                                                                                        (first ticker-div-list)
                                                                                        " for date "
                                                                                        date-of-dividend))
                                                         (displayln e)
                                                         (rollback-transaction dbc))])
                              (start-transaction dbc)
                              ; if we have a record from last week for this symbol, move it forward
                              (query-exec dbc "
delete from
  zacks.dividend_calendar
where
  act_symbol = $1 and
  ex_date >= $2::text::date - '7 days'::interval
"
                                          (first ticker-div-list)
                                          (~t (folder-date) "yyyy-MM-dd"))
                              (query-exec dbc "
insert into zacks.dividend_calendar (
  act_symbol,
  ex_date,
  amount,
  payable_date
) values (
  $1,
  $2::text::date,
  $3::text::decimal,
  case
    when $4 = '--' then null
    else $4::text::date
  end
) on conflict do nothing;
"
                                          (first ticker-div-list)
                                          (sixth ticker-div-list)
                                          (string-replace (fourth ticker-div-list) "$" "")
                                          (eighth ticker-div-list))
                              (commit-transaction dbc))) _))))))))

; vacuum (garbage collect) and reindex table as we deleted from it earlier
(query-exec dbc "
vacuum full freeze analyze zacks.dividend_calendar;
")

(query-exec dbc "
reindex table zacks.dividend_calendar;
")

(disconnect dbc)
