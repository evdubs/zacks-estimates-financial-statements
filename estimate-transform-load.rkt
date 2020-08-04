#lang racket/base

(require db
         gregor
         html-parsing
         racket/cmdline
         racket/list
         racket/sequence
         racket/string
         sxml
         threading)

(define (rank xexp)
  (let* ([zrank-path (sxpath '(html (body (@ (equal? (id "home"))))
                                    (div (@ (equal? (id "main_content"))))
                                    (div (@ (equal? (id "right_content"))))
                                    (section (@ (equal? (id "quote_ribbon_v2"))))
                                    (div 2) (div 1) p))]
         [zrank-node (zrank-path xexp)])
    (substring (string-trim (second ((select-kids (ntype?? '*text*)) zrank-node))) 2)))

(define (style-score xexp #:style style-type)
  (let* ([n (case style-type
              ['value 1]
              ['growth 2]
              ['momentum 3]
              ['vgm 4])]
         [x (cond
              ; Reports downloaded before 2018-10-07 used a slightly different sxpath
              ; The following code might be helpful for finding these differences in the future:
              ; 
              ; (define in-file (open-input-file "/var/tmp/zacks/estimates/2017-10-26/AA.detailed-estimates.html"))
              ; (define in-xexp (html->xexp in-file))
              ; (webscraperhelper '(td (@ (class "alpha")) "Down Last 60 Days") in-xexp)
              [(date<? (folder-date) (date 2018 10 7))
               ((sxpath `(html (body (@ (equal? (id "home"))))
                               (div (@ (equal? (id "main_content"))))
                               (div (@ (equal? (id "right_content"))))
                               (section (@ (equal? (id "quote_ribbon_v2"))))
                               (div 2) (div 2) p (span ,n))) xexp)]
              [(date<? (folder-date) (date 2020 7 4))
               ((sxpath `(html (body (@ (equal? (id "home"))))
                               (div (@ (equal? (id "main_content"))))
                               (div (@ (equal? (id "right_content"))))
                               (section (@ (equal? (id "quote_ribbon_v2"))))
                               (div 2) (div 3) p (span ,n))) xexp)]
              ; Pipes between the style scores now show up as their own span element, so we have a hack to skip them
              [else
               ((sxpath `(html (body (@ (equal? (id "home"))))
                               (div (@ (equal? (id "main_content"))))
                               (div (@ (equal? (id "right_content"))))
                               (section (@ (equal? (id "quote_ribbon_v2"))))
                               (div 2) (div 3) p (span ,(- (* n 2) 1)))) xexp)])])
    (third (first x))))

(define (estimate-figure xexp #:section section #:period period #:entry entry)
  (let*-values ([(section-id first-second index-offset)
                 (case section
                   ['sales-estimates (values "detailed_earnings_estimates" first 0)]
                   ['eps-estimates (values "detailed_earnings_estimates" second 1)]
                   ['eps-revisions (values "agreement_estimate" first 0)]
                   ['eps-upside (values "quote_upside" first 0)]
                   ['eps-surprise (values "surprised_reported" first 0)])]
                [(col) (case period
                         ['current-quarter 2]
                         ['next-quarter 3]
                         ['current-year 4]
                         ['next-year 5]
                         ['last-quarter 2]
                         ['two-quarters-ago 3]
                         ['three-quarters-ago 4]
                         ['four-quarters-ago 5])]
                [(row thead-tbody th-td) (case entry
                                           ['consensus (values 1 `tbody `td)]
                                           ['count (values 2 `tbody `td)]
                                           ['recent (values 3 `tbody `td)]
                                           ['high (values (+ 3 index-offset) `tbody `td)]
                                           ['low (values (+ 4 index-offset) `tbody `td)]
                                           ['year-ago (values (+ 5 index-offset) `tbody `td)]
                                           ['up-7 (values 1 `tbody `td)]
                                           ['up-30 (values 2 `tbody `td)]
                                           ['up-60 (values 3 `tbody `td)]
                                           ['down-7 (values 4 `tbody `td)]
                                           ['down-30 (values 5 `tbody `td)]
                                           ['down-60 (values 6 `tbody `td)]
                                           ['most-accurate (values 1 `tbody `td)]
                                           ['date (values 1 `thead `th)]
                                           ['reported (values 1 `tbody `td)]
                                           ['estimate (values 2 `tbody `td)])])
    (~> ((sxpath `(// (section (@ (equal? (id ,section-id))))
                      table ,thead-tbody (tr ,row) (,th-td ,col))) xexp)
        (first-second _)
        (flatten _)
        (last _)
        (string-trim _)
        (string-replace _ "T" "e12")
        (string-replace _ "B" "e9")
        (regexp-replace #rx"^M$" _ "NA") ; no clue what the "M" value means in the estimate cells, but we set it to NA
        (string-replace _ "M" "e6")
        (string-replace _ "(" "")
        (string-replace _ ")" ""))))

(define base-folder (make-parameter "/var/tmp/zacks/estimates"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket estimate-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Zacks estimates base folder. Defaults to /var/tmp/zacks/estimates"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "Zacks estimates folder date. Defaults to today"
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

(define insert-counter 0)
(define insert-success-counter 0)
(define insert-failure-counter 0)

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".detailed-estimates.html")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [ticker-symbol (string-replace (string-replace file-name (path->string (current-directory)) "") ".detailed-estimates.html" "")])
      (call-with-input-file file-name
        (λ (in) (let ([xexp (html->xexp in)])
                  (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                              ticker-symbol
                                                                              " for date "
                                                                              (~t (folder-date) "yyyy-MM-dd")))
                                               (displayln ((error-value->string-handler) e 1000))
                                               (rollback-transaction dbc)
                                               (set! insert-failure-counter (add1 insert-failure-counter)))])
                    (set! insert-counter (add1 insert-counter))
                    (start-transaction dbc)
                    (query-exec dbc "
insert into zacks.rank_score
(
  act_symbol,
  date,
  rank,
  value,
  growth,
  momentum,
  vgm
) values (
  $1,
  $2::text::date,
  $3::text::zacks.rank,
  $4::text::zacks.score,
  $5::text::zacks.score,
  $6::text::zacks.score,
  $7::text::zacks.score
) on conflict (act_symbol, date) do nothing;
"
                                ticker-symbol
                                (~t (folder-date) "yyyy-MM-dd")
                                (rank xexp)
                                (style-score xexp #:style 'value)
                                (style-score xexp #:style 'growth)
                                (style-score xexp #:style 'momentum)
                                (style-score xexp #:style 'vgm))
                    (for-each (λ (period)
                                (query-exec dbc "
insert into zacks.sales_estimate
(
  act_symbol,
  date,
  period,
  period_end_date,
  consensus,
  count,
  high,
  low,
  year_ago
) values (
  $1,
  $2::text::date,
  case $3
    when 'current-quarter' then 'Current Quarter'::zacks.estimate_period
    when 'next-quarter' then 'Next Quarter'::zacks.estimate_period
    when 'current-year' then 'Current Year'::zacks.estimate_period
    when 'next-year' then 'Next Year'::zacks.estimate_period
  end,
  to_date($4, 'DD/MM/YYYY') + interval '1 month' - interval '1 day',
  case $5
    when 'NA' then NULL
    else $5::decimal
  end,
  case $6
    when 'NA' then NULL
    else $6::smallint
  end,
  case $7
    when 'NA' then NULL
    else $7::decimal
  end,
  case $8
    when 'NA' then NULL
    else $8::decimal
  end,
  case $9
    when 'NA' then NULL
    else $9::decimal
  end
) on conflict (act_symbol, date, period) do nothing;
"
                                            ticker-symbol
                                            (~t (folder-date) "yyyy-MM-dd")
                                            (symbol->string period)
                                            ; Use eps-estimates date as we sometimes have worse coverage with sales estimates than eps estimates
                                            (string-append "01/" (estimate-figure xexp #:section 'eps-estimates #:period period #:entry 'date))
                                            (estimate-figure xexp #:section 'sales-estimates #:period period #:entry 'consensus)
                                            (estimate-figure xexp #:section 'sales-estimates #:period period #:entry 'count)
                                            (estimate-figure xexp #:section 'sales-estimates #:period period #:entry 'high)
                                            (estimate-figure xexp #:section 'sales-estimates #:period period #:entry 'low)
                                            (estimate-figure xexp #:section 'sales-estimates #:period period #:entry 'year-ago))
                                (query-exec dbc "
insert into zacks.eps_estimate
(
  act_symbol,
  date,
  period,
  period_end_date,
  consensus,
  count,
  recent,
  high,
  low,
  year_ago
) values (
  $1,
  $2::text::date,
  case $3
    when 'current-quarter' then 'Current Quarter'::zacks.estimate_period
    when 'next-quarter' then 'Next Quarter'::zacks.estimate_period
    when 'current-year' then 'Current Year'::zacks.estimate_period
    when 'next-year' then 'Next Year'::zacks.estimate_period
  end,
  to_date($4, 'DD/MM/YYYY') + interval '1 month' - interval '1 day',
  case $5
    when 'NA' then NULL
    else $5::decimal
  end,
  case $6
    when 'NA' then NULL
    else $6::smallint
  end,
  case $7
    when 'NA' then NULL
    else $7::decimal
  end,
  case $8
    when 'NA' then NULL
    else $8::decimal
  end,
  case $9
    when 'NA' then NULL
    else $9::decimal
  end,
  case $10
    when 'NA' then NULL
    else $10::decimal
  end
) on conflict (act_symbol, date, period) do nothing;
"
                                            ticker-symbol
                                            (~t (folder-date) "yyyy-MM-dd")
                                            (symbol->string period)
                                            (string-append "01/" (estimate-figure xexp #:section 'eps-estimates #:period period #:entry 'date))
                                            (estimate-figure xexp #:section 'eps-estimates #:period period #:entry 'consensus)
                                            (estimate-figure xexp #:section 'eps-estimates #:period period #:entry 'count)
                                            (estimate-figure xexp #:section 'eps-estimates #:period period #:entry 'recent)
                                            (estimate-figure xexp #:section 'eps-estimates #:period period #:entry 'high)
                                            (estimate-figure xexp #:section 'eps-estimates #:period period #:entry 'low)
                                            (estimate-figure xexp #:section 'eps-estimates #:period period #:entry 'year-ago))
                                (query-exec dbc "
insert into zacks.eps_revision
(
  act_symbol,
  date,
  period,
  period_end_date,
  up_7,
  up_30,
  up_60,
  down_7,
  down_30,
  down_60
) values (
  $1,
  $2::text::date,
  case $3
    when 'current-quarter' then 'Current Quarter'::zacks.estimate_period
    when 'next-quarter' then 'Next Quarter'::zacks.estimate_period
    when 'current-year' then 'Current Year'::zacks.estimate_period
    when 'next-year' then 'Next Year'::zacks.estimate_period
  end,
  to_date($4, 'DD/MM/YYYY') + interval '1 month' - interval '1 day',
  case $5
    when 'NA' then NULL
    else $5::smallint
  end,
  case $6
    when 'NA' then NULL
    else $6::smallint
  end,
  case $7
    when 'NA' then NULL
    else $7::smallint
  end,
  case $8
    when 'NA' then NULL
    else $8::smallint
  end,
  case $9
    when 'NA' then NULL
    else $9::smallint
  end,
  case $10
    when 'NA' then NULL
    else $10::smallint
  end
) on conflict (act_symbol, date, period) do nothing;
"
                                            ticker-symbol
                                            (~t (folder-date) "yyyy-MM-dd")
                                            (symbol->string period)
                                            (string-append "01/" (estimate-figure xexp #:section 'eps-revisions #:period period #:entry 'date))
                                            (estimate-figure xexp #:section 'eps-revisions #:period period #:entry 'up-7)
                                            (estimate-figure xexp #:section 'eps-revisions #:period period #:entry 'up-30)
                                            (estimate-figure xexp #:section 'eps-revisions #:period period #:entry 'up-60)
                                            (estimate-figure xexp #:section 'eps-revisions #:period period #:entry 'down-7)
                                            (estimate-figure xexp #:section 'eps-revisions #:period period #:entry 'down-30)
                                            (estimate-figure xexp #:section 'eps-revisions #:period period #:entry 'down-60))
                                (query-exec dbc "
insert into zacks.eps_perception
(
  act_symbol,
  date,
  period,
  period_end_date,
  most_accurate
) values (
  $1,
  $2::text::date,
  case $3
    when 'current-quarter' then 'Current Quarter'::zacks.estimate_period
    when 'next-quarter' then 'Next Quarter'::zacks.estimate_period
    when 'current-year' then 'Current Year'::zacks.estimate_period
    when 'next-year' then 'Next Year'::zacks.estimate_period
  end,
  to_date($4, 'DD/MM/YYYY') + interval '1 month' - interval '1 day',
  case $5
    when 'NA' then NULL
    else $5::decimal
  end
) on conflict (act_symbol, date, period) do nothing;
"
                                            ticker-symbol
                                            (~t (folder-date) "yyyy-MM-dd")
                                            (symbol->string period)
                                            (string-append "01/" (estimate-figure xexp #:section 'eps-upside #:period period #:entry 'date))
                                            (estimate-figure xexp #:section 'eps-upside #:period period #:entry 'most-accurate)))
                              (list 'current-quarter 'next-quarter 'current-year 'next-year))
                    (for-each (λ (quarter)
                                (query-exec dbc "
insert into zacks.eps_history
(
  act_symbol,
  date,
  period_end_date,
  reported,
  estimate
) values (
  $1,
  $2::text::date,
  to_date($3, 'DD/MM/YYYY') + interval '1 month' - interval '1 day',
  case $4
    when 'NA' then NULL
    else $4::decimal
  end,
  case $5
    when 'NA' then NULL
    else $5::decimal
  end
) on conflict (act_symbol, date, period_end_date) do nothing;
"
                                            ticker-symbol
                                            (~t (folder-date) "yyyy-MM-dd")
                                            (string-append "01/" (estimate-figure xexp #:section 'eps-surprise #:period quarter #:entry 'date))
                                            (estimate-figure xexp #:section 'eps-surprise #:period quarter #:entry 'reported)
                                            (estimate-figure xexp #:section 'eps-surprise #:period quarter #:entry 'estimate)))
                              (list 'last-quarter 'two-quarters-ago 'three-quarters-ago 'four-quarters-ago))
                    (commit-transaction dbc)
                    (set! insert-success-counter (add1 insert-success-counter)))))))))

(disconnect dbc)

(displayln (string-append "Attempted to insert " (number->string insert-counter) " rows. "
                          (number->string insert-success-counter) " were successful. "
                          (number->string insert-failure-counter) " failed."))
