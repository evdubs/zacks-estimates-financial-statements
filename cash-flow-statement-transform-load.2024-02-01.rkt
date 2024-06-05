#lang racket/base

(require db
         gregor
         gregor/period
         html-parsing
         racket/cmdline
         racket/list
         racket/sequence
         racket/string
         sxml
         threading)

(define (cash-flow-statement-figure xexp #:section section #:period period #:date date #:entry entry)
  (let*-values ([(table-id) (case section
                                ['cash-flow 1]
                                ['uses-of-funds 2])]
                [(section-id) (case period
                                ['annual "annual_cash_flow_statement"]
                                ['quarterly "quarterly_cash_flow_statement"])]
                [(col) (case date
                         ['most-recent 2]
                         ['second-most-recent 3]
                         ['third-most-recent 4]
                         ['fourth-most-recent 5]
                         ['fifth-most-recent 6])]
                [(row thead-tbody th-td) (case entry
                                           ['date (values 1 `thead `th)]
                                           ['net-income (values 2 `tbody `td)]
                                           ['depreciation-amortization-and-depletion (values 3 `tbody `td)]
                                           ['net-change-from-assets (values 4 `tbody `td)]
                                           ['net-cash-from-discontinued-operations (values 5 `tbody `td)]
                                           ['other-operating-activities (values 6 `tbody `td)]
                                           ['net-cash-from-operating-activities (values 7 `tbody `td)]
                                           ['property-and-equipment (values 8 `tbody `td)]
                                           ['acquisition-of-subsidiaries (values 9 `tbody `td)]
                                           ['investments (values 10 `tbody `td)]
                                           ['other-investing-activities (values 11 `tbody `td)]
                                           ['net-cash-from-investing-activities (values 12 `tbody `td)]
                                           ['issuance-of-capital-stock (values 1 `tbody `td)]
                                           ['issuance-of-debt (values 2 `tbody `td)]
                                           ['increase-short-term-debt (values 3 `tbody `td)]
                                           ['payment-of-dividends-and-other-distributions (values 4 `tbody `td)]
                                           ['other-financing-activities (values 5 `tbody `td)]
                                           ['net-cash-from-financing-activities (values 6 `tbody `td)]
                                           ['effect-of-exchange-rate-changes (values 7 `tbody `td)]
                                           ['net-change-in-cash-and-equivalents (values 8 `tbody `td)]
                                           ['cash-at-beginning-of-period (values 9 `tbody `td)]
                                           ['cash-at-end-of-period (values 10 `tbody `td)]
                                           ['diluted-net-eps (values 11 `tbody `td)])])
    (~>
     ; (define in-file (open-input-file "/var/tmp/zacks/cash-flow-statement/2018-05-08/AA.cash-flow-statement.html"))
     ; (define in-xexp (html->xexp in-file))
     ; (webscraperhelper '(td (@ (class "alpha")) "Investments") in-xexp)
     ((sxpath `(// (div (@ (equal? (id ,section-id)))) (div ,table-id)
                   table ,thead-tbody (tr ,row) (,th-td ,col))) xexp)
     (flatten _)
     (last _)
     (string-trim _)
     (string-replace _ "," ""))))

(define base-folder (make-parameter "/var/tmp/zacks/cash-flow-statement"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket cash-flow-statement-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Zacks cash flow statement base folder. Defaults to /var/tmp/zacks/cash-flow-statement"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "Zacks cash flow statement folder date. Defaults to today"
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
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".cash-flow-statement.html")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [ticker-symbol (string-replace (string-replace file-name (path->string (current-directory)) "") ".cash-flow-statement.html" "")])
      (call-with-input-file file-name
        (λ (in) (let ([xexp (html->xexp in)])
                  ; we assume that if we have an updated value that is very close to our extract date that the whole set of data is bad.
                  (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to extract a date from " ticker-symbol)))])
                    (cond [(< 15 (period-ref (date-period-between (parse-date (cash-flow-statement-figure xexp #:section 'cash-flow #:period 'annual
                                                                                                          #:date 'most-recent #:entry 'date)
                                                                              "M/dd/yyyy")
                                                                  (folder-date)
                                                                  (list 'days))
                                             'days))
                           (for-each (λ (period-date)
                                       (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                                                   ticker-symbol
                                                                                                   " for date "
                                                                                                   (~t (folder-date) "yyyy-MM-dd")))
                                                                     (displayln e)
                                                                     (rollback-transaction dbc)
                                                                     (set! insert-failure-counter (add1 insert-failure-counter)))])
                                         (set! insert-counter (add1 insert-counter))
                                         (start-transaction dbc)
                                         (query-exec dbc "
-- We use the common table expression below in order to check if we're receiving
-- bad values from Zacks. When the new fiscal year occurs, Zacks apparently has a
-- bug where values from the prior year are copied into the new year column. We
-- guard against that by checking data against the previous year's data and not
-- inserting it if it is exactly the same by trying to insert a NULL act_symbol.
-- A better way might be with a stored procedure that can return a meaningful error.
with should_not_insert as (
  select
    bool_and(
      net_income = $4::text::decimal * 1e6 and
      depreciation_amortization_and_depletion = $5::text::decimal * 1e6 and
      net_change_from_assets = $6::text::decimal * 1e6 and
      net_cash_from_discontinued_operations = $7::text::decimal * 1e6 and
      other_operating_activities = $8::text::decimal * 1e6 and
      net_cash_from_operating_activities = $9::text::decimal * 1e6 and
      property_and_equipment = $10::text::decimal * 1e6 and
      acquisition_of_subsidiaries = $11::text::decimal * 1e6 and
      investments = $12::text::decimal * 1e6 and
      other_investing_activities = $13::text::decimal * 1e6 and
      net_cash_from_investing_activities = $14::text::decimal * 1e6 and
      issuance_of_capital_stock = $15::text::decimal * 1e6 and
      issuance_of_debt = $16::text::decimal * 1e6 and
      increase_short_term_debt = $17::text::decimal * 1e6 and
      payment_of_dividends_and_other_distributions = $18::text::decimal * 1e6 and
      other_financing_activities = $19::text::decimal * 1e6 and
      net_cash_from_financing_activities = $20::text::decimal * 1e6 and
      effect_of_exchange_rate_changes = $21::text::decimal * 1e6 and
      net_change_in_cash_and_equivalents = $22::text::decimal * 1e6 and
      cash_at_beginning_of_period = $23::text::decimal * 1e6 and
      cash_at_end_of_period = $24::text::decimal * 1e6 and
      diluted_net_eps = $25::text::decimal
    ) as sni
  from
    zacks.cash_flow_statement
  where
    act_symbol = $1 and
    case $3
      when 'annual' then
        period = 'Year'::zacks.statement_period and
        date = $2::text::date - interval '1 year'
      when 'quarterly' then
        period = 'Quarter'::zacks.statement_period and
        date = $2::text::date + interval '1 day' - interval '3 months' - interval '1 day'
    end
)
insert into zacks.cash_flow_statement
(
  act_symbol,
  date,
  period,
  net_income,
  depreciation_amortization_and_depletion,
  net_change_from_assets,
  net_cash_from_discontinued_operations,
  other_operating_activities,
  net_cash_from_operating_activities,
  property_and_equipment,
  acquisition_of_subsidiaries,
  investments,
  other_investing_activities,
  net_cash_from_investing_activities,
  issuance_of_capital_stock,
  issuance_of_debt,
  increase_short_term_debt,
  payment_of_dividends_and_other_distributions,
  other_financing_activities,
  net_cash_from_financing_activities,
  effect_of_exchange_rate_changes,
  net_change_in_cash_and_equivalents,
  cash_at_beginning_of_period,
  cash_at_end_of_period,
  diluted_net_eps
) values (
  case (select sni from should_not_insert)
    when true then NULL
    else $1
  end,
  $2::text::date,
  case $3
    when 'annual' then 'Year'::zacks.statement_period
    when 'quarterly' then 'Quarter'::zacks.statement_period
  end,
  $4::text::decimal * 1e6,
  $5::text::decimal * 1e6,
  $6::text::decimal * 1e6,
  $7::text::decimal * 1e6,
  $8::text::decimal * 1e6,
  $9::text::decimal * 1e6,
  $10::text::decimal * 1e6,
  $11::text::decimal * 1e6,
  $12::text::decimal * 1e6,
  $13::text::decimal * 1e6,
  $14::text::decimal * 1e6,
  $15::text::decimal * 1e6,
  $16::text::decimal * 1e6,
  $17::text::decimal * 1e6,
  $18::text::decimal * 1e6,
  $19::text::decimal * 1e6,
  $20::text::decimal * 1e6,
  $21::text::decimal * 1e6,
  $22::text::decimal * 1e6,
  $23::text::decimal * 1e6,
  $24::text::decimal * 1e6,
  $25::text::decimal
) on conflict (act_symbol, date, period) do nothing;
"
                                                     ticker-symbol
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'date)
                                                     (symbol->string (first period-date))
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'net-income)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'depreciation-amortization-and-depletion)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'net-change-from-assets)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'net-cash-from-discontinued-operations)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'other-operating-activities)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'net-cash-from-operating-activities)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'property-and-equipment)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'acquisition-of-subsidiaries)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'investments)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'other-investing-activities)
                                                     (cash-flow-statement-figure xexp #:section 'cash-flow #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'net-cash-from-investing-activities)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'issuance-of-capital-stock)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'issuance-of-debt)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'increase-short-term-debt)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'payment-of-dividends-and-other-distributions)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'other-financing-activities)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'net-cash-from-financing-activities)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'effect-of-exchange-rate-changes)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'net-change-in-cash-and-equivalents)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'cash-at-beginning-of-period)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'cash-at-end-of-period)
                                                     (cash-flow-statement-figure xexp #:section 'uses-of-funds #:period (first period-date)
                                                                                 #:date (second period-date) #:entry 'diluted-net-eps))
                                         (commit-transaction dbc)
                                         (set! insert-success-counter (add1 insert-success-counter))))
                                     (cartesian-product (list 'annual 'quarterly)
                                                        (list 'fifth-most-recent 'fourth-most-recent 'third-most-recent 'second-most-recent 'most-recent)))]
                          [else (displayln (string-append "Skipping " ticker-symbol " as the data is most likely using the wrong date."))]))))))))

(disconnect dbc)

(displayln (string-append "Attempted to insert " (number->string insert-counter) " rows. "
                          (number->string insert-success-counter) " were successful. "
                          (number->string insert-failure-counter) " failed."))
