#lang racket

(require db)
(require html-parsing)
(require racket/cmdline)
(require srfi/19) ; Time Data Types and Procedures
(require sxml)
(require threading)

(define (cash-flow-statement-figure xexp #:section section #:date date #:entry entry)
  (let*-values ([(section-id) (case section
                                ['cash-flow "cash_flow_operation"]
                                ['uses-of-funds "cash_flow_use"])]
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
    (~> ((sxpath `(html (body (@ (equal? (id "home"))))
                        (div (@ (equal? (id "main_content"))))
                        (div (@ (equal? (id "right_content"))))
                        (div (@ (equal? (class "quote_body_full"))))
                        (section (@ (equal? (id ,section-id))))
                        table ,thead-tbody (tr ,row) (,th-td ,col))) xexp)
        (flatten _)
        (last _)
        (string-trim _)
        (string-replace _ "," ""))))

(define base-folder (make-parameter "/var/tmp/zacks/cash-flow-statement"))

(define folder-date (make-parameter (current-date)))

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
                         (folder-date (string->date date "~Y-~m-~d"))]
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

(parameterize ([current-directory (string-append (base-folder) "/" (date->string (folder-date) "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".cash-flow-statement.html")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) ".cash-flow-statement.html" "")])
      (call-with-input-file file-name
        (λ (in) (let ([xexp (html->xexp in)])
                  (for-each (λ (date)
                              (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                                          ticker-symbol
                                                                                          " for date "
                                                                                          (date->string (folder-date) "~1")))
                                                           (displayln ((error-value->string-handler) e 1000))
                                                           (rollback-transaction dbc))])
                                (start-transaction dbc)
                                (query-exec dbc "
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
  other_financing_activites,
  net_cash_from_financing_activites,
  effect_of_exchange_rate_changes,
  net_change_in_cash_and_equivalents,
  cash_at_beginning_of_period,
  cash_at_end_of_period,
  diluted_net_eps
) values (
  $1,
  $2::text::date,
  'Year'::zacks.statement_period,
  $3::text::decimal * 1e6,
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
  $24::text::decimal
) on conflict (act_symbol, date, period) do nothing;
"
                                            ticker-symbol
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'date)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'net-income)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'depreciation-amortization-and-depletion)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'net-change-from-assets)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'net-cash-from-discontinued-operations)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'other-operating-activities)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'net-cash-from-operating-activities)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'property-and-equipment)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'acquisition-of-subsidiaries)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'investments)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'other-investing-activities)
                                            (cash-flow-statement-figure xexp #:section 'cash-flow #:date date #:entry 'net-cash-from-investing-activities)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'issuance-of-capital-stock)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'issuance-of-debt)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'increase-short-term-debt)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'payment-of-dividends-and-other-distributions)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'other-financing-activities)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'net-cash-from-financing-activities)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'effect-of-exchange-rate-changes)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'net-change-in-cash-and-equivalents)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'cash-at-beginning-of-period)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'cash-at-end-of-period)
                                            (cash-flow-statement-figure xexp #:section 'uses-of-funds #:date date #:entry 'diluted-net-eps))
                                (commit-transaction dbc)))
                            (list 'most-recent 'second-most-recent 'third-most-recent 'fourth-most-recent 'fifth-most-recent))))))))

(disconnect dbc)
