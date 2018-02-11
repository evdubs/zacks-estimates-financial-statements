#lang racket

(require db)
(require html-parsing)
(require racket/cmdline)
(require srfi/19) ; Time Data Types and Procedures
(require sxml)
(require threading)

(define (balance-sheet-figure xexp #:section section #:period period #:date date #:entry entry)
  (let*-values ([(table-id) (case section
                              ['assets 1]
                              ['liabilities 2]
                              ['equity 3])]
                [(section-id) (case period
                                ['annual "annual_income_statement"]
                                ['quarterly "quarterly_income_statement"])]
                [(col) (case date
                         ['most-recent 2]
                         ['second-most-recent 3]
                         ['third-most-recent 4]
                         ['fourth-most-recent 5]
                         ['fifth-most-recent 6])]
                [(row thead-tbody th-td) (case entry
                                           ['date (values 1 `thead `th)]
                                           ['cash-and-equivalents (values 2 `tbody `td)]
                                           ['receivables (values 3 `tbody `td)]
                                           ['notes-receivable (values 4 `tbody `td)]
                                           ['inventories (values 5 `tbody `td)]
                                           ['other-current-assets (values 6 `tbody `td)]
                                           ['total-current-assets (values 7 `tbody `td)]
                                           ['net-property-and-equipment (values 8 `tbody `td)]
                                           ['investments-and-advances (values 9 `tbody `td)]
                                           ['other-non-current-assets (values 10 `tbody `td)]
                                           ['deferred-charges (values 11 `tbody `td)]
                                           ['intangibles (values 12 `tbody `td)]
                                           ['deposits-and-other-assets (values 13 `tbody `td)]
                                           ['total-assets (values 14 `tbody `td)]
                                           ['notes-payable (values 1 `tbody `td)]
                                           ['accounts-payable (values 2 `tbody `td)]
                                           ['current-portion-long-term-debt (values 3 `tbody `td)]
                                           ['current-portion-capital-leases (values 4 `tbody `td)]
                                           ['accrued-expenses (values 5 `tbody `td)]
                                           ['income-taxes-payable (values 6 `tbody `td)]
                                           ['other-current-liabilities (values 7 `tbody `td)]
                                           ['total-current-liabilities (values 8 `tbody `td)]
                                           ['mortgages (values 9 `tbody `td)]
                                           ['deferred-taxes-or-income (values 10 `tbody `td)]
                                           ['convertible-debt (values 11 `tbody `td)]
                                           ['long-term-debt (values 12 `tbody `td)]
                                           ['non-current-capital-leases (values 13 `tbody `td)]
                                           ['other-non-current-liabilities (values 14 `tbody `td)]
                                           ['minority-interest (values 15 `tbody `td)]
                                           ['total-liabilities (values 16 `tbody `td)]
                                           ['preferred-stock (values 1 `tbody `td)]
                                           ['common-stock (values 2 `tbody `td)]
                                           ['capital-surplus (values 3 `tbody `td)]
                                           ['retained-earnings (values 4 `tbody `td)]
                                           ['other-equity (values 5 `tbody `td)]
                                           ['treasury-stock (values 6 `tbody `td)]
                                           ['total-equity (values 7 `tbody `td)]
                                           ['total-liabilities-and-equity (values 8 `tbody `td)]
                                           ['shares-outstanding (values 10 `tbody `td)]
                                           ['book-value-per-share (values 11 `tbody `td)])])
    (~> ((sxpath `(html (body (@ (equal? (id "home"))))
                        (div (@ (equal? (id "main_content"))))
                        (div (@ (equal? (id "right_content"))))
                        (div (@ (equal? (class "quote_body_full"))))
                        (section (@ (equal? (id "income_statements_tabs"))))
                        (div (@ (equal? (id ,section-id))))
                        (table ,table-id) ,thead-tbody (tr ,row) (,th-td ,col))) xexp)
        (flatten _)
        (last _)
        (string-trim _)
        (string-replace _ "," ""))))

(define base-folder (make-parameter "/var/tmp/zacks/balance-sheet"))

(define folder-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket balance-sheet-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Zacks balance sheet base folder. Defaults to /var/tmp/zacks/balance-sheet"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "Zacks balance sheet folder date. Defaults to today"
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
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".balance-sheet.html")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) ".balance-sheet.html" "")])
      (call-with-input-file file-name
        (λ (in) (let ([xexp (html->xexp in)])
                  (for-each (λ (period-date)
                              (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                                          ticker-symbol
                                                                                          " for date "
                                                                                          (date->string (folder-date) "~1")))
                                                           (displayln ((error-value->string-handler) e 1000))
                                                           (rollback-transaction dbc))])
                                (start-transaction dbc)
                                (query-exec dbc "
insert into zacks.balance_sheet_assets
(
  act_symbol,
  date,
  period,
  cash_and_equivalents,
  receivables,
  notes_receivable,
  inventories,
  other_current_assets,
  total_current_assets,
  net_property_and_equipment,
  investments_and_advances,
  other_non_current_assets,
  deferred_charges,
  intangibles,
  deposits_and_other_assets,
  total_assets
) values (
  $1,
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
  $16::text::decimal * 1e6
) on conflict (act_symbol, date, period) do nothing;
"
                                            ticker-symbol
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'date)
                                            (symbol->string (first period-date))
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'cash-and-equivalents)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'receivables)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'notes-receivable)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'inventories)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'other-current-assets)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'total-current-assets)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'net-property-and-equipment)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'investments-and-advances)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'other-non-current-assets)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'deferred-charges)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'intangibles)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'deposits-and-other-assets)
                                            (balance-sheet-figure xexp #:section 'assets #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'total-assets))
                                (query-exec dbc "
insert into zacks.balance_sheet_liabilities
(
  act_symbol,
  date,
  period,
  notes_payable,
  accounts_payable,
  current_portion_long_term_debt,
  current_portion_capital_leases,
  accrued_expenses,
  income_taxes_payable,
  other_current_liabilities,
  total_current_liabilities,
  mortgages,
  deferred_taxes_or_income,
  convertible_debt,
  long_term_debt,
  non_current_capital_leases,
  other_non_current_liabilities,
  minority_interest,
  total_liabilities
) values (
  $1,
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
  $19::text::decimal * 1e6
) on conflict (act_symbol, date, period) do nothing;
"
                                            ticker-symbol
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'date)
                                            (symbol->string (first period-date))
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'notes-payable)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'accounts-payable)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'current-portion-long-term-debt)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'current-portion-capital-leases)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'accrued-expenses)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'income-taxes-payable)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'other-current-liabilities)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'total-current-liabilities)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'mortgages)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'deferred-taxes-or-income)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'convertible-debt)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'long-term-debt)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'non-current-capital-leases)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'other-non-current-liabilities)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'minority-interest)
                                            (balance-sheet-figure xexp #:section 'liabilities #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'total-liabilities))
                                (query-exec dbc "
insert into zacks.balance_sheet_equity
(
  act_symbol,
  date,
  period,
  preferred_stock,
  common_stock,
  capital_surplus,
  retained_earnings,
  other_equity,
  treasury_stock,
  total_equity,
  total_liabilities_and_equity,
  shares_outstanding,
  book_value_per_share
) values (
  $1,
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
  $13::text::decimal
) on conflict (act_symbol, date, period) do nothing;
"
                                            ticker-symbol
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'date)
                                            (symbol->string (first period-date))
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'preferred-stock)
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'common-stock)
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'capital-surplus)
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'retained-earnings)
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'other-equity)
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'treasury-stock)
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'total-equity)
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'total-liabilities-and-equity)
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'shares-outstanding)
                                            (balance-sheet-figure xexp #:section 'equity #:period (first period-date) #:date (second period-date)
                                                                  #:entry 'book-value-per-share))
                                (commit-transaction dbc)))
                            (cartesian-product (list 'annual 'quarterly)
                                               (list 'most-recent 'second-most-recent 'third-most-recent 'fourth-most-recent 'fifth-most-recent)))))))))

(disconnect dbc)