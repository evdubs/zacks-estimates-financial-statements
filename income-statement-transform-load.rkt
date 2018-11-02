#lang racket/base

(require db
         html-parsing
         racket/cmdline
         racket/list
         racket/sequence
         racket/string
         srfi/19 ; Time Data Types and Procedures
         sxml
         threading)

(define (income-statement-figure xexp #:period period #:date date #:entry entry)
  (let*-values ([(section-id period-offset) (case period
                                              ['annual (values "annual_income_statement" 1)]
                                              ['quarterly (values "quarterly_income_statement" 0)])]
                [(col) (case date
                         ['most-recent 2]
                         ['second-most-recent 3]
                         ['third-most-recent 4]
                         ['fourth-most-recent 5]
                         ['fifth-most-recent 6])]
                [(table-id row thead-tbody th-td) (case entry
                                                    ['date (values 1 1 `thead `th)]
                                                    ['sales (values 1 1 `tbody `td)]
                                                    ['cost-of-goods (values 1 2 `tbody `td)]
                                                    ['gross-profit (values 1 3 `tbody `td)]
                                                    ['selling-administrative-depreciation-amortization-expenses (values 1 4 `tbody `td)]
                                                    ['income-after-depreciation-and-amortization (values 1 5 `tbody `td)]
                                                    ['non-operating-income (values 1 6 `tbody `td)]
                                                    ['interest-expense (values 1 7 `tbody `td)]
                                                    ['pretax-income (values 1 8 `tbody `td)]
                                                    ['income-taxes (values 1 9 `tbody `td)]
                                                    ['minority-interest (values 1 10 `tbody `td)]
                                                    ['investment-gains (values 1 11 `tbody `td)]
                                                    ['other-income (values 1 12 `tbody `td)]
                                                    ['income-from-continuing-operations (values 1 13 `tbody `td)]
                                                    ['extras-and-discontinued-operations (values 1 14 `tbody `td)]
                                                    ['net-income (values 1 15 `tbody `td)]
                                                    ['income-before-depreciation-and-amortization (values 2 1 `tbody `td)]
                                                    ['depreciation-and-amortization (values 2 2 `tbody `td)]
                                                    ['average-shares (values (+ 2 period-offset) 1 `tbody `td)]
                                                    ['diluted-eps-before-non-recurring-items (values (+ 2 period-offset) 2 `tbody `td)]
                                                    ['diluted-net-eps (values (+ 2 period-offset) 3 `tbody `td)])])
    (~>
     ; (define in-file (open-input-file "/var/tmp/zacks/income-statement/2018-05-08/AA.income-statement.html"))
     ; (define in-xexp (html->xexp in-file))
     ; (webscraperhelper '(td (@ (class "alpha")) "Sales") in-xexp)
     ((sxpath `(// (div (@ (equal? (id ,section-id))))
                   (table ,table-id) ,thead-tbody (tr ,row) (,th-td ,col))) xexp)
     (flatten _)
     (last _)
     (string-trim _)
     (string-replace _ "," ""))))

(define base-folder (make-parameter "/var/tmp/zacks/income-statement"))

(define folder-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket income-statement-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Zacks income statement base folder. Defaults to /var/tmp/zacks/income-statement"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "Zacks income statement folder date. Defaults to today"
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

(define insert-counter 0)
(define insert-success-counter 0)
(define insert-failure-counter 0)

(parameterize ([current-directory (string-append (base-folder) "/" (date->string (folder-date) "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".income-statement.html")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) ".income-statement.html" "")])
      (call-with-input-file file-name
        (λ (in) (let ([xexp (html->xexp in)])
                  (for-each (λ (period-date)
                              (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                                          ticker-symbol
                                                                                          " for date "
                                                                                          (date->string (folder-date) "~1")))
                                                           (displayln ((error-value->string-handler) e 1000))
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
      sales = $4::text::decimal * 1e6 and
      cost_of_goods = $5::text::decimal * 1e6 and
      gross_profit = $6::text::decimal * 1e6 and
      selling_administrative_depreciation_amortization_expenses = $7::text::decimal * 1e6 and
      income_after_depreciation_and_amortization = $8::text::decimal * 1e6 and
      non_operating_income = $9::text::decimal * 1e6 and
      interest_expense = $10::text::decimal * 1e6 and
      pretax_income = $11::text::decimal * 1e6 and
      income_taxes = $12::text::decimal * 1e6 and
      minority_interest = $13::text::decimal * 1e6 and
      investment_gains = $14::text::decimal * 1e6 and
      other_income = $15::text::decimal * 1e6 and
      income_from_continuing_operations = $16::text::decimal * 1e6 and
      extras_and_discontinued_operations = $17::text::decimal * 1e6 and
      net_income = $18::text::decimal * 1e6 and
      case $3
        when 'annual' then income_before_depreciation_and_amortization = $19::text::decimal * 1e6
        when 'quarterly' then income_before_depreciation_and_amortization is null
      end and
      case $3
        when 'annual' then depreciation_and_amortization = $20::text::decimal * 1e6
        when 'quarterly' then depreciation_and_amortization is null
      end and
      average_shares = $21::text::decimal * 1e6 and
      diluted_eps_before_non_recurring_items = $22::text::decimal and
      diluted_net_eps = $23::text::decimal
    ) as sni
  from
    zacks.income_statement
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
insert into zacks.income_statement
(
  act_symbol,
  date,
  period,
  sales,
  cost_of_goods,
  gross_profit,
  selling_administrative_depreciation_amortization_expenses,
  income_after_depreciation_and_amortization,
  non_operating_income,
  interest_expense,
  pretax_income,
  income_taxes,
  minority_interest,
  investment_gains,
  other_income,
  income_from_continuing_operations,
  extras_and_discontinued_operations,
  net_income,
  income_before_depreciation_and_amortization,
  depreciation_and_amortization,
  average_shares,
  diluted_eps_before_non_recurring_items,
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
  case $3
    when 'annual' then $19::text::decimal * 1e6
    when 'quarterly' then null
  end,
  case $3
    when 'annual' then $20::text::decimal * 1e6
    when 'quarterly' then null
  end,
  $21::text::decimal * 1e6,
  $22::text::decimal,
  $23::text::decimal
) on conflict (act_symbol, date, period) do nothing;
"
                                            ticker-symbol
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'date)
                                            (symbol->string (first period-date))
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'sales)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'cost-of-goods)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'gross-profit)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'selling-administrative-depreciation-amortization-expenses)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'income-after-depreciation-and-amortization)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'non-operating-income)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'interest-expense)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'pretax-income)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'income-taxes)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'minority-interest)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'investment-gains)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'other-income)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'income-from-continuing-operations)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'extras-and-discontinued-operations)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'net-income)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'income-before-depreciation-and-amortization)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'depreciation-and-amortization)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'average-shares)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'diluted-eps-before-non-recurring-items)
                                            (income-statement-figure xexp #:period (first period-date) #:date (second period-date) #:entry 'diluted-net-eps))
                                (commit-transaction dbc)
                                (set! insert-success-counter (add1 insert-success-counter))))
                            (cartesian-product (list 'annual 'quarterly)
                                               (list 'fifth-most-recent 'fourth-most-recent 'third-most-recent 'second-most-recent 'most-recent)))))))))

(disconnect dbc)

(displayln (string-append "Attempted to insert " (number->string insert-counter) " rows. "
                          (number->string insert-success-counter) " were successful. "
                          (number->string insert-failure-counter) " failed."))
