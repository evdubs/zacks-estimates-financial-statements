#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/tmp/dolt/earnings"))

(define start-date (make-parameter (~t (-days (today) 250) "yyyy-MM-dd")))

(define end-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dump-dolt-statements.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Base dolt folder. Defaults to /var/tmp/dolt/earnings"
                         (base-folder folder)]
 [("-e" "--end-date") end
                      "Final date for history retrieval. Defaults to today"
                      (end-date end)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-s" "--start-date") start
                        "Earliest date for history retrieval. Defaults to today minus 250 days"
                        (start-date start)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

; balance-sheet-assets
(for-each (λ (date)
            (define balance-sheet-assets-file (string-append (base-folder) "/balance-sheet-assets-" date ".csv"))
            (call-with-output-file* balance-sheet-assets-file
              (λ (out)
                (displayln "act_symbol,date,period,cash_and_equivalents,receivables,notes_receivable,inventories,other_current_assets,total_current_assets,net_property_and_equipment,investments_and_advances,other_non_current_assets,deferred_charges,intangibles,deposits_and_other_assets,total_assets" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  act_symbol::text,
  date::text,
  period::text,
  coalesce(cash_and_equivalents::text, ''),
  coalesce(receivables::text, ''),
  coalesce(notes_receivable::text, ''),
  coalesce(inventories::text, ''),
  coalesce(other_current_assets::text, ''),
  coalesce(total_current_assets::text, ''),
  coalesce(net_property_and_equipment::text, ''),
  coalesce(investments_and_advances::text, ''),
  coalesce(other_non_current_assets::text, ''),
  coalesce(deferred_charges::text, ''),
  coalesce(intangibles::text, ''),
  coalesce(deposits_and_other_assets::text, ''),
  coalesce(total_assets::text, '')
from
  zacks.balance_sheet_assets
where
  date = $1::text::date
order by
  act_symbol, date, period;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u balance_sheet_assets balance-sheet-assets-" date ".csv")))
          (query-list dbc "
select distinct
  date::text
from
  zacks.balance_sheet_assets
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add balance_sheet_assets; "
                       "/usr/local/bin/dolt commit -m 'balance_sheet_assets " (end-date) " update'; /usr/local/bin/dolt push --silent"))

; balance-sheet-equity
(for-each (λ (date)
            (define balance-sheet-equity-file (string-append (base-folder) "/balance-sheet-equity-" date ".csv"))
            (call-with-output-file* balance-sheet-equity-file
              (λ (out)
                (displayln "act_symbol,date,period,preferred_stock,common_stock,capital_surplus,retained_earnings,other_equity,treasury_stock,total_equity,total_liabilities_and_equity,shares_outstanding,book_value_per_share" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  act_symbol::text,
  date::text,
  period::text,
  coalesce(preferred_stock::text, ''),
  coalesce(common_stock::text, ''),
  coalesce(capital_surplus::text, ''),
  coalesce(retained_earnings::text, ''),
  coalesce(other_equity::text, ''),
  coalesce(treasury_stock::text, ''),
  coalesce(total_equity::text, ''),
  coalesce(total_liabilities_and_equity::text, ''),
  coalesce(shares_outstanding::text, ''),
  coalesce(book_value_per_share::text, '')
from
  zacks.balance_sheet_equity
where
  date = $1::text::date
order by
  act_symbol, date, period;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u balance_sheet_equity balance-sheet-equity-" date ".csv")))
          (query-list dbc "
select distinct
  date::text
from
  zacks.balance_sheet_equity
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add balance_sheet_equity; "
                       "/usr/local/bin/dolt commit -m 'balance_sheet_equity " (end-date) " update'; /usr/local/bin/dolt push --silent"))

; balance-sheet-liabilities
(for-each (λ (date)
            (define balance-sheet-liabilities-file (string-append (base-folder) "/balance-sheet-liabilities-" date ".csv"))
            (call-with-output-file* balance-sheet-liabilities-file
              (λ (out)
                (displayln "act_symbol,date,period,notes_payable,accounts_payable,current_portion_long_term_debt,current_portion_capital_leases,accrued_expenses,income_taxes_payable,other_current_liabilities,total_current_liabilities,mortgages,deferred_taxes_or_income,convertible_debt,long_term_debt,non_current_capital_leases,other_non_current_liabilities,minority_interest,total_liabilities" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  act_symbol::text,
  date::text,
  period::text,
  coalesce(notes_payable::text, ''),
  coalesce(accounts_payable::text, ''),
  coalesce(current_portion_long_term_debt::text, ''),
  coalesce(current_portion_capital_leases::text, ''),
  coalesce(accrued_expenses::text, ''),
  coalesce(income_taxes_payable::text, ''),
  coalesce(other_current_liabilities::text, ''),
  coalesce(total_current_liabilities::text, ''),
  coalesce(mortgages::text, ''),
  coalesce(deferred_taxes_or_income::text, ''),
  coalesce(convertible_debt::text, ''),
  coalesce(long_term_debt::text, ''),
  coalesce(non_current_capital_leases::text, ''),
  coalesce(other_non_current_liabilities::text, ''),
  coalesce(minority_interest::text, ''),
  coalesce(total_liabilities::text, '')
from
  zacks.balance_sheet_liabilities
where
  date = $1::text::date
order by
  act_symbol, date, period;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u balance_sheet_liabilities balance-sheet-liabilities-" date ".csv")))
          (query-list dbc "
select distinct
  date::text
from
  zacks.balance_sheet_liabilities
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add balance_sheet_liabilities; "
                       "/usr/local/bin/dolt commit -m 'balance_sheet_liabilities " (end-date) " update'; /usr/local/bin/dolt push --silent"))

; cash-flow-statement
(for-each (λ (date)
            (define cash-flow-statement-file (string-append (base-folder) "/cash-flow-statement-" date ".csv"))
            (call-with-output-file* cash-flow-statement-file
              (λ (out)
                (displayln "act_symbol,date,period,net_income,depreciation_amortization_and_depletion,net_change_from_assets,net_cash_from_discontinued_operations,other_operating_activities,net_cash_from_operating_activities,property_and_equipment,acquisition_of_subsidiaries,investments,other_investing_activities,net_cash_from_investing_activities,issuance_of_capital_stock,issuance_of_debt,increase_short_term_debt,payment_of_dividends_and_other_distributions,other_financing_activities,net_cash_from_financing_activities,effect_of_exchange_rate_changes,net_change_in_cash_and_equivalents,cash_at_beginning_of_period,cash_at_end_of_period,diluted_net_eps" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  act_symbol::text,
  date::text,
  period::text,
  coalesce(net_income::text, ''),
  coalesce(depreciation_amortization_and_depletion::text, ''),
  coalesce(net_change_from_assets::text, ''),
  coalesce(net_cash_from_discontinued_operations::text, ''),
  coalesce(other_operating_activities::text, ''),
  coalesce(net_cash_from_operating_activities::text, ''),
  coalesce(property_and_equipment::text, ''),
  coalesce(acquisition_of_subsidiaries::text, ''),
  coalesce(investments::text, ''),
  coalesce(other_investing_activities::text, ''),
  coalesce(net_cash_from_investing_activities::text, ''),
  coalesce(issuance_of_capital_stock::text, ''),
  coalesce(issuance_of_debt::text, ''),
  coalesce(increase_short_term_debt::text, ''),
  coalesce(payment_of_dividends_and_other_distributions::text, ''),
  coalesce(other_financing_activities::text, ''),
  coalesce(net_cash_from_financing_activities::text, ''),
  coalesce(effect_of_exchange_rate_changes::text, ''),
  coalesce(net_change_in_cash_and_equivalents::text, ''),
  coalesce(cash_at_beginning_of_period::text, ''),
  coalesce(cash_at_end_of_period::text, ''),
  coalesce(diluted_net_eps::text, '')
from
  zacks.cash_flow_statement
where
  date = $1::text::date
order by
  act_symbol, date, period;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u cash_flow_statement cash-flow-statement-" date ".csv")))
          (query-list dbc "
select distinct
  date::text
from
  zacks.cash_flow_statement
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add cash_flow_statement; "
                       "/usr/local/bin/dolt commit -m 'cash_flow_statement " (end-date) " update'; /usr/local/bin/dolt push --silent"))

; income-statement
(for-each (λ (date)
            (define income-statement-file (string-append (base-folder) "/income-statement-" date ".csv"))
            (call-with-output-file* income-statement-file
              (λ (out)
                (displayln "act_symbol,date,period,sales,cost_of_goods,gross_profit,selling_administrative_depreciation_amortization_expenses,income_after_depreciation_and_amortization,non_operating_income,interest_expense,pretax_income,income_taxes,minority_interest,investment_gains,other_income,income_from_continuing_operations,extras_and_discontinued_operations,net_income,income_before_depreciation_and_amortization,depreciation_and_amortization,average_shares,diluted_eps_before_non_recurring_items,diluted_net_eps" out)
                (for-each (λ (row)
                            (displayln (string-join (vector->list row) ",") out))
                          (query-rows dbc "
select
  act_symbol::text,
  date::text,
  period::text,
  coalesce(sales::text, ''),
  coalesce(cost_of_goods::text, ''),
  coalesce(gross_profit::text, ''),
  coalesce(selling_administrative_depreciation_amortization_expenses::text, ''),
  coalesce(income_after_depreciation_and_amortization::text, ''),
  coalesce(non_operating_income::text, ''),
  coalesce(interest_expense::text, ''),
  coalesce(pretax_income::text, ''),
  coalesce(income_taxes::text, ''),
  coalesce(minority_interest::text, ''),
  coalesce(investment_gains::text, ''),
  coalesce(other_income::text, ''),
  coalesce(income_from_continuing_operations::text, ''),
  coalesce(extras_and_discontinued_operations::text, ''),
  coalesce(net_income::text, ''),
  coalesce(income_before_depreciation_and_amortization::text, ''),
  coalesce(depreciation_and_amortization::text, ''),
  coalesce(average_shares::text, ''),
  coalesce(diluted_eps_before_non_recurring_items::text, ''),
  coalesce(diluted_net_eps::text, '')
from
  zacks.income_statement
where
  date = $1::text::date
order by
  act_symbol, date, period;
"
                                      date)))
              #:exists 'replace)
            (system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -u income_statement income-statement-" date ".csv")))
          (query-list dbc "
select distinct
  date::text
from
  zacks.income_statement
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add income_statement; "
                       "/usr/local/bin/dolt commit -m 'income_statement " (end-date) " update'; /usr/local/bin/dolt push --silent"))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt gc"))
