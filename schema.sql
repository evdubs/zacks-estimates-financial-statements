CREATE SCHEMA zacks;

CREATE TYPE zacks.estimate_period AS ENUM
    ('Current Quarter', 'Next Quarter', 'Current Year', 'Next Year');
	
CREATE TYPE zacks.rank AS ENUM
    ('Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell');
	
CREATE TYPE zacks.score AS ENUM
    ('A', 'B', 'C', 'D', 'F');
	
CREATE TYPE zacks.statement_period AS ENUM
    ('Year', 'Quarter');

CREATE TYPE zacks."when" AS ENUM
    ('Before market open', 'After market close');

CREATE TABLE zacks.balance_sheet_assets
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period zacks.statement_period NOT NULL,
    cash_and_equivalents numeric,
    receivables numeric,
    notes_receivable numeric,
    inventories numeric,
    other_current_assets numeric,
    total_current_assets numeric,
    net_property_and_equipment numeric,
    investments_and_advances numeric,
    other_non_current_assets numeric,
    deferred_charges numeric,
    intangibles numeric,
    deposits_and_other_assets numeric,
    total_assets numeric,
    CONSTRAINT balance_sheet_assets_pkey PRIMARY KEY (act_symbol, date, period),
    CONSTRAINT balance_sheet_assets_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.balance_sheet_equity
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period zacks.statement_period NOT NULL,
    preferred_stock numeric,
    common_stock numeric,
    capital_surplus numeric,
    retained_earnings numeric,
    other_equity numeric,
    treasury_stock numeric,
    total_equity numeric,
    total_liabilities_and_equity numeric,
    shares_outstanding numeric,
    book_value_per_share numeric,
    CONSTRAINT balance_sheet_equity_pkey PRIMARY KEY (act_symbol, date, period),
    CONSTRAINT balance_sheet_equity_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.balance_sheet_liabilities
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period zacks.statement_period NOT NULL,
    notes_payable numeric,
    accounts_payable numeric,
    current_portion_long_term_debt numeric,
    current_portion_capital_leases numeric,
    accrued_expenses numeric,
    income_taxes_payable numeric,
    other_current_liabilities numeric,
    total_current_liabilities numeric,
    mortgages numeric,
    deferred_taxes_or_income numeric,
    convertible_debt numeric,
    long_term_debt numeric,
    non_current_capital_leases numeric,
    other_non_current_liabilities numeric,
    minority_interest numeric,
    total_liabilities numeric,
    CONSTRAINT balance_sheet_liabilities_pkey PRIMARY KEY (act_symbol, date, period),
    CONSTRAINT balance_sheet_liabilities_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.cash_flow_statement
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period zacks.statement_period NOT NULL,
    net_income numeric,
    depreciation_amortization_and_depletion numeric,
    net_change_from_assets numeric,
    net_cash_from_discontinued_operations numeric,
    other_operating_activities numeric,
    net_cash_from_operating_activities numeric,
    property_and_equipment numeric,
    acquisition_of_subsidiaries numeric,
    investments numeric,
    other_investing_activities numeric,
    net_cash_from_investing_activities numeric,
    issuance_of_capital_stock numeric,
    issuance_of_debt numeric,
    increase_short_term_debt numeric,
    payment_of_dividends_and_other_distributions numeric,
    other_financing_activites numeric,
    net_cash_from_financing_activites numeric,
    effect_of_exchange_rate_changes numeric,
    net_change_in_cash_and_equivalents numeric,
    cash_at_beginning_of_period numeric,
    cash_at_end_of_period numeric,
    diluted_net_eps numeric,
    CONSTRAINT cash_flow_statement_pkey PRIMARY KEY (act_symbol, date, period),
    CONSTRAINT cash_flow_statement_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.eps_estimate
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period zacks.estimate_period NOT NULL,
    period_end_date date NOT NULL,
    consensus numeric,
    recent numeric,
    count smallint,
    high numeric,
    low numeric,
    year_ago numeric,
    CONSTRAINT eps_estimate_pkey PRIMARY KEY (act_symbol, date, period),
    CONSTRAINT eps_estimate_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.eps_history
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period_end_date date NOT NULL,
    reported numeric,
    estimate numeric,
    CONSTRAINT eps_history_pkey PRIMARY KEY (act_symbol, date, period_end_date),
    CONSTRAINT eps_history_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.eps_perception
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period zacks.estimate_period NOT NULL,
    period_end_date date NOT NULL,
    most_accurate numeric,
    CONSTRAINT eps_perception_pkey PRIMARY KEY (act_symbol, date, period),
    CONSTRAINT eps_perception_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.eps_revision
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period zacks.estimate_period NOT NULL,
    period_end_date date NOT NULL,
    up_7 smallint,
    up_30 smallint,
    up_60 smallint,
    down_7 smallint,
    down_30 smallint,
    down_60 smallint,
    CONSTRAINT eps_revision_pkey PRIMARY KEY (act_symbol, date, period),
    CONSTRAINT eps_revision_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.income_statement
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period zacks.statement_period NOT NULL,
    sales numeric,
    cost_of_goods numeric,
    gross_profit numeric,
    selling_administrative_depreciation_amortization_expenses numeric,
    income_after_depreciation_and_amortization numeric,
    non_operating_income numeric,
    interest_expense numeric,
    pretax_income numeric,
    income_taxes numeric,
    minority_interest numeric,
    investment_gains numeric,
    other_income numeric,
    income_from_continuing_operations numeric,
    extras_and_discontinued_operations numeric,
    net_income numeric,
    income_before_depreciation_and_amortization numeric,
    depreciation_and_amortization numeric,
    average_shares numeric,
    diluted_eps_before_non_recurring_items numeric,
    diluted_net_eps numeric,
    CONSTRAINT income_statement_pkey PRIMARY KEY (act_symbol, date, period),
    CONSTRAINT income_statement_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.rank_score
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    rank zacks.rank NOT NULL,
    value zacks.score NOT NULL,
    growth zacks.score NOT NULL,
    momentum zacks.score NOT NULL,
    vgm zacks.score NOT NULL,
    CONSTRAINT rank_score_pkey PRIMARY KEY (act_symbol, date),
    CONSTRAINT rank_score_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.sales_estimate
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    period zacks.estimate_period NOT NULL,
    period_end_date date NOT NULL,
    consensus numeric,
    count smallint,
    high numeric,
    low numeric,
    year_ago numeric,
    CONSTRAINT sales_estimate_pkey PRIMARY KEY (act_symbol, date, period),
    CONSTRAINT sales_estimate_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE zacks.earnings_calendar
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    "when" zacks."when",
    CONSTRAINT earnings_calendar_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

CREATE OR REPLACE FUNCTION zacks.to_integer_rank(
	rank zacks.rank)
    RETURNS integer
    LANGUAGE 'sql'
AS $BODY$
select
  case "rank"::text
    when 'Strong Buy' then 1
    when 'Buy' then 2
    when 'Hold' then 3
    when 'Sell' then 4
    when 'Strong Sell' then 5
  end;
$BODY$;

