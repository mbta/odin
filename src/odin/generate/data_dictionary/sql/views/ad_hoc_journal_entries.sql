DROP VIEW IF EXISTS cubic_reports.ad_hoc_journal_entries;
CREATE VIEW cubic_reports.ad_hoc_journal_entries
AS
SELECT
accounting_date,
voucher,
ledger_account,
txn_currency_debit_amount,
txn_currency_credit_amount,
external_account_name,
description,
split_part(description, '|', 1) as transaction_category,
split_part(description, '|', 2) as apportionment_rule,
split_part(description, '|', 4) as operating_date,
split_part(description, '|', 5) as fare_instrument_id
FROM
    cubic_ods.edw_fnp_general_jrnl_account_entry
WHERE
    ledger_name = 'MBTA'
;
