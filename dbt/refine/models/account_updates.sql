with account_updates as (
SELECT
    f004.p9_dw004_prin_crn AS principal_crn,
    f004.f9_dw004_upd_tms AS updated_timestamp_dw004,
    PARSE_DATE(
        '%Y%m%d', f004.f9_dw004_loc_stat_dt
    ) AS acct_status_changed_date,
    CAST(f004.f9_dw004_loc_lmt AS INT64) AS credit_limit,
    f004.fx_dw004_loc_stat AS account_status,
    CONCAT(
        CAST(f004.fx_dw004_cls AS STRING), CAST(f004.fx_dw004_loc AS STRING)
    ) AS card_status,
    f004.fx_dw004_coll_stat_cde AS statement_account_status,
    f004.f9_dw004_cycc_day AS billing_cycle_day,
    CASE
        WHEN
            f004.f9_dw004_stmt_due_dt != '0' THEN PARSE_DATE(
                '%Y%m%d', f004.f9_dw004_stmt_due_dt
            )
    END AS payment_due_date,
    CAST(f004.f9_dw004_open_bal AS INT64) AS cycle_opening_balance,
    CAST(f004.f9_dw004_os_bill_amt AS INT64) AS remaining_statement_balance,
    CAST(f004.f9_dw004_curr_min_rpmt AS INT64) AS remaining_min_due,
    CAST(f004.f9_dw004_clo_bal AS INT64) AS daily_closing_balance,
    CAST(
        f004.f9_dw004_loc_bal AS INT64
    ) AS previous_statement_available_balance,
    CAST(f004.f9_dw004_tot_exposure AS INT64) AS total_exposure,
    CAST(f004.f9_dw004_avail_bal AS INT64) AS available_credit_limit,
    CAST(f004.f9_dw004_prin AS INT64) AS total_principal_balance,
    CAST(f004.f9_dw004_db_amt AS INT64) AS cycle_debit_amount,
    CAST(f004.f9_dw004_cr_amt AS INT64) AS cycle_credit_amount,
    CAST(f004.f9_dw004_int_1 AS INT64) AS last_statement_interest,
    CAST(f004.f9_dw004_int_2 AS INT64) AS carryover_interest,
    CAST(f004.f9_dw004_tot_int AS INT64) AS unbilled_interest,
    CAST(f004.f9_dw004_bil_fee_chrg_1 AS INT64) AS last_statement_fees,
    CAST(f004.f9_dw004_bil_fee_chrg_2 AS INT64) AS carryover_fees,
    CAST(f004.f9_dw004_unbil_fee_chrg AS INT64) AS unbilled_fees,
    CAST(f004.f9_dw004_pmt_amt AS INT64) AS last_payment_amount,
    CAST(f004.fx_dw004_pmt_meth AS STRING) AS last_payment_method,
    CASE
        WHEN
            f004.f9_dw004_lst_pmt_dt != '0' THEN PARSE_DATE(
                '%Y%m%d', f004.f9_dw004_lst_pmt_dt
            )
    END AS last_payment_date,
    f004.fx_dw004_auto_db_opt AS auto_debit_option,
    CAST(f004.f9_dw004_cmr AS INT64) AS cumulative_minimum_repayment,
    CASE
        WHEN
            f004.f9_dw004_cncl_dt != '0' THEN PARSE_DATE(
                '%Y%m%d', f004.f9_dw004_cncl_dt
            )
    END AS cancelled_date,
    CAST(f004.f9_dw004_curr_dpd AS INT64) AS days_past_due,
    CAST(f004.fx_dw004_curr_age_cde AS INT64) AS months_past_due,
    f004.fx_dw004_fpd_stat AS first_payment_default_flag,
    CASE
        WHEN
            f004.f9_dw004_fpd_dt != '0' THEN PARSE_DATE(
                '%Y%m%d', f004.f9_dw004_fpd_dt
            )
    END AS first_payment_default_date
FROM
    `ingest.dw004` as f004)
    SELECT * from account_updates