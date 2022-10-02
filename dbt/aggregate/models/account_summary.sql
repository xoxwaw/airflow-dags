WITH min_max_agg_tbl AS (SELECT CAST(au.principal_crn AS string) AS user_id,
                                MAX(daily_closing_balance)       AS max_closing_balance,
                                MAX(total_exposure)              AS max_total_exposure,
                                MAX(last_payment_amount)         AS max_payment_amount,
                                MAX(days_past_due)               AS max_days_past_due
                         FROM `refine.account_updates` au
                         GROUP BY 1),
     min_agg_tbl AS (SELECT CAST(au.principal_crn AS string) AS user_id,
                            MIN(au.updated_timestamp_dw004)  AS last_updated_timestamp_dw004,
                            MIN(au.last_payment_date)        AS first_payment_date,
                            MIN(au.credit_limit)             AS initial_credit_limit
                     FROM `refine.account_updates` au
                     GROUP BY 1),
     max_agg_tbl AS (SELECT CAST(au.principal_crn AS string)             AS user_id,
                            MAX(au.updated_timestamp_dw004)              AS last_updated_timestamp_dw004,
                            MAX(au.acct_status_changed_date)             as last_acct_status_changed_date,
                            MAX(au.billing_cycle_day)                    AS billing_cycle_day,
                            MAX(au.payment_due_date)                     AS payment_due_date,
                            MAX(au.cycle_opening_balance)                AS cycle_opening_balance,
                            MAX(au.remaining_statement_balance)          AS remaining_statement_balance,
                            MAX(au.remaining_min_due)                    AS remaining_min_due,
                            max(au.account_status)                       as current_account_status,
                            MAX(au.credit_limit)                         as current_credit_limit,
                            MAX(au.card_status)                          as current_card_status,
                            MAX(au.statement_account_status)             as statement_account_status,
                            MAX(au.daily_closing_balance)                as last_closing_balance,
                            MAX(au.previous_statement_available_balance) as previous_statement_available_balance,
                            MAX(au.total_exposure)                       as current_total_exposure,
                            MAX(au.available_credit_limit)               as available_credit_limit,
                            MAX(au.total_principal_balance)              as total_principal_balance,
                            MAX(au.cycle_debit_amount)                   as cycle_debit_amount,
                            MAX(au.cycle_credit_amount)                  AS cycle_credit_amount,
                            MAX(au.last_statement_interest)              AS last_statement_interest,
                            MAX(au.payment_due_date)                     as last_statement_due_date,
                            MAX(au.carryover_interest)                   AS carryover_interest,
                            MAX(au.unbilled_interest)                    AS unbilled_interest,
                            MAX(au.last_statement_fees)                  AS last_statement_fees,
                            MAX(au.carryover_fees)                       AS carryover_fees,
                            MAX(au.unbilled_fees)                        AS unbilled_fees,
                            MAX(au.last_payment_amount)                  AS last_payment_amount,
                            CAST(au.last_payment_method AS STRING)       AS last_payment_method,
                            MAX(au.last_payment_date)                    AS last_payment_date,
                            MAX(au.auto_debit_option)                    AS auto_debit_option,
                            MAX(au.cumulative_minimum_repayment)         AS cumulative_minimum_repayment,
                            MAX(au.cancelled_date)                       AS cancelled_date,
                            MAX(au.days_past_due)                        AS current_days_past_due,
                            MAX(au.months_past_due)                      AS current_months_past_due,
                            MAX(au.first_payment_default_flag)           AS first_payment_default_flag,
                            MAX(au.first_payment_default_date)           AS first_payment_default_date
                     FROM `refine.account_updates` au
                     GROUP BY 1, 28)
SELECT DISTINCT CAST(au.principal_crn AS string)      AS user_id,
                CAST(um.finexus_user_id AS string)    AS finexus_user_id,
                um.finexus_input_date                 AS finexus_input_date,
                CAST(um.principal_crn AS string)      AS principal_crn,
                max_tbl.last_updated_timestamp_dw004  AS last_updated_timestamp_dw004,
                max_tbl.last_acct_status_changed_date as last_acct_status_changed_date,
                min_tbl.initial_credit_limit,
                max_tbl.current_credit_limit,
                max_tbl.current_account_status,
                max_tbl.current_card_status,
                max_tbl.statement_account_status,
                max_tbl.billing_cycle_day,
                max_tbl.payment_due_date,
                max_tbl.cycle_opening_balance,
                max_tbl.remaining_statement_balance,
                max_tbl.remaining_min_due,
                max_tbl.last_closing_balance,
                max_tbl.previous_statement_available_balance,
                max_tbl.current_total_exposure,
                max_tbl.available_credit_limit,
                max_tbl.total_principal_balance,
                max_tbl.cycle_debit_amount,
                max_tbl.cycle_credit_amount,
                max_tbl.last_statement_interest,
                max_tbl.carryover_interest,
                max_tbl.unbilled_interest,
                max_tbl.last_statement_fees,
                max_tbl.carryover_fees,
                max_tbl.unbilled_fees,
                max_tbl.last_payment_amount,
                max_tbl.last_payment_method,
                max_tbl.last_payment_date,
                min_tbl.first_payment_date,
                max_tbl.auto_debit_option,
                max_tbl.cumulative_minimum_repayment,
                max_tbl.cancelled_date,
                max_tbl.current_days_past_due,
                max_tbl.current_months_past_due,
                max_tbl.first_payment_default_flag,
                max_tbl.first_payment_default_date
FROM `refine.account_updates` AS au
         LEFT JOIN
     `refine.user_mapping` AS um
     ON
         CAST(um.principal_crn AS string) = CAST(au.principal_crn AS string)
         LEFT JOIN
     max_agg_tbl AS max_tbl
     ON
         max_tbl.user_id = CAST(au.principal_crn AS string)
         LEFT JOIN
     min_agg_tbl AS min_tbl
     ON
         min_tbl.user_id = CAST(au.principal_crn AS string)
         LEFT JOIN
     min_max_agg_tbl AS min_max_tbl
     ON
         min_max_tbl.user_id = CAST(au.principal_crn AS string)
WHERE CAST(au.principal_crn AS string) IS NOT NULL
