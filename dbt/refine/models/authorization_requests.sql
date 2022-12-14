with authorization_requests as (SELECT fnx007.fx_dw007_txn_id                         AS transaction_id,
                                       fnx007.fx_dw007_urn                            AS finexus_user_id,
                                       fnx007.f9_dw007_prin_crn                       AS principal_crn,
                                       fnx007.f9_dw007_upd_tms                        AS updated_timestamp_dw007,
                                       fnx007.p9_dw007_seq                            AS sequence_number,
                                       PARSE_DATE('%Y%m%d', fnx007.f9_dw007_dt)       AS transaction_date,
                                       fnx007.f9_dw007_tm                             AS transaction_time,
                                       CAST(fnx007.f9_dw007_amt_req AS INT64)         AS pos_requested_amount,
                                       fnx007.f9_dw007_amt_req_crncy_cde              AS pos_requested_currency_code,
                                       fnx007.f9_dw007_pos_crncy_cde                  AS merchant_settled_currency_code,
                                       CAST(fnx007.f9_dw007_ori_amt AS INT64)         AS original_amount,
                                       fnx007.f9_dw007_crncy_cde                      AS currency_code,
                                       fnx007.fx_dw007_stat                           AS status_code,
                                       fnx007.fx_dw007_given_apv_cde                  AS approval_code,
                                       fnx007.fx_dw007_given_resp_cde                 AS response_code,
                                       fnx007.fx_dw007_ref_cde                        AS referral_code,
                                       fnx007.fx_dw007_txn_typ                        AS transaction_type_code,
                                       fnx007.f9_dw007_eci_sec_lvl                    AS ecom_indicator,
                                       fnx007.fx_dw007_pos_mode                       AS pos_mode,
                                       fnx007.fx_dw007_cntry_cde                      AS pos_country_code,
                                       fnx007.fx_dw007_pos_cond_cde                   AS additional_pos_info_code,
                                       fnx007.fx_dw007_tid                            AS pos_id,
                                       fnx007.fx_dw007_onus_ind                       AS on_us_indicator,
                                       fnx007.fx_dw007_mid                            AS merchant_id,
                                       fnx007.fx_dw007_merc_name                      AS merchant_name,
                                       fnx007.f9_dw007_mcc                            AS merchant_category_code,
                                       fnx007.fx_dw007_ori_mid                        AS acquirer_merchant_id,
                                       fnx007.f9_dw007_acq_ica                        AS acquirer_institution_code,
                                       fnx007.fx_dw007_fbck_flg                       AS fallback_txn_flag,
                                       fnx007.fx_dw007_contc_less_flg                 AS contactless_flag,
                                       fnx007.fx_dw007_chrg_slp_ind                   AS charge_slip_flag,
                                       CAST(fnx007.f9_dw007_surchrg_fee AS INT64)     AS surchurge_fee,
                                       CAST(fnx007.f9_dw007_surchrg_vat_fee AS INT64) AS surchurge_vat_fee,
                                       CAST(fnx007.f9_dw007_iss_surchrg_fee AS INT64) AS issuer_surchurge_fee,
                                       CAST(
                                               fnx007.f9_dw007_iss_surchrg_vat_fee AS INT64
                                           )                                          AS issuer_surchurge_vat_fee,
                                       fnx007.fx_dw007_rte_dest                       AS routing_destination,
                                       fnx007.fx_dw007_cvv_rslt_cde                   AS cvc_result_code,
                                       fnx007.fx_dw007_stnd_in_ind                    AS stand_in_flag,
                                       fnx007.f9_dw007_pin_stat                       AS pin_status_code
                                FROM `ingest.dw007` AS fnx007)
SELECT *
from authorization_requests
