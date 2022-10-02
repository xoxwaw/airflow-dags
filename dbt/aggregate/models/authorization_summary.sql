SELECT ar.transaction_id                 AS transaction_id,
       ar.finexus_user_id                AS finexus_user_id,
       ar.principal_crn                  AS principal_crn,
       ar.updated_timestamp_dw007        AS updated_timestamp_dw007,
       ar.sequence_number                AS sequence_number,
       ar.transaction_date               AS transaction_date,
       ar.transaction_time               AS transaction_time,
       ar.pos_requested_amount           AS pos_requested_amount,
       ar.pos_requested_currency_code    AS pos_requested_currency_code,
       ar.merchant_settled_currency_code AS merchant_settled_currency_code,
       ar.original_amount                AS original_amount,
       ar.currency_code                  AS currency_code,
       ar.status_code                    AS auth_status_code,
       arc.value                         AS auth_status_text,
       ar.approval_code                  AS approval_code,
       ar.response_code                  AS response_code,
       ar.referral_code                  AS auth_referral_code,
       arc.value                         AS auth_referral_text,
       arc.code                          AS auth_transaction_type_code,
       ar.transaction_type_code          AS auth_transaction_type_text,
       ar.ecom_indicator                 AS ecom_indicator,
       ar.pos_mode                       AS pos_mode,
       ar.pos_country_code               AS pos_country_code,
       ar.additional_pos_info_code       AS additional_pos_info_code,
       ar.pos_id                         AS pos_id,
       ar.on_us_indicator                AS on_us_indicator,
       ar.merchant_id                    AS merchant_id,
       ar.merchant_name                  AS merchant_name,
       ar.merchant_category_code         AS merchant_category_code,
       ar.acquirer_merchant_id           AS acquirer_merchant_id,
       ar.acquirer_institution_code      AS acquirer_institution_code,
       ar.fallback_txn_flag              AS fallback_txn_flag,
       ar.contactless_flag               AS contactless_flag,
       ar.charge_slip_flag               AS charge_slip_flag,
       ar.surchurge_fee                  AS surchurge_fee,
       ar.surchurge_vat_fee              AS surchurge_vat_fee,
       ar.issuer_surchurge_fee           AS issuer_surchurge_fee,
       ar.issuer_surchurge_vat_fee       AS issuer_surchurge_vat_fee,
       ar.routing_destination            AS routing_destination,
       ar.cvc_result_code                AS cvc_result_code,
       ar.stand_in_flag                  AS stand_in_flag,
       ar.pin_status_code                AS pin_status_code,
       psc.value                         AS pin_status_text
FROM `refine.authorization_requests` AS ar
         LEFT JOIN
     `mapping.transaction_type_code` AS tyc
     ON
         CAST(tyc.code AS string) = ar.transaction_type_code
         LEFT JOIN
     `mapping.pin_status_code` AS psc
     ON
         CAST(psc.code AS string) = CAST(ar.pin_status_code AS string)
         LEFT JOIN
     `mapping.authorization_referral_code` AS arc
     ON
         CAST(arc.code AS string) = ar.referral_code
