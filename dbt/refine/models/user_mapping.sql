SELECT DISTINCT
    l.guid AS guid,
    l.id AS lead_id,
    l.created_at AS lead_created_at,
    l.updated_at AS lead_updated_at,
    l.guid AS user_id,
    u.id AS user_record,
    u.created_at AS user_created_at,
    u.updated_at AS user_updated_at,
    u.deleted_at AS user_deleted_at,
    u.ref_id AS finexus_user_id,
    PARSE_DATE('%Y%m%d', f001.f9_dw001_appl_rcv_dt) AS finexus_input_date,
    u.ref_id AS cif_number,
    f002.p9_dw002_crn AS principal_crn
FROM
    `ingest.lead` AS l
LEFT JOIN
    `ingest.user` AS u
    ON
        u.guid = l.guid
LEFT JOIN
    `ingest.dw001` AS f001
    ON
        CAST( u.ref_id AS string) = f001.fx_dw001_cif_no
LEFT JOIN
    `ingest.dw002` AS f002
    ON
        CAST( u.ref_id AS string) = f002.fx_dw002_cif_no
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13