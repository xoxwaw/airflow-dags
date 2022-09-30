SELECT
    l.id AS lead_id,
    l.created_at AS lead_created_at,
    MAX(l.updated_at) AS lead_updated_at,
    u.id AS user_record,
    u.created_at AS user_created_at,
    MAX(u.updated_at) AS user_updated_at,
    u.title_id AS title_id,
    CASE
        WHEN u.title_id = 0 THEN 'UNKNOWN'
        WHEN u.title_id = 1 THEN 'Mr'
        WHEN u.title_id = 2 THEN 'Ms'
        WHEN u.title_id = 3 THEN 'Mrs'
    END
    AS title,
    u.first_name AS first_name,
    u.middle_name AS middle_name,
    u.last_name AS last_name,
    lr.full_name AS full_name,
    u.email AS email,
    u.mobile_phone AS mobile_phone,
    u.status AS application_status,
    udi.gender AS gender_id,
    CASE
        WHEN udi.gender = 0 THEN 'UNKNOWN'
        WHEN udi.gender = 1 THEN 'MALE'
        WHEN udi.gender = 2 THEN 'FEMALE'
    END
    AS gender,
    udi.date_of_birth AS date_of_birth,
    udi.city_of_birth AS city_of_birth,
    udi.country_of_nationality_id AS country_of_nationality_id,
    id.document_number AS identity_document_number,
    lr.monthly_income AS initial_monthly_income,
    udi.monthly_income AS monthly_income,
    udi.monthly_income_currency_code AS monthly_income_currency
FROM
    ingest.lead AS l
LEFT JOIN
    ingest.user AS u
    ON
        l.guid = u.guid
LEFT JOIN
    refine.lead_response AS lr
    ON
        l.id = lr.lead_id
LEFT JOIN
    ingest.user_demographic_info AS udi
    ON
        udi.user_id = u.id
LEFT JOIN
    ingest.identity_document AS id
    ON
        u.id = id.user_id
WHERE
    full_name IS NOT NULL
    AND u.id IS NOT NULL
GROUP BY
    1,
    2,
    4,
    5,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24