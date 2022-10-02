SELECT um.user_id                    AS user_id,
       um.user_record                AS user_record,
       um.lead_id                    AS lead_id,
       um.finexus_user_id            AS finexus_user_id,
       um.user_created_at            AS user_created_at,
       um.user_updated_at            AS user_updated_at,
       um.user_deleted_at            AS user_deleted_at,
       ud.title                      AS title,
       ud.first_name                 AS first_name,
       ud.middle_name                AS middle_name,
       ud.last_name                  AS last_name,
       ud.email                      AS email,
       ud.mobile_phone               AS mobile_phone,
       ud.application_status         AS application_status,
       ud.gender                     AS gender,
       ud.date_of_birth              AS date_of_birth,
       ud.city_of_birth              AS city_of_birth,
       ud.country_of_nationality_id  AS country_of_nationality_id,
       ud.identity_document_number   as identity_document_number,
       ud.initial_monthly_income     AS initial_monthly_income,
       ud.monthly_income             AS monthly_income,
       ud.monthly_income_currency    AS monthly_income_currency,
       ud.full_name                  AS full_name,
       upa.ktp_building_name         AS ktp_building_name,
       upa.ktp_street_address_1      AS ktp_street_address_1,
       upa.ktp_street_address_2      AS ktp_street_address_2,
       upa.ktp_rt_rw                 AS ktp_rt_rw,
       upa.ktp_village               AS ktp_village,
       upa.ktp_district              AS ktp_district,
       upa.ktp_city_code             AS ktp_city_code,
       upa.ktp_city                  AS ktp_city,
       upa.ktp_province              AS ktp_province,
       upa.ktp_postal_code           AS ktp_postal_code,
       upa.delivery_building_name    AS delivery_building_name,
       upa.delivery_street_address_1 AS delivery_street_address_1,
       upa.delivery_street_address_2 AS delivery_street_address_2,
       upa.delivery_rt_rw            AS delivery_rt_rw,
       upa.delivery_village          AS delivery_village,
       upa.delivery_district         AS delivery_district,
       upa.delivery_city_code        AS delivery_city_code,
       upa.delivery_city             AS delivery_city,
       upa.delivery_province         AS delivery_province,
       upa.delivery_postal_code      AS delivery_postal_code
FROM `refine.user_mapping` AS um
         LEFT JOIN
     `refine.user_physical_address` AS upa
     ON
         upa.user_record = um.user_record
         LEFT JOIN
     `refine.user_demographics` AS ud
     ON
         ud.user_record = um.user_record
WHERE um.user_record is not null
