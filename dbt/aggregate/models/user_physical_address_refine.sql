 WITH upa AS (
        SELECT
        user_id AS user_record,
        created_at AS created_at,
        deleted_at AS deleted_at,
        street_address3 AS ktp_building_name,
        street_address1 AS ktp_street_address_1,
        street_address2 AS ktp_street_address_2,
        postal_code AS ktp_postal_code,
        locality_name AS ktp_city,
        -- CAST(format("%04d", locality) AS string) AS ktp_city_code,
        administrative_area1 AS ktp_province,
        -- locality,
        sub_locality1 AS ktp_district,
        sub_locality2 AS ktp_village,
        sub_locality5 AS ktp_rt_rw,
        CAST(locality AS string) AS ktp_city_code
    FROM
        `ingest.user_physical_address`
    WHERE is_mailing = 0
),
upa1 AS (
    SELECT
        user_id AS user_record,
        street_address3 AS delivery_building_name,
        street_address1 AS delivery_street_address_1,
        street_address2 AS delivery_street_address_2,
        postal_code AS delivery_postal_code,
        locality_name AS delivery_city,
        administrative_area1 AS delivery_province,
        sub_locality1 AS delivery_district,
        sub_locality2 AS delivery_village,
        sub_locality5 AS delivery_rt_rw,
        CAST(
            locality AS string
        ) AS delivery_city_code
    FROM
        `ingest.user_physical_address`
    WHERE is_mailing = 1
)
SELECT
    um.user_record,
    upa.created_at,
    upa.deleted_at,
    upa.ktp_building_name,
    upa.ktp_street_address_1,
    upa.ktp_street_address_2,
    upa.ktp_postal_code,
    upa.ktp_city,
    upa.ktp_city_code,
    upa.ktp_province,
    upa.ktp_district,
    upa.ktp_village,
    upa.ktp_rt_rw,
    upa1.delivery_postal_code,
    upa1.delivery_province,
    upa1.delivery_city,
    upa1.delivery_city_code,
    upa1.delivery_district,
    upa1.delivery_village,
    upa1.delivery_rt_rw,
    upa1.delivery_street_address_2,
    upa1.delivery_street_address_1,
    upa1.delivery_building_name,
    cdc.silaras_lokasi_kode AS silaras_lokasi_kode,
    "" AS country_code
FROM (
    SELECT user_record
    FROM `refine.user_mapping`
    WHERE user_record IS NOT NULL
) AS um
LEFT JOIN
    upa
    ON um.user_record = upa.user_record
LEFT JOIN
    upa1
    ON um.user_record = upa1.user_record
LEFT JOIN
    `mapping.city_district_code` AS cdc
    ON
        upa1.delivery_city_code = cdc.district_code