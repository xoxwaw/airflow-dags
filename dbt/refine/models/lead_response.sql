SELECT clr.lead_id                            as lead_id,
       ca.status                              as application_status,
       clr.mobile_number                      as mobile_number,
       clr.application_stage                  as application_stage,
       clr.esign_reference_id                 as esign_reference_id,
       clr.ocr_transaction_id                 as ocr_transaction_id,
       clr.ocr_document_id                    as ocr_document_id,
       clr.income_verification_transaction_id as income_verification_transaction_id,
       clr.ekyc_transaction_id                as ekyc_transaction_id,
       clr.application_status_id              as application_status_id,
       clr.delivery_postal_code               as delivery_postal_code,
       clr.delivery_province                  as delivery_province,
       clr.delivery_city                      as delivery_city,
       clr.delivery_district                  as delivery_district,
       clr.delivery_village                   as delivery_village,
       clr.delivery_rt_rw                     as delivery_rt_rw,
       clr.delivery_street_address_2          as delivery_street_address_2,
       clr.delivery_street_address_1          as delivery_street_address_1,
       clr.delivery_building_name             as delivery_building_name,
       clr.ktp_postal_code                    as ktp_postal_code,
       clr.ktp_province                       as ktp_province,
       clr.ktp_city                           as ktp_city,
       clr.ktp_district                       as ktp_district,
       clr.ktp_village                        as ktp_village,
       clr.ktp_rt_rw                          as ktp_rt_rw,
       clr.ktp_street_address_2               as ktp_street_address_2,
       clr.ktp_street_address_1               as ktp_street_address_1,
       clr.ktp_building_name                  as ktp_building_name,
       clr.job_type_code                      as job_type_code,
       clr.tax_id_number                      as tax_id_number,
       clr.employer_industry_code             as employer_industry_code,
       clr.source_of_income_code              as source_of_income_code,
       clr.employer_name                      as employer_name,
       clr.monthly_income                     as monthly_income,
       clr.education_level_code               as education_level_code,
       clr.mother_maiden_name                 as mother_maiden_name,
       clr.number_of_dependents_code          as number_of_dependents_code,
       clr.marital_status_code                as marital_status_code,
       clr.date_of_birth                      as date_of_birth,
       clr.last_name                          as last_name,
       clr.middle_name                        as middle_name,
       clr.first_name                         as first_name,
       clr.full_name                          as full_name,
       clr.email                              as email,
       clr.ocr_residence_number               as ocr_residence_number,
       clr.ocr_city                           as ocr_city,
       clr.ocr_religion                       as ocr_religion,
       clr.ocr_place_of_birth                 as ocr_place_of_birth,
       clr.ocr_occupation                     as ocr_occupation,
       clr.ocr_marital_status                 as ocr_marital_status,
       clr.ocr_marital_status_code            as ocr_marital_status_code,
       clr.ocr_gender                         as ocr_gender,
       clr.ocr_postal_code                    as ocr_postal_code,
       clr.ocr_street_address                 as ocr_street_address,
       clr.ocr_province                       as ocr_province,
       clr.ocr_sub_district                   as ocr_sub_district,
       clr.ocr_district                       as ocr_district,
       clr.ocr_date_of_birth                  as ocr_date_of_birth,
       clr.ocr_full_name                      as ocr_full_name,
       cel.name                               as education_level,
       cel.name_locale_id                     as education_level_locale_id,
       ci.name                                as employer_industry,
       ci.name_locale_id                      as employer_industry_locale_id,
       cj.name                                as job_type,
       cj.name_locale_id                      as job_type_locale_id,
       cms.name                               as marital_status,
       cnd.name                               as number_of_dependents,
       csi.name                               as source_of_income,
       csi.name_locale_id                     as source_of_income_locale_id
FROM refine.lead_response_stage as clr
         LEFT JOIN ingest.application_status as ca ON clr.lead_id = ca.lead_id
         LEFT JOIN `ingest.education_level` as cel ON clr.education_level_code = cast(cel.code as string)
         LEFT JOIN `ingest.industry` as ci ON clr.employer_industry_code = cast(ci.code as string)
         LEFT JOIN `ingest.job` as cj ON clr.job_type_code = CAST(cj.code AS string)
         LEFT JOIN `ingest.marital_status` as cms ON clr.marital_status_code = cast(cms.code as string)
         LEFT JOIN `ingest.number_of_dependents` as cnd
                   ON cast(clr.number_of_dependents_code as string) = cast(cnd.code as string)
         LEFT JOIN `ingest.source_of_income` as csi
                   ON cast(clr.source_of_income_code as string) = cast(csi.code as string)
