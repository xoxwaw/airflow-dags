SELECT lead_id,
       max(case when question_key = 'mobile_number' then answer end)             mobile_number,
       max(case when question_key = 'stage' then answer end)                     application_stage,
       max(case when question_key = 'esign_reference_id' then answer end)        esign_reference_id,
       max(case when question_key = 'ocr_transaction_id' then answer end)        ocr_transaction_id,
       max(case when question_key = 'ocr_document_id' then answer end)           ocr_document_id,
       max(case
               when question_key = 'verification_income_transaction_id'
                   then answer end)                                              income_verification_transaction_id,
       max(case when question_key = 'ekyc_transaction_id' then answer end)       ekyc_transaction_id,
       application_status_id,
       max(case when question_key = 'delivery_postal_code' then answer end)      delivery_postal_code,
       max(case when question_key = 'delivery_province' then answer end)         delivery_province,
       max(case when question_key = 'delivery_city' then answer end)             delivery_city,
       max(case when question_key = 'delivery_district' then answer end)         delivery_district,
       max(case when question_key = 'delivery_village' then answer end)          delivery_village,
       max(case when question_key = 'delivery_rt_rw' then answer end)            delivery_rt_rw,
       max(case when question_key = 'delivery_street_address_2' then answer end) delivery_street_address_2,
       max(case when question_key = 'delivery_street_address_1' then answer end) delivery_street_address_1,
       max(case when question_key = 'delivery_building_name' then answer end)    delivery_building_name,
       max(case when question_key = 'ktp_postal_code' then answer end)           ktp_postal_code,
       max(case when question_key = 'ktp_province' then answer end)              ktp_province,
       max(case when question_key = 'ktp_city' then answer end)                  ktp_city,
       max(case when question_key = 'ktp_district' then answer end)              ktp_district,
       max(case when question_key = 'ktp_village' then answer end)               ktp_village,
       max(case when question_key = 'ktp_rt_rw' then answer end)                 ktp_rt_rw,
       max(case when question_key = 'ktp_street_address_2' then answer end)      ktp_street_address_2,
       max(case when question_key = 'ktp_street_address_1' then answer end)      ktp_street_address_1,
       max(case when question_key = 'ktp_building_name' then answer end)         ktp_building_name,
       max(case when question_key = 'job_type_code' then answer end)             job_type_code,
       max(case when question_key = 'tax_id_number' then answer end)             tax_id_number,
       max(case when question_key = 'employer_industry_code' then answer end)    employer_industry_code,
       max(case when question_key = 'source_of_income_code' then answer end)     source_of_income_code,
       max(case when question_key = 'employer_name' then answer end)             employer_name,
       max(case when question_key = 'monthly_income' then answer end)            monthly_income,
       max(case when question_key = 'education_level_code' then answer end)      education_level_code,
       max(case when question_key = 'mother_maiden_name' then answer end)        mother_maiden_name,
       max(case when question_key = 'number_of_dependents_code' then answer end) number_of_dependents_code,
       max(case when question_key = 'marital_status_code' then answer end)       marital_status_code,
       max(case when question_key = 'date_of_birth' then answer end)             date_of_birth,
       max(case when question_key = 'last_name' then answer end)                 last_name,
       max(case when question_key = 'middle_name' then answer end)               middle_name,
       max(case when question_key = 'first_name' then answer end)                first_name,
       max(case when question_key = 'full_name' then answer end)                 full_name,
       max(case when question_key = 'email' then answer end)                     email,
       max(case when question_key = 'ocr_residence_number' then answer end)      ocr_residence_number,
       max(case when question_key = 'ocr_city' then answer end)                  ocr_city,
       max(case when question_key = 'ocr_religion' then answer end)              ocr_religion,
       max(case when question_key = 'ocr_place_of_birth' then answer end)        ocr_place_of_birth,
       max(case when question_key = 'ocr_occupation' then answer end)            ocr_occupation,
       max(case when question_key = 'ocr_marital_status' then answer end)        ocr_marital_status,
       max(case when question_key = 'ocr_marital_status_code' then answer end)   ocr_marital_status_code,
       max(case when question_key = 'ocr_gender' then answer end)                ocr_gender,
       max(case when question_key = 'ocr_postal_code' then answer end)           ocr_postal_code,
       max(case when question_key = 'ocr_street_address' then answer end)        ocr_street_address,
       max(case when question_key = 'ocr_province' then answer end)              ocr_province,
       max(case when question_key = 'ocr_sub_district' then answer end)          ocr_sub_district,
       max(case when question_key = 'ocr_district' then answer end)              ocr_district,
       max(case when question_key = 'ocr_date_of_birth' then answer end)         ocr_date_of_birth,
       max(case when question_key = 'ocr_full_name' then answer end)             ocr_full_name
FROM ingest.lead_response
GROUP BY lead_id, application_status_id
