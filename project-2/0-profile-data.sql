with

first_stats AS
  (
    select a.usertoken,
      DATE_TRUNC('day',convert_timezone('America/Los_Angeles',creditinfodate )) as First_CS_Pull_Date,
      vantage3 as First_CS,
      payment_history_grade as first_payment_history_grade,
      credit_utilization_grade as first_credit_utilization_grade,
      credit_age_grade as first_credit_age_grade,
      account_mix_grade as first_account_mix_grade,
      credit_inquiries_grade as first_credit_inquiries_grade,
      (2018-birth_year_bureau) as Age_B,
      gender,
      income,
      a.creditinfoid
    from
    (SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.usertoken ORDER BY creditinfodate ASC) as rn
      FROM credit_profile_history as a
      where scoreonlypull=0 and usertoken is NOT NULL
    ) as a
    Join public."user" as b On a.usertoken=b.usertoken
    where rn=1
    and cast(b.acct_registration_complete_datetime as date) >='2017-01-01'
    and vantage3 between 575 and 625

  ),

first_MoreStats AS
  (select
      a.usertoken
      , (1.0 * sum(
        case when crt.openclosed <> 'Closed' and crt.creditlimit>0 and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
        then crt.currentbalance else 0 end))
            /
            (sum(
        case when crt.openclosed <> 'Closed' and  crt.creditlimit>0 and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
        then crt.creditlimit else 0 END)+1)
      as f_credit_utilization_ratio
  , count(*)                                                     as f_tradeline_count
  , sum(case when crt.openclosed <> 'Closed' then 1 else 0 end)  as f_tradeline_open_count
  , sum(case when crt.openclosed = 'Closed' then 1 else 0 end)   as f_tradeline_closed_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD' then 1 else 0 end)         as f_cc_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'AUTO_LOAN' then 1 else 0 end)           as f_auto_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'MORTGAGE' then 1 else 0 end)            as f_mortgage_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'COLLECTION_ACCOUNT' then 1 else 0 end)  as f_collection_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'STUDENT_LOAN' then 1 else 0 end)        as f_student_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'PERSONAL_LOAN' then 1 else 0 end)       as f_pl_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'OTHER' then 1 else 0 end)               as f_other_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD' then 1 else 0 end)         as f_cc_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'AUTO_LOAN' then 1 else 0 end)           as f_auto_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'MORTGAGE' then 1 else 0 end)            as f_mortgage_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'COLLECTION_ACCOUNT' then 1 else 0 end)  as f_collection_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'STUDENT_LOAN' then 1 else 0 end)        as f_student_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'PERSONAL_LOAN' then 1 else 0 end)       as f_pl_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'OTHER' then 1 else 0 end)               as f_other_open_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD' then 1 else 0 end)         as f_cc_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'AUTO_LOAN' then 1 else 0 end)           as f_auto_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'MORTGAGE' then 1 else 0 end)            as f_mortgage_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'COLLECTION_ACCOUNT' then 1 else 0 end)  as f_collection_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'STUDENT_LOAN' then 1 else 0 end)        as f_student_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'PERSONAL_LOAN' then 1 else 0 end)       as f_pl_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'OTHER' then 1 else 0 end)               as f_other_closed_count
  , sum(crt.currentbalance)                                                      as f_total_balance
  , sum(case when crt.openclosed <> 'Closed' then crt.currentbalance else 0 end) as f_total_open_balance
  , sum(case when crt.openclosed <> 'Closed' then crt.creditlimit else 0 end) as f_total_open_limit

  , sum(case when crt.openclosed = 'Closed' then crt.currentbalance else 0 end)  as f_total_closed_balance
  , sum(crt.highbalance)                                                         as f_total_highbalance
  , sum(case when crt.openclosed <> 'Closed' then crt.currentbalance else 0 end) as f_total_open_highbalance
  , sum(case when crt.openclosed = 'Closed' then crt.currentbalance else 0 end)  as f_total_closed_highbalance
  , sum(crt.creditlimit)                                                         as f_total_limit
  , sum(case when crt.openclosed = 'Closed' then crt.currentbalance else 0 end)   as f_total_closed_limit
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus = 'Late 30 Days' then 1 else 0 end)         as f_closed_late30_count
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus = 'Late 60 Days' then 1 else 0 end)         as f_closed_late60_count
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus = 'Late 90 Days' then 1 else 0 end)         as f_closed_late90_count
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus = 'Late 120 Days' then 1 else 0 end)        as f_closed_Late120_count
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus like 'Late%' then 1 else 0 end)             as f_closed_late_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus = 'Late 30 Days' then 1 else 0 end)        as f_open_late30_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus = 'Late 60 Days' then 1 else 0 end)        as f_open_late60_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus = 'Late 90 Days' then 1 else 0 end)        as f_open_late90_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus = 'Late 120 Days' then 1 else 0 end)       as f_open_Late120_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus like 'Late%' then 1 else 0 end)             as f_open_late_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus = 'Late 30 Days' then 1 else 0 end)    as f_closed_worst_late30_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus = 'Late 60 Days' then 1 else 0 end)    as f_closed_worst_late60_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus = 'Late 90 Days' then 1 else 0 end)    as f_closed_worst_late90_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus = 'Late 120 Days' then 1 else 0 end)   as f_closed_worst_Late120_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus like 'Late%' then 1 else 0 end)        as f_closed_worst_late_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus = 'Late 30 Days' then 1 else 0 end)   as f_open_worst_late30_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus = 'Late 60 Days' then 1 else 0 end)   as f_open_worst_late60_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus = 'Late 90 Days' then 1 else 0 end)   as f_open_worst_late90_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus = 'Late 120 Days' then 1 else 0 end)  as f_open_worst_Late120_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus like 'Late%' then 1 else 0 end)       as f_open_worst_late_count
  , min(crt.creditinfodate - crt.dateopened)                                                          as f_youngest_account_age
  , max(crt.creditinfodate - crt.dateopened)                                                          as f_oldest_account_age
  , min(case when crt.openclosed <> 'Closed' then crt.creditinfodate - crt.dateopened else null end)  as f_youngest_closed_account_age
  , max(case when crt.openclosed <> 'Closed' then crt.creditinfodate - crt.dateopened else null end)  as f_oldest_closed_account_age
  , min(case when crt.openclosed = 'Closed' then crt.creditinfodate - crt.dateopened else null end)   as f_youngest_open_account_age
  , max(case when crt.openclosed = 'Closed' then crt.creditinfodate - crt.dateopened else null end)   as f_oldest_open_account_age
  , min(crt.dateopened)                                                          as f_yougest_account_date
  , max(crt.dateopened)                                                          as f_oldest_account_date
  , min(case when crt.openclosed <> 'Closed' then crt.dateopened else null end)  as f_youngest_closed_account_date
  , max(case when crt.openclosed <> 'Closed' then crt.dateopened else null end)  as f_oldest_closed_account_date
  , min(case when crt.openclosed = 'Closed' then crt.dateopened else null end)   as f_youngest_open_account_date
  , max(case when crt.openclosed = 'Closed' then crt.dateopened else null end)   as f_oldest_open_account_date

  from first_stats a Left Join public.credit_report_tradelines  crt
  ON a.usertoken=crt.usertoken
  and a.creditinfoid = crt.creditinfoid
  GROUP BY 1),

Second_Date AS
  (select a.usertoken,min(b.creditinfoid) as Second_CreditinfoId, min(creditinfodate) as Second_Credit_Date, min(First_CS_Pull_Date) as First_CS_Pull_Date
   from first_stats a
    Left JOIN public.credit_profile_history b ON a.usertoken=b.usertoken and b.creditinfodate between dateadd(day,25,a.First_CS_Pull_Date) and dateadd(day,45,a.First_CS_Pull_Date) and  b.scoreonlypull=0
   Group By 1),

Second_Month_Stats as
  (select
   a.usertoken,
   max(b.vantage3) as Second_CS,
   max(b.payment_history_grade) as second_payment_history_grade,
   max(b.credit_utilization_grade) as second_first_credit_utilization_grade,
   max(b.credit_age_grade) as second_credit_age_grade,
   max(b.account_mix_grade) as second_account_mix_grade,
   max(b.credit_inquiries_grade) as second_credit_inquiries_grade
 , (1.0 * sum(
    case when crt.openclosed <> 'Closed' and crt.creditlimit>0 and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
    then crt.currentbalance else 0 end))
        /
        (sum(
    case when crt.openclosed <> 'Closed' and  crt.creditlimit>0 and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
    then crt.creditlimit else 0 END)+1)
  as s_credit_utilization_ratio
  , count(*)                                                     as s_tradeline_count
  , sum(case when crt.openclosed <> 'Closed' then 1 else 0 end)  as s_tradeline_open_count
  , sum(case when crt.openclosed = 'Closed' then 1 else 0 end)   as s_tradeline_closed_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD' then 1 else 0 end)         as s_cc_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'AUTO_LOAN' then 1 else 0 end)           as s_auto_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'MORTGAGE' then 1 else 0 end)            as s_mortgage_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'COLLECTION_ACCOUNT' then 1 else 0 end)  as s_collection_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'STUDENT_LOAN' then 1 else 0 end)        as s_student_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'PERSONAL_LOAN' then 1 else 0 end)       as s_pl_count
  , sum(case when f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'OTHER' then 1 else 0 end)               as s_other_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD' then 1 else 0 end)         as s_cc_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'AUTO_LOAN' then 1 else 0 end)           as s_auto_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'MORTGAGE' then 1 else 0 end)            as s_mortgage_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'COLLECTION_ACCOUNT' then 1 else 0 end)  as s_collection_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'STUDENT_LOAN' then 1 else 0 end)        as s_student_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'PERSONAL_LOAN' then 1 else 0 end)       as s_pl_open_count
  , sum(case when crt.openclosed <> 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'OTHER' then 1 else 0 end)               as s_other_open_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD' then 1 else 0 end)         as s_cc_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'AUTO_LOAN' then 1 else 0 end)           as s_auto_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'MORTGAGE' then 1 else 0 end)            as s_mortgage_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'COLLECTION_ACCOUNT' then 1 else 0 end)  as s_collection_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'STUDENT_LOAN' then 1 else 0 end)        as s_student_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'PERSONAL_LOAN' then 1 else 0 end)       as s_pl_closed_count
  , sum(case when crt.openclosed = 'Closed' and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'OTHER' then 1 else 0 end)               as s_other_closed_count
  , sum(case when crt.openclosed <> 'Closed' then crt.currentbalance else 0 end) as s_total_open_balance
  , sum(case when crt.openclosed <> 'Closed' then crt.creditlimit else 0 end) as s_total_open_limit
  , sum(crt.currentbalance)                                                      as s_total_balance
  , sum(case when crt.openclosed = 'Closed' then crt.currentbalance else 0 end)  as s_total_closed_balance
  , sum(crt.highbalance)                                                         as s_total_highbalance
  , sum(case when crt.openclosed <> 'Closed' then crt.currentbalance else 0 end) as s_total_open_highbalance
  , sum(case when crt.openclosed = 'Closed' then crt.currentbalance else 0 end)  as s_total_closed_highbalance
  , sum(crt.creditlimit)                                                         as s_total_limit
  , sum(case when crt.openclosed = 'Closed' then crt.currentbalance else 0 end)   as s_total_closed_limit
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus = 'Late 30 Days' then 1 else 0 end)         as s_closed_late30_count
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus = 'Late 60 Days' then 1 else 0 end)         as s_closed_late60_count
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus = 'Late 90 Days' then 1 else 0 end)         as s_closed_late90_count
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus = 'Late 120 Days' then 1 else 0 end)        as s_closed_Late120_count
  , sum(case when crt.openclosed = 'Closed' and crt.paystatus like 'Late%' then 1 else 0 end)             as s_closed_late_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus like 'Late%' then 1 else 0 end)             as s_open_late_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus = 'Late 30 Days' then 1 else 0 end)        as s_open_late30_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus = 'Late 60 Days' then 1 else 0 end)        as s_open_late60_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus = 'Late 90 Days' then 1 else 0 end)        as s_open_late90_count
  , sum(case when crt.openclosed <> 'Closed' and crt.paystatus = 'Late 120 Days' then 1 else 0 end)       as s_open_Late120_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus = 'Late 30 Days' then 1 else 0 end)    as s_closed_worst_late30_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus = 'Late 60 Days' then 1 else 0 end)    as s_closed_worst_late60_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus = 'Late 90 Days' then 1 else 0 end)    as s_closed_worst_late90_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus = 'Late 120 Days' then 1 else 0 end)   as s_closed_worst_Late120_count
  , sum(case when crt.openclosed = 'Closed' and crt.worstpaystatus like 'Late%' then 1 else 0 end)        as s_closed_worst_late_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus = 'Late 30 Days' then 1 else 0 end)   as s_open_worst_late30_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus = 'Late 60 Days' then 1 else 0 end)   as s_open_worst_late60_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus = 'Late 90 Days' then 1 else 0 end)   as s_open_worst_late90_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus = 'Late 120 Days' then 1 else 0 end)  as s_open_worst_Late120_count
  , sum(case when crt.openclosed <> 'Closed' and crt.worstpaystatus like 'Late%' then 1 else 0 end)       as s_open_worst_late_count
  , min(crt.creditinfodate - crt.dateopened)                                                          as s_youngest_account_age
  , max(crt.creditinfodate - crt.dateopened)                                                          as s_oldest_account_age
  , min(case when crt.openclosed <> 'Closed' then crt.creditinfodate - crt.dateopened else null end)  as s_youngest_closed_account_age
  , max(case when crt.openclosed <> 'Closed' then crt.creditinfodate - crt.dateopened else null end)  as s_oldest_closed_account_age
  , min(case when crt.openclosed = 'Closed' then crt.creditinfodate - crt.dateopened else null end)   as s_youngest_open_account_age
  , max(case when crt.openclosed = 'Closed' then crt.creditinfodate - crt.dateopened else null end)   as s_oldest_open_account_age
  , min(crt.dateopened)                                                          as s_yougest_account_date
  , max(crt.dateopened)                                                          as s_oldest_account_date
  , min(case when crt.openclosed <> 'Closed' then crt.dateopened else null end)  as s_youngest_closed_account_date
  , max(case when crt.openclosed <> 'Closed' then crt.dateopened else null end)  as s_oldest_closed_account_date
  , min(case when crt.openclosed = 'Closed' then crt.dateopened else null end)   as s_youngest_open_account_date
  , max(case when crt.openclosed = 'Closed' then crt.dateopened else null end)   as s_oldest_open_account_date
  , sum(case when dateopened BETWEEN First_CS_Pull_Date and Second_Credit_Date  then 1 else 0 end) as Account_Open_In_between
  , sum(case when dateopened BETWEEN First_CS_Pull_Date and Second_Credit_Date and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD' then 1 else 0 end)         as cc_open_count_In_between
  , sum(case when dateopened BETWEEN First_CS_Pull_Date and Second_Credit_Date and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'AUTO_LOAN' then 1 else 0 end)           as auto_open_count_In_between
  , sum(case when dateopened BETWEEN First_CS_Pull_Date and Second_Credit_Date and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'MORTGAGE' then 1 else 0 end)            as mortgage_open_count_In_between
  , sum(case when dateopened BETWEEN First_CS_Pull_Date and Second_Credit_Date and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'COLLECTION_ACCOUNT' then 1 else 0 end)  as collection_open_count_In_between
  , sum(case when dateopened BETWEEN First_CS_Pull_Date and Second_Credit_Date and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'STUDENT_LOAN' then 1 else 0 end)        as student_open_count_In_between
  , sum(case when dateopened BETWEEN First_CS_Pull_Date and Second_Credit_Date and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'PERSONAL_LOAN' then 1 else 0 end)       as pl_open_count_In_between
  , sum(case when dateopened BETWEEN First_CS_Pull_Date and Second_Credit_Date and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'OTHER' then 1 else 0 end)               as other_open_count_In_between
   from Second_Date a
    Left JOIN public.credit_profile_history b ON a.usertoken=b.usertoken and a.Second_CreditinfoId=b.creditinfoid
    Left Join public.credit_report_tradelines crt ON a.usertoken=crt.usertoken and  a.Second_CreditinfoId = crt.creditinfoid
  GROUP BY 1),

first_inquiry as
  (select
    a.usertoken
  , count(*) as first_total_inquiry_count
  , sum(case when cri.type like 'A%' then 1 else 0 end) as first_type_auth_count
  , sum(case when cri.type like 'C%' then 1 else 0 end) as first_type_contr_count
  , sum(case when cri.type like 'I%' then 1 else 0 end) as first_type_indi_count
  , sum(case when cri.type like 'P%' then 1 else 0 end) as first_type_parti_count
  , max(datediff(days,cri.inquirydate,a.First_CS_Pull_Date)) as first_age_of_oldest_inquiry
  , min(datediff(days,cri.inquirydate,a.First_CS_Pull_Date)) as first_age_of_youngest_inquiry
from first_stats a Left Join public.credit_report_inquiries cri ON a.usertoken=cri.usertoken and  a.creditinfoid = cri.creditinfoid
  GROUP BY 1),

second_inquiry as (
select
    a.usertoken
  , count(*) as second_total_inquiry_count
  , sum(case when cri.type like 'A%' then 1 else 0 end) as second_type_auth_count
  , sum(case when cri.type like 'C%' then 1 else 0 end) as second_type_contr_count
  , sum(case when cri.type like 'I%' then 1 else 0 end) as second_type_indi_count
  , sum(case when cri.type like 'P%' then 1 else 0 end) as second_type_parti_count
  , max(datediff(days,cri.inquirydate,a.First_CS_Pull_Date)) as second_age_of_oldest_inquiry
  , min(datediff(days,cri.inquirydate,a.First_CS_Pull_Date)) as second_age_of_youngest_inquiry
from Second_Date a Left Join public.credit_report_inquiries cri ON a.usertoken=cri.usertoken and  a.Second_CreditinfoId = cri.creditinfoid
  GROUP BY 1),

first_negative as (
select
    a.usertoken
  , count(*) as first_total_negativemark_count
  , sum(
        case
        when crpr.classification = 'Legal Item' then 1
        else 0
        end
    ) as first_classification_count_Legal_Item
  , sum(
        case
        when crpr.classification = 'Bankruptcy' then 1
        else 0
        end
    ) as first_classification_count_Bankruptcy
  , sum(
        case
        when crpr.classification = 'Wage Attachment/Garnishment' then 1
        else 0
        end
    ) as first_classification_count_Wage_Attachment_Garnishment
  , sum(
        case
        when crpr.classification = 'Lien' then 1
        else 0
        end
    ) as first_classification_count_Lien
  , sum(
        case
        when crpr.classification = 'Miscellaneous' then 1
        else 0
        end
    ) as first_classification_count_Miscellaneous
  , sum(
        case
        when crpr.classification = 'Real Estate Foreclosure' then 1
        else 0
        end
    ) as first_classification_count_Real_Estate_Foreclosure
  , sum(
        case
        when crpr.classification not in ('Legal Item', 'Bankruptcy', 'Wage Attachment/Garnishment', 'Lien', 'Miscellaneous', 'Real Estate Foreclosure') then 1
        else 0
        end
    ) as first_classification_other_count
from first_stats a Left Join public.credit_report_public_records crpr ON a.usertoken=crpr.usertoken and  a.creditinfoid = crpr.creditinfoid
group by 1),

second_negative as (
select
    a.usertoken
  , count(*) as second_total_negativemark_count
  , sum(
        case
        when crpr.classification = 'Legal Item' then 1
        else 0
        end
    ) as second_classification_count_Legal_Item
  , sum(
        case
        when crpr.classification = 'Bankruptcy' then 1
        else 0
        end
    ) as second_classification_count_Bankruptcy
  , sum(
        case
        when crpr.classification = 'Wage Attachment/Garnishment' then 1
        else 0
        end
    ) as second_classification_count_Wage_Attachment_Garnishment
  , sum(
        case
        when crpr.classification = 'Lien' then 1
        else 0
        end
    ) as second_classification_count_Lien
  , sum(
        case
        when crpr.classification = 'Miscellaneous' then 1
        else 0
        end
    ) as second_classification_count_Miscellaneous
  , sum(
        case
        when crpr.classification = 'Real Estate Foreclosure' then 1
        else 0
        end
    ) as second_classification_count_Real_Estate_Foreclosure
  , sum(
        case
        when crpr.classification not in ('Legal Item', 'Bankruptcy', 'Wage Attachment/Garnishment', 'Lien', 'Miscellaneous', 'Real Estate Foreclosure') then 1
        else 0
        end
    ) as second_classification_other_count
from Second_Date a Left Join public.credit_report_public_records crpr ON a.usertoken=crpr.usertoken and  a.Second_CreditinfoId = crpr.creditinfoid
group by 1)





select
*
from first_stats a
inner join Second_Month_Stats b using(usertoken)
inner join first_MoreStats c using (usertoken)
inner join first_inquiry d using (usertoken)
inner join second_inquiry e using (usertoken)
inner join first_negative f using (usertoken)
inner join second_negative g using (usertoken)
where 1
and (Second_CS - First_CS) between 30 and 100
and Second_CS is not null
