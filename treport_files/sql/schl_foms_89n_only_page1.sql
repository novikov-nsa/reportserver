--page1_89n
with all_docs as
         (select true                                                                                            as isd003,
                 false                                                                                           as isd004,
                 d003.datazl_datefinishmp,
                 d003.version,
                 d003.hasprotocolflk,
                 d003.datazl_countassociateddisease,
                 d003.tech_countdistrows,
                 null                                                                                            as datacovid_smpfirstdate,
                 null                                                                                            as datacovid_smpsendinfoclinicdate,
                 d003.p1_hr_report_date,
                 d003.maininfo_doccreatedate,
                 d003.maininfo_docnumber,
                 null                                                                                            as datacovid_firstdochomedate,
                 null                                                                                            as maininfo_datecompletionichlapp,
                 null                                                                                            as datacovid_firstselfclinicdate,
                 null                                                                                            as datacovid_countsmpcall,
                 null                                                                                            as datacovid_countsmpsendinfoclinic,
                 d003.p1_importdatetime,
                 null                                                                                            as datacovid_countcheckillhome,
                 null                                                                                            as datacovid_counttelemedicinecheck,
                 d003.datazl_datebirth,
                 d003.p1_transansysdatetime,
                 d003.p1_sd_z,
                 null                                                                                            as datazl_datecovid,
                 d003.datazl_datestartmp,
                 d003.p1_summav,
                 d003.p1_summap,
                 d003.p1_sank_mek,
                 d003.p1_sank_mee,
                 d003.p1_sank_ekmp,
                 d003.docid,
                 d003.p1_statctrls_guidflc,
                 d003.p1_statctrls_guidflc_gen,
                 d003.p1_statctrls_stateidc,
                 d003.p1_statctrls_statebdc,
                 d003.p1_statctrls_guididc,
                 d003.p1_statctrls_guidbdc,
                 d003.p1_code_org,
                 d003.p1_code_tfoms,
                 d003.p1_package_num,
                 d003.p1_year,
                 d003.p1_month,
                 d003.p1_z_sl_signed,
                 d003.datazl_numpolicyoms,
                 d003.datazl_nummedcard,
                 d003.datazl_gender,
                 d003.datazl_pregnancy,
                 d003.datazl_maindiseasecode,
                 null                                                                                            as datacovid_sevcovidcontact,
                 null                                                                                            as datacovid_approvsevcovidcontact,
                 null                                                                                            as datacovid_expertsevcovidcontact,
                 null                                                                                            as datacovid_maxsevcovidtreat,
                 null                                                                                            as datacovid_approvmaxsevcovidtreat,
                 null                                                                                            as datacovid_expertmaxsevcovidtreat,
                 d003.datacovid_deathcovid,
                 null                                                                                            as datacovid_vacbeforetreat,
                 d003.maininfo_doccreateuser,
                 d003.maininfo_disturbance,
                 d003.maininfo_codetfoms,
                 d003.maininfo_nameexpert,
                 d003.maininfo_codeexpert,
                 d003.maininfo_namesmo,
                 d003.maininfo_codesmo,
                 d003.maininfo_namemo,
                 d003.maininfo_codemo,
                 d003.maininfo_cancelreason,
                 d003.attach_link,
                 d003.tech_namefile,
                 d003.p1_codereportform,
                 d003.p1_hr_period_type,
                 d003.p1_statctrls_stateflc,
                 d003.datacovid_approvsevcovidarrival,
                 d003.datacovid_expertmaxsevcovidhospit,
                 d003.datacovid_expertsevcovidarrival,
                 d003.datacovid_approvmaxsevcovidhospit,
                 d003.datazl_timereceptdepartzl,
                 d003.datacovid_maxsevcovidhospit,
                 d003.datazl_weightzl,
                 d003.maininfo_datecompletionichlks,
                 d003.datacovid_vacbeforehospit,
                 d003.datacovid_app30daybeforehospit,
                 d003.datacovid_sevcovidarrival,
                 d.globaldocid,
                 upper(dtc.systemname)                                                                           as sourcetable,
                 row_number()
                 over (partition by (d003.datazl_numpolicyoms) order by d003.datazl_countassociateddisease desc) as rank_dis,
                 row_number()
                 over (partition by (d003.datazl_numpolicyoms) order by d003.datacovid_vacbeforehospit desc)     as rank_vac,
                 extract(year from
                         age(d003.datazl_datestartmp, d003.datazl_datebirth))                                    as agemp,
                 1                                                                                               as cnt003,
                 0                                                                                               as cnt004,
                 (case
                      when datazl_datefinishmp is not null
                          then datazl_datefinishmp - datazl_datestartmp
                      else to_date('{{ p_end_date }}', 'dd.mm.yyyy') - maininfo_datecompletionichlks
                     end)                                                                                        as days

          from dc_doc_01_003 d003
                   inner join doc d on d.docid = d003.docid
                   inner join doctype dtc on dtc.doctypeid = d.doctypeid
                   inner join docstate ds on d.docstateid = ds.docstateid

    where {{ p_is_ks }}
      and date(coalesce(d003.datazl_datefinishmp, d003.maininfo_datecompletionichlks)) between symmetric to_date('{{ p_start_date }}', 'dd.mm.yyyy') and to_date('{{ p_end_date }}', 'dd.mm.yyyy')
      and upper(dtc.systemname) = 'DOC_01_003'
      and upper(ds.systemname) in ('RECEIVED', 'INCLUDED_CONSOLID')
          union all
          select false                                                                                           as isd003,
                 true                                                                                            as isd004,
                 d004.datazl_datefinishmp,
                 d004.version,
                 d004.hasprotocolflk,
                 d004.datazl_countassociateddisease,
                 d004.tech_countdistrows,
                 d004.datacovid_smpfirstdate,
                 d004.datacovid_smpsendinfoclinicdate,
                 d004.p1_hr_report_date,
                 d004.maininfo_doccreatedate,
                 d004.maininfo_docnumber,
                 d004.datacovid_firstdochomedate,
                 d004.maininfo_datecompletionichlapp,
                 d004.datacovid_firstselfclinicdate,
                 d004.datacovid_countsmpcall,
                 d004.datacovid_countsmpsendinfoclinic,
                 d004.p1_importdatetime,
                 d004.datacovid_countcheckillhome,
                 d004.datacovid_counttelemedicinecheck,
                 d004.datazl_datebirth,
                 d004.p1_transansysdatetime,
                 d004.p1_sd_z,
                 d004.datazl_datecovid,
                 d004.datazl_datestartmp,
                 d004.p1_summav,
                 d004.p1_summap,
                 d004.p1_sank_mek,
                 d004.p1_sank_mee,
                 d004.p1_sank_ekmp,
                 d004.docid,
                 d004.p1_statctrls_guidflc,
                 d004.p1_statctrls_guidflc_gen,
                 d004.p1_statctrls_stateidc,
                 d004.p1_statctrls_statebdc,
                 d004.p1_statctrls_guididc,
                 d004.p1_statctrls_guidbdc,
                 d004.p1_code_org,
                 d004.p1_code_tfoms,
                 d004.p1_package_num,
                 d004.p1_year,
                 d004.p1_month,
                 d004.p1_z_sl_signed,
                 d004.datazl_numpolicyoms,
                 d004.datazl_nummedcard,
                 d004.datazl_gender,
                 d004.datazl_pregnancy,
                 d004.datazl_maindiseasecode,
                 d004.datacovid_sevcovidcontact,
                 d004.datacovid_approvsevcovidcontact,
                 d004.datacovid_expertsevcovidcontact,
                 d004.datacovid_maxsevcovidtreat,
                 d004.datacovid_approvmaxsevcovidtreat,
                 d004.datacovid_expertmaxsevcovidtreat,
                 d004.datacovid_deathcovid,
                 d004.datacovid_vacbeforetreat,
                 d004.maininfo_doccreateuser,
                 d004.maininfo_disturbance,
                 d004.maininfo_codetfoms,
                 d004.maininfo_nameexpert,
                 d004.maininfo_codeexpert,
                 d004.maininfo_namesmo,
                 d004.maininfo_codesmo,
                 d004.maininfo_namemo,
                 d004.maininfo_codemo,
                 d004.maininfo_cancelreason,
                 d004.attach_link,
                 d004.tech_namefile,
                 d004.p1_codereportform,
                 d004.p1_hr_period_type,
                 d004.p1_statctrls_stateflc,
                 null                                                                                            as datacovid_approvsevcovidarrival,
                 null                                                                                            as datacovid_expertmaxsevcovidhospit,
                 null                                                                                            as datacovid_expertsevcovidarrival,
                 null                                                                                            as datacovid_approvmaxsevcovidhospit,
                 null                                                                                            as datazl_timereceptdepartzl,
                 null                                                                                            as datacovid_maxsevcovidhospit,
                 null                                                                                            as datazl_weightzl,
                 null                                                                                            as maininfo_datecompletionichlks,
                 null                                                                                            as datacovid_vacbeforehospit,
                 null                                                                                            as datacovid_app30daybeforehospit,
                 null                                                                                            as datacovid_sevcovidarrival,
                 d.globaldocid,
                 upper(dtc.systemname)                                                                           as sourcetable,
                 row_number()
                 over (partition by (d004.datazl_numpolicyoms) order by d004.datazl_countassociateddisease desc) as rank_dis,
                 row_number()
                 over (partition by (d004.datazl_numpolicyoms) order by d004.datacovid_vacbeforetreat desc)      as rank_vac,
                 extract(year from
                         age(d004.datazl_datestartmp, d004.datazl_datebirth))                                    as agemp,
                 0                                                                                               as cnt003,
                 1                                                                                               as cnt004,
                 (case
                      when datazl_datefinishmp is not null
                          then datazl_datefinishmp - datazl_datestartmp
                      else to_date('{{ p_end_date }}', 'dd.mm.yyyy') - maininfo_datecompletionichlapp
                     end)                                                                                        as days

          from dc_doc_01_004 d004
                   inner join doc d on d.docid = d004.docid
                   inner join doctype dtc on dtc.doctypeid = d.doctypeid
                   inner join docstate ds on d.docstateid = ds.docstateid

    where {{ p_is_app }}
      and date(coalesce(d004.datazl_datefinishmp, d004.maininfo_datecompletionichlapp)) between symmetric to_date('{{ p_start_date }}', 'dd.mm.yyyy') and to_date('{{ p_end_date }}', 'dd.mm.yyyy')
      and upper(dtc.systemname) = 'DOC_01_004'
      and upper(ds.systemname) in ('RECEIVED', 'INCLUDED_CONSOLID')
      ),
     calc_data as
         (select maininfo_codetfoms::varchar(6)                                                            as codetfoms,
                 count(all_docs.docid)::integer                                                            as ICHLNumber,
                 count(distinct all_docs.datazl_numpolicyoms)::integer                                     as ZLNumber,
                 count(distinct case
                                    when all_docs.datazl_gender = '2' then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as FemaleNumber,
                 count(distinct case
                                    when all_docs.datazl_pregnancy = '1' then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as PregnantNumber,
                 count(distinct case
                                    when all_docs.datazl_gender = '1' then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as MaleNumber,
                 count(distinct case
                                    when all_docs.datacovid_deathcovid = '1' then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as DeadNumber,
                 count(distinct case
                                    when all_docs.datacovid_deathcovid = '1' and all_docs.datazl_gender = '2'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as DeadNumberFemale,
                 count(distinct case
                                    when all_docs.datacovid_deathcovid = '1' and all_docs.datazl_gender = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as DeadNumberMale,
                 round(avg(extract(year from
                                   age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))))::integer as AverageAge,
                 round(avg(case
                               when all_docs.datazl_gender = '2' then
                                   extract(year from
                                           age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))
                               else null end)
                     )::integer                                                                            as AverageAgeFemale,
                 round(avg(case
                               when all_docs.datazl_gender = '1' then
                                   extract(year from
                                           age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))
                               else null end)
                     )::integer                                                                            as AverageAgeMale,
                 count(distinct case
                                    when all_docs.isd003 then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as KSNumber,
                 count(distinct case
                                    when all_docs.isd004 then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as APPNumber,
                 count(case when all_docs.isd003 then 1 else null end)::integer                            as KSExpertiseNumber,
                 count(case
                           when all_docs.isd003 and upper(all_docs.maininfo_disturbance) = upper('lit1') then 1
                           else null end)::integer                                                         as KSDisturbanceNumber,
                 count(case when all_docs.isd004 then 1 else null end)::integer                            as APPExpertiseNumber,
                 count(case
                           when all_docs.isd004 and upper(all_docs.maininfo_disturbance) = upper('lit1') then 1
                           else null end)::integer                                                         as APPDisturbanceNumber,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datazl_gender = '2'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as FemaleNumberKS,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datazl_pregnancy = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as PregnantNumberKS,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datazl_gender = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as MaleNumberKS,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datacovid_deathcovid = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as DeadNumberKS,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datacovid_deathcovid = '1' and
                                         all_docs.datazl_gender = '2' then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as DeadNumberFemaleKS,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datacovid_deathcovid = '1' and
                                         all_docs.datazl_gender = '1' then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as DeadNumberMaleKS,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datacovid_deathcovid = '1' and
                                         all_docs.datacovid_maxsevcovidhospit in ('1', '2')
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as NonTypicalLethalKS,
                 round(avg(case
                               when all_docs.isd003
                                   then extract(year from
                                                age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))
                               else null end)
                     )::integer                                                                            as AverageAgeKS,
                 round(avg(case
                               when all_docs.isd003 and all_docs.datazl_gender = '2' then
                                   extract(year from
                                           age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))
                               else null end)
                     )::integer                                                                            as AverageAgeFemaleKS,
                 round(avg(case
                               when all_docs.isd003 and all_docs.datazl_gender = '1' then
                                   extract(year from
                                           age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))
                               else null end)
                     )::integer                                                                            as AverageAgeMaleKS,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datacovid_vacbeforehospit = '0'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeHospit0,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datacovid_vacbeforehospit = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeHospit1,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datacovid_vacbeforehospit = '2'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeHospit2,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datacovid_vacbeforehospit = '3'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeHospit3,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.datazl_countassociateddisease is not null
                                        and all_docs.datazl_countassociateddisease <> '0'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDiseaseAllKS,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datazl_countassociateddisease = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDisease1KS,
                 count(distinct case
                                    when all_docs.isd003 and all_docs.datazl_countassociateddisease = '2'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDisease2KS,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.datazl_countassociateddisease is not null
                                        and all_docs.datazl_countassociateddisease not in ('1', '2')
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDisease3KS,
                 coalesce(round(avg(case
                                        when all_docs.isd003
                                            then all_docs.datazl_datefinishmp - all_docs.datazl_datestartmp
                                        else null end)),
                          0)::integer                                                                      as TreatTimingKS,
                 coalesce(to_char(avg(case
                                          when all_docs.isd003
                                              then all_docs.datazl_timereceptdepartzl::time
                                          else null
                     end),
                                  'HH24:MI'),
                          '00:00')::varchar(10)                                                            as TimeRecept,
                 count(case
                           when all_docs.isd003 and all_docs.datazl_maindiseasecode = 'U07.1'
                               then all_docs.docid
                           else null end)::integer                                                         as MainDiseaseCovidKS,
                 count(case
                           when all_docs.isd003 and all_docs.datacovid_app30daybeforehospit = '1'
                               then all_docs.docid
                           else null end)::integer                                                         as APP30Day,
                 '[]'::json                                                                                as SevCovidKS,
                 '[]'::json                                                                                as MaxSevCovidKS,
                 count(case
                           when
                                   all_docs.isd003
                                   and all_docs.datacovid_sevcovidarrival::integer <
                                       all_docs.datacovid_maxsevcovidhospit::integer
                               then all_docs.docid
                           else null end)::integer                                                         as SeverityRiseAllKS,
                 count(case
                           when
                                   all_docs.isd003
                                   and all_docs.datacovid_sevcovidarrival = '1'
                                   and all_docs.datacovid_sevcovidarrival::integer <
                                       all_docs.datacovid_maxsevcovidhospit::integer
                               then all_docs.docid
                           else null end)::integer                                                         as SeverityRiseLightKS,
                 count(case
                           when
                                   all_docs.isd003
                                   and all_docs.datacovid_sevcovidarrival = '2'
                                   and all_docs.datacovid_sevcovidarrival::integer <
                                       all_docs.datacovid_maxsevcovidhospit::integer
                               then all_docs.docid
                           else null end)::integer                                                         as SeverityRiseMediumKS,
                 count(case
                           when
                                   all_docs.isd003
                                   and all_docs.datacovid_deathcovid = '0'
                                   and all_docs.datacovid_sevcovidarrival::integer <
                                       all_docs.datacovid_maxsevcovidhospit::integer
                               then all_docs.docid
                           else null end)::integer
                                                                                                           as SeverityRiseNonLethalKS,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datazl_gender = '2'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as FemaleNumberAPP,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datazl_pregnancy = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as PregnantNumberAPP,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datazl_gender = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as MaleNumberAPP,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datacovid_deathcovid = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as DeadNumberAPP,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datacovid_deathcovid = '1' and
                                         all_docs.datazl_gender = '2' then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as DeadNumberFemaleAPP,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datacovid_deathcovid = '1' and
                                         all_docs.datazl_gender = '1' then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as DeadNumberMaleAPP,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datacovid_deathcovid = '1' and
                                         all_docs.datacovid_maxsevcovidtreat in ('1', '2')
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as NonTypicalLethalAPP,
                 round(avg(case
                               when all_docs.isd004
                                   then extract(year from
                                                age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))
                               else null end)
                     )::integer                                                                            as AverageAgeAPP,
                 round(avg(case
                               when all_docs.isd004 and all_docs.datazl_gender = '2' then
                                   extract(year from
                                           age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))
                               else null end)
                     )::integer                                                                            as AverageAgeFemaleAPP,
                 round(avg(case
                               when all_docs.isd004 and all_docs.datazl_gender = '1' then
                                   extract(year from
                                           age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))
                               else null end)
                     )::integer                                                                            as AverageAgeMaleAPP,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datacovid_vacbeforetreat = '0'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeTreat0,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datacovid_vacbeforetreat = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeTreat1,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datacovid_vacbeforetreat = '2'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeTreat2,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datacovid_vacbeforetreat = '3'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeTreat3,
                 count(distinct case
                                    when all_docs.isd004
                                        and all_docs.datazl_countassociateddisease is not null
                                        and all_docs.datazl_countassociateddisease <> '0'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDiseaseAllAPP,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datazl_countassociateddisease = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDisease1APP,
                 count(distinct case
                                    when all_docs.isd004 and all_docs.datazl_countassociateddisease = '2'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDisease2APP,
                 count(distinct case
                                    when all_docs.isd004
                                        and all_docs.datazl_countassociateddisease is not null
                                        and all_docs.datazl_countassociateddisease not in ('1', '2')
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDisease3APP,
                 round(avg(case
                               when all_docs.isd004
                                   then all_docs.datazl_datefinishmp - all_docs.datazl_datestartmp
                               else null end))::integer                                                    as TreatTimingAPP,
                 count(case
                           when all_docs.isd004 and all_docs.datazl_maindiseasecode = 'U07.1'
                               then all_docs.docid
                           else null end)::integer                                                         as MainDiseaseCovidAPP,
                 count(case
                           when all_docs.isd004 and all_docs.datacovid_smpfirstdate is not null
                               then all_docs.docid
                           else null end)::integer                                                         as SMPFirst,
                 count(case
                           when all_docs.isd004 and all_docs.datacovid_firstdochomedate is not null
                               then all_docs.docid
                           else null end)::integer                                                         as DocHomeFirst,
                 count(case
                           when all_docs.isd004 and all_docs.datacovid_firstselfclinicdate is not null
                               then all_docs.docid
                           else null end)::integer                                                         as SelfClinicFirst,
                 sum(case
                         when all_docs.isd004 then all_docs.datacovid_countsmpcall
                         else 0 end)::integer                                                              as SMPCount,
                 round(avg(case
                               when all_docs.isd004 and all_docs.datacovid_countsmpcall is not null
                                   then all_docs.datacovid_countsmpcall
                               else null end))::numeric(5, 2)                                              as AverageSMPCount,
                 sum(case
                         when all_docs.isd004 then all_docs.datacovid_countcheckillhome
                         else 0 end)::integer                                                              as HomeCheckCount,
                 round(avg(case
                               when all_docs.isd004 and all_docs.datacovid_countcheckillhome is not null
                                   then all_docs.datacovid_countcheckillhome
                               else null end))::numeric(5, 2)                                              as AverageHomeCheckCount,
                 sum(case
                         when all_docs.isd004 then all_docs.datacovid_counttelemedicinecheck
                         else 0 end)::integer                                                              as TelemedicineCheckCount,
                 round(avg(case
                               when all_docs.isd004 and all_docs.datacovid_counttelemedicinecheck is not null
                                   then all_docs.datacovid_counttelemedicinecheck
                               else null end))::numeric(5, 2)                                              as AverageTelemedicineCheckCount,
                 '[]'::json                                                                                as SevCovidAPP,
                 '[]'::json                                                                                as MaxSevCovidAPP,
                 count(case
                           when
                                   all_docs.isd004
                                   and all_docs.datacovid_sevcovidcontact::integer <
                                       all_docs.datacovid_maxsevcovidtreat::integer
                               then all_docs.docid
                           else null end)::integer
                                                                                                           as SeverityRiseAllAPP,
                 count(case
                           when
                                   all_docs.isd004
                                   and all_docs.datacovid_sevcovidcontact = '1'
                                   and all_docs.datacovid_sevcovidcontact::integer <
                                       all_docs.datacovid_maxsevcovidtreat::integer
                               then all_docs.docid
                           else null end)::integer                                                         as SeverityRiseLightAPP,
                 count(case
                           when
                                   all_docs.isd004
                                   and all_docs.datacovid_sevcovidcontact = '2'
                                   and all_docs.datacovid_sevcovidcontact::integer <
                                       all_docs.datacovid_maxsevcovidtreat::integer
                               then all_docs.docid
                           else null end)::integer                                                         as SeverityRiseMediumAPP,
                 count(case
                           when
                                   all_docs.isd004
                                   and all_docs.datacovid_deathcovid = '0'
                                   and all_docs.datacovid_sevcovidcontact::integer <
                                       all_docs.datacovid_maxsevcovidtreat::integer
                               then all_docs.docid
                           else null end)::integer                                                         as SeverityRiseNonLethalAPP,
                 0                                                                                         as RegimenKSAll,
                 0                                                                                         as ApprovRegimenKS,
                 0                                                                                         as AverageRegimenKS,
                 '[]'::json                                                                                as DataRegimenTreatKS,
                 0                                                                                         as RegimenAPPAll,
                 0                                                                                         as ApprovRegimenAPP,
                 0                                                                                         as AverageRegimenAPP,
                 '[]'::json                                                                                as DataRegimenTreatAPP,
                 0                                                                                         as DisturbanceKSAll,
                 '[]'::json                                                                                as DataDisturbanceCodeKS,
                 0                                                                                         as DisturbanceAPPAll,
                 '[]'::json                                                                                as DataDisturbanceCodeAPP,
                 '[]'::json                                                                                as DataCodeStringIchlDistCodeKS,
                 '[]'::json                                                                                as DataCodeStringIchlDistCodeAPP,

                 '[]'::jsonb                                                                               as DrillDown,
                 count(distinct
                       case
                           when round(extract(year from
                                              age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) <=
                                17
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as ZLNumber17,
                 count(distinct
                       case
                           when
                                       round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) <=
                                       17
                                   and all_docs.datazl_gender = '2'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Female17Number,
                 count(distinct
                       case
                           when
                                       round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) <=
                                       17
                                   and all_docs.datazl_gender = '1'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Male17Number,

                 count(distinct
                       case
                           when round(extract(year from
                                              age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) between 18 and 64
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as ZLNumber18_64,
                 count(distinct
                       case
                           when
                                   round(extract(year from
                                                 age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) between 18 and 64
                                   and all_docs.datazl_gender = '2'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Female18_64Number,
                 count(distinct
                       case
                           when
                                   round(extract(year from
                                                 age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) between 18 and 64
                                   and all_docs.datazl_gender = '1'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Male18_64Number,

                 count(distinct
                       case
                           when round(extract(year from
                                              age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) >=
                                65
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as ZLNumber65,
                 count(distinct
                       case
                           when
                                       round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) >=
                                       65
                                   and all_docs.datazl_gender = '2'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Female65Number,
                 count(distinct
                       case
                           when
                                       round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) >=
                                       65
                                   and all_docs.datazl_gender = '1'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Male65Number

          from all_docs

          where
             --003_new_conditions_start
                (all_docs.isd003 and all_docs.datacovid_app30daybeforehospit = '1')
             or (all_docs.isd003 and all_docs.datacovid_deathcovid = '1')
             or (
                  all_docs.isd003 and all_docs.datacovid_maxsevcovidhospit in ('3', '4')
                  and exists(select 1
                             from fs_dateexecutioncriteria_st_list ll
                             where ll.docid = all_docs.docid
                               --and (ll.codestringichlks = '4.25.' or ll.tablerownum = 37)
                               and (ll.tablerownum = 36)
                               and ll.executionmark = '1'
                      )
              )
             or (
                  all_docs.isd003 and all_docs.agemp between 18 and 64 and days > 5
                  and exists(select 1
                             from fs_datazl_associateddisease_st_list fdasl
                             where fdasl.docid = all_docs.docid
                               and (
                                     associateddiseasecode between 'J44' and 'J44*' or
                                     associateddiseasecode between 'E10' and 'E14*' or
                                     associateddiseasecode between 'I05' and 'I09*' or
                                     associateddiseasecode between 'I10' and 'I15*' or
                                     associateddiseasecode between 'I20' and 'I25*' or
                                     associateddiseasecode between 'I48' and 'I48*' or
                                     associateddiseasecode between 'I63' and 'I69*'
                                 )
                      )
              )
             --003_new_conditions_end
             --004_new_conditions_start
             or (all_docs.isd004 and all_docs.datacovid_deathcovid = '1')
             or (all_docs.isd004 and all_docs.agemp > 65 and days > 7)
             or (
                  all_docs.isd004 and all_docs.agemp between 18 and 64 and days > 10
                  and exists(select 1
                             from ufos.fs_datazl_associateddisease_list fdasl
                             where fdasl.docid = all_docs.docid
                               and (
                                     associateddiseasecode between 'J44' and 'J44*' or
                                     associateddiseasecode between 'E10' and 'E14*' or
                                     associateddiseasecode between 'I05' and 'I09*' or
                                     associateddiseasecode between 'I10' and 'I15*' or
                                     associateddiseasecode between 'I20' and 'I25*' or
                                     associateddiseasecode between 'I48' and 'I48*' or
                                     associateddiseasecode between 'I63' and 'I69*'
                                 )
                      )
              )
             or (
                  all_docs.isd004 and all_docs.agemp between 0 and 17 and days > 7
                  and exists(select 1
                             from ufos.fs_datazl_associateddisease_list fdasl
                             where fdasl.docid = all_docs.docid
                               and (
                                     associateddiseasecode between 'G80' and 'G83*' or
                                     associateddiseasecode between 'E66' and 'E66*' or
                                     associateddiseasecode between 'C00' and 'C97*' or
                                     associateddiseasecode between 'D00' and 'D09*' or
                                     associateddiseasecode between 'D21' and 'D21*' or
                                     associateddiseasecode between 'D31' and 'D33*' or
                                     associateddiseasecode between 'D35' and 'D48*'
                                 )
                      )
              )
             --004_new_conditions_end
          group by all_docs.maininfo_codetfoms),

     covid_data as
         (select d_rb_erz.code_tfoms as tfo,
                 count(*)            as CovidAll,
                 sum(case
                         when coalesce(il.sick_p_amb, false) = false
                             then 1
                         else 0 end) as CovidKSAll,
                 sum(case
                         when coalesce(il.sick_p_amb, false) = false
                             and extract(year from (age(il.sick_date_2, il.pers_dr))) between 18 and 64
                             and abs(coalesce(il.sick_date_3, to_date('{{ p_end_date }}', 'dd.mm.yyyy')-10) - il.sick_date_2) > 5
                             then 1
                         else 0 end) as CovidKS18_64,

                 sum(case
                         when coalesce(il.sick_p_amb, false) = false
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) between 18 and 64
                             and il.sick_date_3 - il.sick_date_2 > 5
                             and coalesce(il.acci_yn_cardio, false)
                             then 1
                         else 0 end) as CovidKS18_64SZ,
                 sum(case
                         when coalesce(il.sick_p_amb, false) = false
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) between 18 and 64
                             and il.sick_date_3 - il.sick_date_2 > 5
                             and coalesce(il.acci_yn_bronchi, false)
                             and il.acci_bronchi between 'J44' and 'J44*'
                             then 1
                         else 0 end) as CovidKS18_64HOBL,
                 sum(case
                         when coalesce(il.sick_p_amb, false) = false
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) between 18 and 64
                             and il.sick_date_3 - il.sick_date_2 > 5
                             and coalesce(il.acci_yn_endocr, false)
                             and il.acci_endocr between 'E10' and 'E14*'
                             then 1
                         else 0 end) as CovidKS18_64SD,
                 sum(case
                         when coalesce(il.sick_p_amb, false) = true
                             and upper(il.sick_dsresult) = upper('Перевод пациента на стационарное лечение')
                             then 1
                         else 0 end) as CovidKS_APP,
                 sum(case
                         when coalesce(il.sick_p_amb, false) = false
                             and il.sick_severity in ('Тяжелое', 'Крайне тяжелое')
                             then 1
                         else 0 end) as CovidKSHeavy,
                 sum(case
                         when coalesce(il.sick_p_amb, false) = false
                             and upper(il.sick_dsresult) = 'СМЕРТЬ'
                             then 1
                         else 0 end) as CovidKSLethal,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             then 1
                         else 0 end) as CovidAPPAll,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and extract(year from (age(il.sick_date_1, il.pers_dr))) <= 17
                             and abs(coalesce(il.sick_date_3, to_date('{{ p_end_date }}', 'dd.mm.yyyy')-10) - il.sick_date_1) > 7
                             then 1
                         else 0 end) as CovidAPP17,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) <= 17
                             and il.sick_date_3 - il.sick_date_1 > 7
                             and coalesce(il.acci_yn_onko, false)
                             then 1
                         else 0 end) as CovidAPP17Onko,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) <= 17
                             and il.sick_date_3 - il.sick_date_1 > 7
                             and coalesce(il.acci_yn_endocr, false)
                             and il.acci_endocr between 'E66' and 'E66*'
                             then 1
                         else 0 end) as CovidAPP17Fat,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) <= 17
                             and il.sick_date_3 - il.sick_date_1 > 7
                             and coalesce(il.acci_yn_etc, false)
                             and il.acci_etc between 'G80' and 'G83*'
                             then 1
                         else 0 end) as CovidAPP17DCP,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) between 18 and 64
                             and il.sick_date_3 - il.sick_date_1 > 10
                             then 1
                         else 0 end) as CovidAPP18_64,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) between 18 and 64
                             and il.sick_date_3 - il.sick_date_1 > 10
                             and coalesce(il.acci_yn_cardio, false)
                             then 1
                         else 0 end) as CovidAPP18_64SZ,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) between 18 and 64
                             and il.sick_date_3 - il.sick_date_1 > 10
                             and coalesce(il.acci_yn_bronchi, false)
                             and il.acci_bronchi between 'J44' and 'J44*'
                             then 1
                         else 0 end) as CovidAPP18_64HOBL,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and extract(year from (age(il.sick_date_3, il.pers_dr))) between 18 and 64
                             and il.sick_date_3 - il.sick_date_1 > 10
                             and coalesce(il.acci_yn_endocr, false)
                             and il.acci_endocr between 'E10' and 'E14*'
                             then 1
                         else 0 end) as CovidAPP18_64SD,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and extract(year from (age(il.sick_date_1, il.pers_dr))) > 65
                             and abs(coalesce(il.sick_date_3, to_date('{{ p_end_date }}', 'dd.mm.yyyy')-10) - il.sick_date_1) > 7
                             then 1
                         else 0 end) as CovidAPP65,
                 sum(case
                         when coalesce(il.sick_p_amb, false)
                             and upper(il.sick_dsresult) = 'СМЕРТЬ'
                             then 1
                         else 0 end) as CovidAPPLethal

          from ufos.dc_insurancelists il
                   --        inner join doc d_erz on (d_erz.docid = il.docid)
                   inner join dc_report_base d_RB_erz on (d_RB_erz.docid = il.docid)
          where false
            and il.info_p_inf = 'COVID'
            and il.sick_ds1 in ('U07.1', 'U07.2')
            and il.sick_date_1 between to_date('{{ p_start_date }}', 'dd.mm.yyyy')-10 and to_date('{{ p_end_date }}', 'dd.mm.yyyy')-10

          group by d_rb_erz.code_tfoms),
     data_all as (select calc_data.codetfoms,
                         calc_data.ichlnumber,
                         calc_data.zlnumber,
                         calc_data.femalenumber,
                         calc_data.pregnantnumber,
                         calc_data.malenumber,
                         calc_data.deadnumber,
                         calc_data.deadnumberfemale,
                         calc_data.deadnumbermale,
                         calc_data.averageage                                 as averageage,
                         calc_data.averageagefemale                           as averageagefemale,
                         calc_data.averageagemale                             as averageagemale,
                         calc_data.ksnumber,
                         calc_data.appnumber,
                         calc_data.ksexpertisenumber,
                         calc_data.ksdisturbancenumber,
                         calc_data.appexpertisenumber,
                         calc_data.appdisturbancenumber,
                         calc_data.femalenumberks,
                         calc_data.pregnantnumberks,
                         calc_data.malenumberks,
                         calc_data.deadnumberks,
                         calc_data.deadnumberfemaleks,
                         calc_data.deadnumbermaleks,
                         calc_data.nontypicallethalks,
                         calc_data.averageageks                               as averageageks,
                         calc_data.averageagefemaleks                         as averageagefemaleks,
                         calc_data.averageagemaleks                           as averageagemaleks,
                         calc_data.vacbeforehospit0,
                         calc_data.vacbeforehospit1,
                         calc_data.vacbeforehospit2,
                         calc_data.vacbeforehospit3,
                         calc_data.associateddiseaseallks,
                         calc_data.associateddisease1ks,
                         calc_data.associateddisease2ks,
                         calc_data.associateddisease3ks,
                         calc_data.treattimingks,
                         calc_data.timerecept,
                         calc_data.maindiseasecovidks,
                         calc_data.app30day,
                         calc_data.severityriseallks,
                         calc_data.severityriselightks,
                         calc_data.severityrisemediumks,
                         calc_data.severityrisenonlethalks,
                         calc_data.femalenumberapp,
                         calc_data.pregnantnumberapp,
                         calc_data.malenumberapp,
                         calc_data.deadnumberapp,
                         calc_data.deadnumberfemaleapp,
                         calc_data.deadnumbermaleapp,
                         calc_data.nontypicallethalapp,
                         coalesce(calc_data.averageageapp, 0)                 as averageageapp,
                         coalesce(calc_data.averageagefemaleapp, 0)           as averageagefemaleapp,
                         coalesce(calc_data.averageagemaleapp, 0)             as averageagemaleapp,
                         calc_data.vacbeforetreat0,
                         calc_data.vacbeforetreat1,
                         calc_data.vacbeforetreat2,
                         calc_data.vacbeforetreat3,
                         calc_data.associateddiseaseallapp,
                         calc_data.associateddisease1app,
                         calc_data.associateddisease2app,
                         calc_data.associateddisease3app,
                         coalesce(calc_data.treattimingapp, 0)                as treattimingapp,
                         calc_data.maindiseasecovidapp,
                         calc_data.smpfirst,
                         calc_data.dochomefirst,
                         calc_data.selfclinicfirst,
                         calc_data.smpcount,
                         coalesce(calc_data.averagesmpcount, 0)               as averagesmpcount,
                         coalesce(calc_data.homecheckcount, 0)                as homecheckcount,
                         coalesce(calc_data.averagehomecheckcount, 0)         as averagehomecheckcount,
                         coalesce(calc_data.telemedicinecheckcount, 0)        as telemedicinecheckcount,
                         coalesce(calc_data.averagetelemedicinecheckcount, 0) as averagetelemedicinecheckcount,
                         calc_data.severityriseallapp,
                         calc_data.severityriselightapp,
                         calc_data.severityrisemediumapp,
                         calc_data.severityrisenonlethalapp,
                         calc_data.ZLNumber17,
                         calc_data.Female17Number,
                         calc_data.Male17Number,
                         calc_data.ZLNumber18_64,
                         calc_data.Female18_64Number,
                         calc_data.Male18_64Number,
                         calc_data.ZLNumber65,
                         calc_data.Female65Number,
                         calc_data.Male65Number
                  from calc_data),

     order_report as (select f010.kod_tf,
                             f010.okrug,
                             case
                                 when f010.okrug is null and grouping(f010.okrug, f010.kod_tf) = 3 then 100
                                 when f010.okrug = 1 and grouping(f010.okrug, f010.kod_tf) = 1 then 1000
                                 when f010.okrug = 2 and grouping(f010.okrug, f010.kod_tf) = 1 then 2000
                                 when f010.okrug = 3 and grouping(f010.okrug, f010.kod_tf) = 1 then 3000
                                 when f010.okrug = 4 and grouping(f010.okrug, f010.kod_tf) = 1 then 4000
                                 when f010.okrug = 5 and grouping(f010.okrug, f010.kod_tf) = 1 then 5000
                                 when f010.okrug = 6 and grouping(f010.okrug, f010.kod_tf) = 1 then 6000
                                 when f010.okrug = 7 and grouping(f010.okrug, f010.kod_tf) = 1 then 7000
                                 when f010.okrug = 8 and grouping(f010.okrug, f010.kod_tf) = 1 then 8000
                                 when f010.okrug = 0 and grouping(f010.okrug, f010.kod_tf) = 1 then 9000
                                 end                                as gr,

                             sum(coalesce(ICHLNumber, 0))           as ICHLNumber,
                             null                                   as AverageTreatNumber,
                             sum(coalesce(ZLNumber, 0))             as ZLNumber,
                             sum(coalesce(FemaleNumber, 0))         as FemaleNumber,
                             sum(coalesce(PregnantNumber, 0))       as PregnantNumber,
                             null                                   as FemaleProportion,
                             sum(coalesce(MaleNumber, 0))           as MaleNumber,
                             null                                   as MaleProportion,
                             sum(coalesce(DeadNumber, 0))           as DeadNumber,
                             null                                   as DeadProportion,
                             sum(coalesce(DeadNumberFemale, 0))     as DeadNumberFemale,
                             null                                   as DeadFemaleProportion,
                             sum(coalesce(DeadNumberMale, 0))       as DeadNumberMale,
                             null                                   as DeadMaleProportion,
                             avg(AverageAge)                        as AverageAge,
                             avg(AverageAgeFemale)                  as AverageAgeFemale,
                             avg(AverageAgeMale)                    as AverageAgeMale,
                             sum(coalesce(KSNumber, 0))             as KSNumber,
                             null                                   as KSNumberProportion,
                             sum(coalesce(APPNumber, 0))            as APPNumber,
                             null                                   as APPNumberProportion,
                             sum(coalesce(KSExpertiseNumber, 0))    as KSExpertiseNumber,
                             sum(coalesce(KSDisturbanceNumber, 0))  as KSDisturbanceNumber,
                             null                                   as KSDisturbanceProportion,
                             sum(coalesce(APPExpertiseNumber, 0))   as APPExpertiseNumber,
                             sum(coalesce(APPDisturbanceNumber, 0)) as APPDisturbanceNumber,
                             null                                   as APPDisturbanceProportion,
                             sum(coalesce(ZLNumber17, 0))           as ZLNumber17,
                             sum(coalesce(Female17Number, 0))       as Female17Number,
                             sum(coalesce(Male17Number, 0))         as Male17Number,
                             sum(coalesce(ZLNumber18_64, 0))        as ZLNumber18_64,
                             sum(coalesce(Female18_64Number, 0))    as Female18_64Number,
                             sum(coalesce(Male18_64Number, 0))      as Male18_64Number,
                             sum(coalesce(ZLNumber65, 0))           as ZLNumber65,
                             sum(coalesce(Female65Number, 0))       as Female65Number,
                             sum(coalesce(Male65Number, 0))         as Male65Number,
                             sum(coalesce(CovidAll, 0))             as CovidAll,
                             sum(coalesce(CovidKSAll, 0))           as CovidKSAll,
                             sum(coalesce(CovidKS18_64, 0))         as CovidKS18_64,
                             sum(coalesce(CovidKS18_64SZ, 0))       as CovidKS18_64SZ,
                             sum(coalesce(CovidKS18_64HOBL, 0))     as CovidKS18_64HOBL,
                             sum(coalesce(CovidKS18_64SD, 0))       as CovidKS18_64SD,
                             sum(coalesce(CovidKS_APP, 0))          as CovidKS_APP,
                             sum(coalesce(CovidKSHeavy, 0))         as CovidKSHeavy,
                             sum(coalesce(CovidKSLethal, 0))        as CovidKSLethal,
                             sum(coalesce(CovidAPPAll, 0))          as CovidAPPAll,
                             sum(coalesce(CovidAPP17, 0))           as CovidAPP17,
                             sum(coalesce(CovidAPP17Onko, 0))       as CovidAPP17Onko,
                             sum(coalesce(CovidAPP17Fat, 0))        as CovidAPP17Fat,
                             sum(coalesce(CovidAPP17DCP, 0))        as CovidAPP17DCP,
                             sum(coalesce(CovidAPP18_64, 0))        as CovidAPP18_64,
                             sum(coalesce(CovidAPP18_64SZ, 0))      as CovidAPP18_64SZ,
                             sum(coalesce(CovidAPP18_64HOBL, 0))    as CovidAPP18_64HOBL,
                             sum(coalesce(CovidAPP18_64SD, 0))      as CovidAPP18_64SD,
                             sum(coalesce(CovidAPP65, 0))           as CovidAPP65,
                             sum(coalesce(CovidAPPLethal, 0))       as CovidAPPLethal

                      from ufos.DC_RFOMS_F001 f001
                               left join data_all on data_all.codetfoms = f001.tf_kod
                               left join covid_data on covid_data.tfo = f001.tf_kod
                               left join ufos.dc_rfoms_f010 f010
                                         on f001.tf_okato = f010.kod_okato and f010.dateend is null

                      group by rollup (f010.okrug, f010.kod_tf))
select f010.kod_tf                       as CodeTfoms,
       f010.subname                      as SubjectName,
       coalesce(CovidAll, 0)             as CovidAll,
       coalesce(CovidKSAll, 0)           as CovidKSAll,
       coalesce(CovidKS18_64, 0)         as CovidKS18_64,
       coalesce(CovidKS18_64SZ, 0)       as CovidKS18_64SZ,
       coalesce(CovidKS18_64HOBL, 0)     as CovidKS18_64HOBL,
       coalesce(CovidKS18_64SD, 0)       as CovidKS18_64SD,
       coalesce(CovidKS_APP, 0)          as CovidKS_APP,
       coalesce(CovidKSHeavy, 0)         as CovidKSHeavy,
       coalesce(CovidKSLethal, 0)        as CovidKSLethal,
       --null                              as CovidAPPAll,
       coalesce(CovidAPPAll, 0)          as CovidAPPAll,
       coalesce(CovidAPP17, 0)           as CovidAPP17,
       coalesce(CovidAPP17Onko, 0)       as CovidAPP17Onko,
       coalesce(CovidAPP17Fat, 0)        as CovidAPP17Fat,
       coalesce(CovidAPP17DCP, 0)        as CovidAPP17DCP,
       coalesce(CovidAPP18_64, 0)        as CovidAPP18_64,
       coalesce(CovidAPP18_64SZ, 0)      as CovidAPP18_64SZ,
       coalesce(CovidAPP18_64HOBL, 0)    as CovidAPP18_64HOBL,
       coalesce(CovidAPP18_64SD, 0)      as CovidAPP18_64SD,
       coalesce(CovidAPP65, 0)           as CovidAPP65,
       coalesce(CovidAPPLethal, 0)       as CovidAPPLethal,
       coalesce(ICHLNumber, 0)           as ICHLNumber,
       coalesce(ZLNumber, 0)             as ZLNumber,
       coalesce(DeadNumber, 0)           as DeadNumber,
       null                              as DeadProportion,
       coalesce(KSExpertiseNumber, 0)    as KSExpertiseNumber,
       coalesce(KSDisturbanceNumber, 0)  as KSDisturbanceNumber,
       null                              as KSDisturbanceProportion,
       coalesce(APPExpertiseNumber, 0)   as APPExpertiseNumber,
       coalesce(APPDisturbanceNumber, 0) as APPDisturbanceNumber,
       null                              as APPDisturbanceProportion

from ufos.v_icl_report_f010 f010

         left join order_report on f010.kod_tf = order_report.kod_tf or f010.sort = order_report.gr

order by f010.sort
;