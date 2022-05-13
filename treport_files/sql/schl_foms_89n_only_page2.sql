--page2_89n
with all_docs as
         (
         select true                                                                                            as isd003,
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
                      else to_date('{p_end_date}', 'dd.mm.yyyy') - maininfo_datecompletionichlks
                     end)                                                                                        as days,
                 case
                     when datacovid_deathcovid = '1' then 14
                     when
                                 datacovid_maxsevcovidhospit in ('3', '4')
                             and exists(select 1
                                        from fs_dateexecutioncriteria_st_list ll
                                        where ll.docid = d003.docid
                                          --and (ll.codestringichlks = '4.25.' or ll.tablerownum = 37)
                                          and (ll.tablerownum = 36)
                                          and ll.executionmark = '1'
                                     )
                         then 13
                     when datacovid_app30daybeforehospit = '1' then 12
                     else 0
                     end                                                                                         as rec_rank

          from dc_doc_01_003 d003
                   inner join doc d on d.docid = d003.docid
                   inner join doctype dtc on dtc.doctypeid = d.doctypeid
                   inner join docstate ds on d.docstateid = ds.docstateid

    where {p_is_ks}
      and date(coalesce(d003.datazl_datefinishmp, d003.maininfo_datecompletionichlks)) between symmetric to_date('{p_start_date}', 'dd.mm.yyyy') and to_date('{p_end_date}', 'dd.mm.yyyy')
      {d003_select_create_date}
      and upper(dtc.systemname) = 'DOC_01_003'
      and upper(ds.systemname) in ('RECEIVED', 'INCLUDED_CONSOLID')
      {d003_select_tfoms_list}
      {d003_custom_request}
),

     calc_data as (select all_docs.maininfo_codetfoms::varchar(6)                                                   as codetfoms,
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
                                    when all_docs.isd003 then all_docs.docid
                                    else null end)::integer                                                as KSExpertiseNumber,
                 count(case
                           when all_docs.isd003 and upper(all_docs.maininfo_disturbance) = upper('lit1') then 1
                           else null end)::integer                                                         as KSDisturbanceNumber,
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
                                    when all_docs.isd003 and all_docs.rec_rank = 14
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
                                    when all_docs.isd003
                                        and all_docs.rank_vac = 1
                                        and (all_docs.datacovid_vacbeforehospit in ('1', '2', '3'))
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as vachospitall,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.rank_vac = 1
                                        and (all_docs.datacovid_vacbeforehospit = '0'
                                            or all_docs.datacovid_vacbeforehospit is null)
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeHospit0,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.rank_vac = 1
                                        and all_docs.datacovid_vacbeforehospit = '1'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeHospit1,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.rank_vac = 1
                                        and all_docs.datacovid_vacbeforehospit = '2'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeHospit2,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.rank_vac = 1
                                        and all_docs.datacovid_vacbeforehospit = '3'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as VacBeforeHospit3,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.datazl_countassociateddisease is not null
                                        and all_docs.rank_dis = 1
                                        and all_docs.datazl_countassociateddisease <> '0'
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDiseaseAllKS,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.rank_dis = 1
                                        and exists(select 1
                                                   from fs_datazl_associateddisease_st_list fdasl
                                                   where fdasl.docid = all_docs.docid
                                                     and (
                                                           associateddiseasecode between 'I05' and 'I09*' or
                                                           associateddiseasecode between 'I10' and 'I15*' or
                                                           associateddiseasecode between 'I20' and 'I25*' or
                                                           associateddiseasecode between 'I48' and 'I48*' or
                                                           associateddiseasecode between 'I63' and 'I69*'
                                                       )
                                             )
                                        then all_docs.datazl_numpolicyoms
                     end)::integer                                                                         as AssociatedDisease1KS,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.rank_dis = 1
                                        and exists(select 1
                                                   from fs_datazl_associateddisease_st_list fdasl
                                                   where fdasl.docid = all_docs.docid
                                                     and (
                                                       associateddiseasecode between 'E10' and 'E14*'
                                                       )
                                             )
                                        then all_docs.datazl_numpolicyoms
                     end)::integer                                                                         as AssociatedDisease2KS,
                 count(distinct case
                                    when all_docs.isd003
                                        and all_docs.rank_dis = 1
                                        and exists(select 1
                                                   from fs_datazl_associateddisease_st_list fdasl
                                                   where fdasl.docid = all_docs.docid
                                                     and (
                                                       associateddiseasecode between 'J44' and 'J44*'
                                                       )
                                             )
                                        then all_docs.datazl_numpolicyoms
                                    else null end)::integer                                                as AssociatedDisease3KS,
                 avg(case
                         when all_docs.isd003
                             then all_docs.datazl_datefinishmp - all_docs.datazl_datestartmp
                         else null end)::integer                                                           as TreatTimingKS,
                 avg(case
                         when all_docs.isd003
                             then all_docs.datazl_timereceptdepartzl::time
                     end)::time                                                                            as TimeRecept,

                 count(case
                           when all_docs.isd003 and all_docs.datazl_maindiseasecode = 'U07.1'
                               then all_docs.docid
                           else null end)::integer                                                         as MainDiseaseCovidKS,
                 count(case
                           when all_docs.isd003 and all_docs.rec_rank = 12
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
                 count(case
                           when
                                   all_docs.isd003
                                   and all_docs.datacovid_sevcovidarrival = '3'
                                   and all_docs.datacovid_sevcovidarrival::integer <
                                       all_docs.datacovid_maxsevcovidhospit::integer
                               then all_docs.docid
                           else null end)::integer
                                                                                                           as SeverityRiseHeavyKS,
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
                 count(distinct case
                                    when all_docs.datacovid_sevcovidarrival = '1' and all_docs.isd003
                                        then all_docs.docid end)                                           as SevCovidArrivalAll1,
                 count(distinct case
                                    when all_docs.datacovid_sevcovidarrival = '2' and all_docs.isd003
                                        then all_docs.docid end)                                           as SevCovidArrivalAll2,
                 count(distinct case
                                    when all_docs.datacovid_sevcovidarrival = '3' and all_docs.isd003
                                        then all_docs.docid end)                                           as SevCovidArrivalAll3,
                 count(distinct case
                                    when all_docs.datacovid_sevcovidarrival = '4' and all_docs.isd003
                                        then all_docs.docid end)                                           as SevCovidArrivalAll4,
                 count(distinct case
                                    when all_docs.datacovid_sevcovidarrival = '1' and all_docs.isd003 and
                                         ((all_docs.datacovid_sevcovidarrival =
                                           all_docs.datacovid_expertsevcovidarrival)
                                             or (all_docs.datacovid_approvsevcovidarrival = '1' and
                                                 all_docs.datacovid_expertsevcovidarrival is null))
                                        then
                                        all_docs.docid
                                    else null end)                                                         as ApprovSevCovidArrival1,
                 count(distinct case
                                    when all_docs.datacovid_sevcovidarrival = '2' and all_docs.isd003 and
                                         ((all_docs.datacovid_sevcovidarrival =
                                           all_docs.datacovid_expertsevcovidarrival)
                                             or (all_docs.datacovid_approvsevcovidarrival = '1' and
                                                 all_docs.datacovid_expertsevcovidarrival is null))
                                        then
                                        all_docs.docid
                                    else null end)                                                         as ApprovSevCovidArrival2,
                 count(distinct case
                                    when all_docs.datacovid_sevcovidarrival = '3' and all_docs.isd003 and
                                         ((all_docs.datacovid_sevcovidarrival =
                                           all_docs.datacovid_expertsevcovidarrival)
                                             or (all_docs.datacovid_approvsevcovidarrival = '1' and
                                                 all_docs.datacovid_expertsevcovidarrival is null))
                                        then
                                        all_docs.docid
                                    else null end)                                                         as ApprovSevCovidArrival3,
                 count(distinct case
                                    when all_docs.datacovid_sevcovidarrival = '4' and all_docs.isd003 and
                                         ((all_docs.datacovid_sevcovidarrival =
                                           all_docs.datacovid_expertsevcovidarrival)
                                             or (all_docs.datacovid_approvsevcovidarrival = '1' and
                                                 all_docs.datacovid_expertsevcovidarrival is null))
                                        then
                                        all_docs.docid
                                    else null end)                                                         as ApprovSevCovidArrival4,


                 count(distinct case
                                    when all_docs.datacovid_maxsevcovidhospit = '1' and all_docs.isd003
                                        then all_docs.docid end)                                           as MaxSevCovidHospitAll1,
                 count(distinct case
                                    when all_docs.datacovid_maxsevcovidhospit = '2' and all_docs.isd003
                                        then all_docs.docid end)                                           as MaxSevCovidHospitAll2,
                 count(distinct case
                                    when all_docs.datacovid_maxsevcovidhospit = '3' and all_docs.isd003
                                        then all_docs.docid end)                                           as MaxSevCovidHospitAll3,
                 count(distinct case
                                    when all_docs.datacovid_maxsevcovidhospit = '4' and all_docs.isd003
                                        then all_docs.docid end)                                           as MaxSevCovidHospitAll4,

                 count(distinct case
                                    when all_docs.datacovid_maxsevcovidhospit = '1' and all_docs.isd003 and
                                         ((all_docs.datacovid_maxsevcovidhospit =
                                           all_docs.datacovid_expertmaxsevcovidhospit)
                                             or (all_docs.datacovid_approvmaxsevcovidhospit = '1' and
                                                 all_docs.datacovid_expertmaxsevcovidhospit is null))
                                        then
                                        all_docs.docid
                                    else null end)                                                         as ApprovMaxSevCovidHospit1,
                 count(distinct case
                                    when all_docs.datacovid_maxsevcovidhospit = '2' and all_docs.isd003 and
                                         ((all_docs.datacovid_maxsevcovidhospit =
                                           all_docs.datacovid_expertmaxsevcovidhospit)
                                             or (all_docs.datacovid_approvmaxsevcovidhospit = '1' and
                                                 all_docs.datacovid_expertmaxsevcovidhospit is null))
                                        then
                                        all_docs.docid
                                    else null end)                                                         as ApprovMaxSevCovidHospit2,
                 count(distinct case
                                    when all_docs.datacovid_maxsevcovidhospit = '3' and all_docs.isd003 and
                                         ((all_docs.datacovid_maxsevcovidhospit =
                                           all_docs.datacovid_expertmaxsevcovidhospit)
                                             or (all_docs.datacovid_approvmaxsevcovidhospit = '1' and
                                                 all_docs.datacovid_expertmaxsevcovidhospit is null))
                                        then
                                        all_docs.docid
                                    else null end)                                                         as ApprovMaxSevCovidHospit3,
                 count(distinct case
                                    when all_docs.datacovid_maxsevcovidhospit = '4' and all_docs.isd003 and
                                         ((all_docs.datacovid_maxsevcovidhospit =
                                           all_docs.datacovid_expertmaxsevcovidhospit)
                                             or (all_docs.datacovid_approvmaxsevcovidhospit = '1' and
                                                 all_docs.datacovid_expertmaxsevcovidhospit is null))
                                        then
                                        all_docs.docid
                                    else null end)                                                         as ApprovMaxSevCovidHospit4,

                 count(distinct
                       case
                           when
                                   all_docs.isd003
                                   and round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) <=
                                       17
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as ZLNumber17KS,
                 count(distinct
                       case
                           when
                                   all_docs.isd003
                                   and round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) <=
                                       17
                                   and all_docs.datazl_gender = '2'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Female17NumberKS,
                 count(distinct
                       case
                           when
                                   all_docs.isd003
                                   and round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) <=
                                       17
                                   and all_docs.datazl_gender = '1'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Male17NumberKS,


                 count(distinct
                       case
                           when
                                   all_docs.isd003
                                   and round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) between 18 and 64
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as ZLNumber18_64KS,
                 count(distinct
                       case
                           when
                                   all_docs.isd003
                                   and round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) between 18 and 64
                                   and all_docs.datazl_gender = '2'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Female18_64NumberKS,
                 count(distinct
                       case
                           when
                                   all_docs.isd003
                                   and round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) between 18 and 64
                                   and all_docs.datazl_gender = '1'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Male18_64NumberKS,

                 count(distinct
                       case
                           when
                                   all_docs.isd003
                                   and round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) >=
                                       65
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as ZLNumber65KS,
                 count(distinct
                       case
                           when
                                   all_docs.isd003
                                   and round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) >=
                                       65
                                   and all_docs.datazl_gender = '2'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Female65NumberKS,
                 count(distinct
                       case
                           when
                                   all_docs.isd003
                                   and round(extract(year from
                                                     age(all_docs.datazl_datestartmp, all_docs.datazl_datebirth))) >=
                                       65
                                   and all_docs.datazl_gender = '1'
                               then all_docs.datazl_numpolicyoms
                           end)::integer                                                                   as Male65NumberKS,
                 -------------
                 sum(case
                         when all_docs.isd003
                            and all_docs.agemp between 18 and 64
                            and days > 5
                            and all_docs.rec_rank = 0
                             and exists(select 1
                                       from fs_datazl_associateddisease_st_list fdasl
                                       where fdasl.docid = all_docs.docid
                                         and (
                                               associateddiseasecode between 'I05' and 'I09*' or
                                               associateddiseasecode between 'I10' and 'I15*' or
                                               associateddiseasecode between 'I20' and 'I25*' or
                                               associateddiseasecode between 'I48' and 'I48*' or
                                               associateddiseasecode between 'I63' and 'I69*' or
                                               associateddiseasecode between 'E10' and 'E14*' or
                                               associateddiseasecode between 'J44' and 'J44*'
                                           )
                                 )
                         then 1
                         else 0 end)                                                                       as KS18_64,
                sum(case
                        when all_docs.isd003
                                 and all_docs.agemp between 18 and 64
                                 and days > 5
                                 and all_docs.rec_rank = 0
                            and exists(select 1
                                       from fs_datazl_associateddisease_st_list fdasl
                                       where fdasl.docid = all_docs.docid
                                         and (
                                               associateddiseasecode between 'I05' and 'I09*' or
                                               associateddiseasecode between 'I10' and 'I15*' or
                                               associateddiseasecode between 'I20' and 'I25*' or
                                               associateddiseasecode between 'I48' and 'I48*' or
                                               associateddiseasecode between 'I63' and 'I69*'
                                           )
                                 )
                            then 1
                        else 0 end)                                                                       as KS18_64SZ,
                sum(case
                        when all_docs.isd003
                                 and all_docs.agemp between 18 and 64
                                 and days > 5
                                 and all_docs.rec_rank = 0
                            and exists(select 1
                                       from fs_datazl_associateddisease_st_list fdasl
                                       where fdasl.docid = all_docs.docid
                                         and (
                                           associateddiseasecode between 'E10' and 'E14*'
                                           )
                                 )
                            then 1
                        else 0 end)                                                                       as KS18_64SD,
                sum(case
                        when all_docs.isd003
                                 and all_docs.agemp between 18 and 64
                                 and days > 5
                                 and all_docs.rec_rank = 0
                            and exists(select 1
                                       from fs_datazl_associateddisease_st_list fdasl
                                       where fdasl.docid = all_docs.docid
                                         and (
                                           associateddiseasecode between 'J44' and 'J44*'
                                           )
                                 )
                            then 1
                        else 0 end)                                                                       as KS18_64HOBL,

                 --------
                 sum(case
                         when all_docs.isd003
                             and all_docs.rec_rank = 13
                             then 1
                         else 0 end)                                                                       as KS18_64ORIT


          from all_docs
               --003_new_conditions_start
          where (all_docs.isd003 and all_docs.datacovid_app30daybeforehospit = '1')
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
          group by all_docs.maininfo_codetfoms),
     data_all as (select calc_data.codetfoms,
                         calc_data.ichlnumber,
                         calc_data.zlnumber,
                         calc_data.femalenumber,
                         calc_data.pregnantnumber,
                         calc_data.malenumber,
                         calc_data.deadnumber,
                         calc_data.deadnumberfemale,
                         calc_data.deadnumbermale,
                         coalesce(calc_data.averageage, 0)             as averageage,
                         coalesce(calc_data.averageagefemale, 0)       as averageagefemale,
                         coalesce(calc_data.averageagemale, 0)         as averageagemale,
                         calc_data.ksnumber,
                         calc_data.ksexpertisenumber,
                         calc_data.ksdisturbancenumber,
                         calc_data.femalenumberks,
                         calc_data.pregnantnumberks,
                         calc_data.malenumberks,
                         calc_data.deadnumberks,
                         calc_data.deadnumberfemaleks,
                         calc_data.deadnumbermaleks,
                         calc_data.nontypicallethalks,
                         calc_data.averageageks                        as averageageks,
                         calc_data.averageagefemaleks                  as averageagefemaleks,
                         calc_data.averageagemaleks                    as averageagemaleks,
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
                         calc_data.SeverityRiseHeavyKS,
                         calc_data.SevCovidArrivalAll1,
                         calc_data.SevCovidArrivalAll2,
                         calc_data.SevCovidArrivalAll3,
                         calc_data.SevCovidArrivalAll4,
                         calc_data.ApprovSevCovidArrival1,
                         calc_data.ApprovSevCovidArrival2,
                         calc_data.ApprovSevCovidArrival3,
                         calc_data.ApprovSevCovidArrival4,

                         calc_data.MaxSevCovidHospitAll1,
                         calc_data.MaxSevCovidHospitAll2,
                         calc_data.MaxSevCovidHospitAll3,
                         calc_data.MaxSevCovidHospitAll4,

                         calc_data.ApprovMaxSevCovidHospit1,
                         calc_data.ApprovMaxSevCovidHospit2,
                         calc_data.ApprovMaxSevCovidHospit3,
                         calc_data.ApprovMaxSevCovidHospit4,

                         calc_data.ZLNumber17KS,
                         calc_data.ZLNumber18_64KS,
                         calc_data.ZLNumber65KS,
                         calc_data.Male17NumberKS,
                         calc_data.Male18_64NumberKS,
                         calc_data.Male65NumberKS,
                         calc_data.Female17NumberKS,
                         calc_data.Female18_64NumberKS,
                         calc_data.Female65NumberKS,
                         calc_data.KS18_64,
                         calc_data.KS18_64SZ,
                         calc_data.KS18_64HOBL,
                         calc_data.KS18_64SD,
                         calc_data.KS18_64ORIT,
                         calc_data.VacHospitAll

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
                                 end                                    as gr,
                             sum(coalesce(KSExpertiseNumber, 0))        as KSExpertiseNumber,
                             sum(coalesce(MainDiseaseCovidKS, 0))       as MainDiseaseCovidKS,
                             null                                       as CovidKSProportion,
                             null                                       as AverageTreatNumberKS,
                             sum(coalesce(KSNumber, 0))                 as KSNumber,
                             sum(coalesce(MaleNumberKS, 0))             as MaleNumberKS,
                             null                                       as MaleProportionKS,
                             sum(coalesce(FemaleNumberKS, 0))           as FemaleNumberKS,
                             null                                       as FemaleProportionKS,
                             sum(coalesce(PregnantNumberKS, 0))         as PregnantNumberKS,
                             avg(AverageAgeKS)                          as AverageAgeKS,
                             avg(AverageAgeFemaleKS)                    as AverageAgeFemaleKS,
                             avg(AverageAgeMaleKS)                      as AverageAgeMaleKS,
                             sum(coalesce(VacBeforeHospit0, 0))         as VacBeforeHospit0,
                             null                                       as VacHospitProportion,
                             sum(coalesce(VacHospitAll, 0))             as VacHospitAll,
                             sum(coalesce(VacBeforeHospit1, 0))         as VacBeforeHospit1,
                             sum(coalesce(VacBeforeHospit2, 0))         as VacBeforeHospit2,
                             sum(coalesce(VacBeforeHospit3, 0))         as VacBeforeHospit3,
                             sum(coalesce(AssociatedDiseaseAllKS, 0))   as AssociatedDiseaseAllKS,
                             sum(coalesce(AssociatedDisease1KS, 0))     as AssociatedDisease1KS,
                             sum(coalesce(AssociatedDisease2KS, 0))     as AssociatedDisease2KS,
                             sum(coalesce(AssociatedDisease3KS, 0))     as AssociatedDisease3KS,
                             sum(coalesce(DeadNumberKS, 0))             as DeadNumberKS,
                             null                                       as DeadProportionKS,
                             sum(coalesce(NonTypicalLethalKS, 0))       as NonTypicalLethalKS,
                             null                                       as NonTypicalLethalProportionKS,
                             sum(coalesce(DeadNumberMaleKS, 0))         as DeadNumberMaleKS,
                             null                                       as DeadMaleProportionKS,
                             sum(coalesce(DeadNumberFemaleKS, 0))       as DeadNumberFemaleKS,
                             null                                       as DeadFemaleProportionKS,
                             avg(TreatTimingKS)                         as TreatTimingKS,
                             avg(TimeRecept)                            as TimeRecept,
                             sum(coalesce(APP30Day, 0))                 as APP30Day,
                             null                                       as APP30DayProportion,
                             sum(coalesce(SevCovidArrivalAll1, 0))      as SevCovidArrivalAll1,
                             sum(coalesce(ApprovSevCovidArrival1, 0))   as ApprovSevCovidArrival1,
                             null                                       as SevCovidArrivalProportion1,
                             sum(coalesce(SevCovidArrivalAll2, 0))      as SevCovidArrivalAll2,
                             sum(coalesce(ApprovSevCovidArrival2, 0))   as ApprovSevCovidArrival2,
                             null                                       as SevCovidArrivalProportion2,
                             sum(coalesce(SevCovidArrivalAll3, 0))      as SevCovidArrivalAll3,
                             sum(coalesce(ApprovSevCovidArrival3, 0))   as ApprovSevCovidArrival3,
                             null                                       as SevCovidArrivalProportion3,
                             sum(coalesce(SevCovidArrivalAll4, 0))      as SevCovidArrivalAll4,
                             sum(coalesce(ApprovSevCovidArrival4, 0))   as ApprovSevCovidArrival4,
                             null                                       as SevCovidArrivalProportion4,
                             sum(coalesce(MaxSevCovidHospitAll1, 0))    as MaxSevCovidHospitAll1,
                             sum(coalesce(ApprovMaxSevCovidHospit1, 0)) as ApprovMaxSevCovidHospit1,
                             null                                       as MaxSevCovidHospitProportion1,
                             sum(coalesce(MaxSevCovidHospitAll2, 0))    as MaxSevCovidHospitAll2,
                             sum(coalesce(ApprovMaxSevCovidHospit2, 0)) as ApprovMaxSevCovidHospit2,
                             null                                       as MaxSevCovidHospitProportion2,
                             sum(coalesce(MaxSevCovidHospitAll3, 0))    as MaxSevCovidHospitAll3,
                             sum(coalesce(ApprovMaxSevCovidHospit3, 0)) as ApprovMaxSevCovidHospit3,
                             null                                       as MaxSevCovidHospitProportion3,
                             sum(coalesce(MaxSevCovidHospitAll4, 0))    as MaxSevCovidHospitAll4,
                             sum(coalesce(ApprovMaxSevCovidHospit4, 0)) as ApprovMaxSevCovidHospit4,
                             null                                       as MaxSevCovidHospitProportion4,
                             sum(coalesce(SeverityRiseAllKS, 0))        as SeverityRiseAllKS,
                             null                                       as SeverityRiseAllKSProportion,
                             sum(coalesce(SeverityRiseLightKS, 0))      as SeverityRiseLightKS,
                             null                                       as SeverityRiseLightKSProportion,
                             sum(coalesce(SeverityRiseMediumKS, 0))     as SeverityRiseMediumKS,
                             null                                       as SeverityRiseMediumKSProportion,
                             sum(coalesce(SeverityRiseHeavyKS, 0))      as SeverityRiseHeavyKS,
                             null                                       as SeverityRiseHeavyKSProportion,
                             sum(coalesce(SeverityRiseNonLethalKS, 0))  as SeverityRiseNonLethalKS,
                             null                                       as SeverityRiseNonLethalKSProportion,
                             sum(coalesce(ZLNumber17KS, 0))             as ZLNumber17KS,
                             sum(coalesce(ZLNumber18_64KS, 0))          as ZLNumber18_64KS,
                             sum(coalesce(ZLNumber65KS, 0))             as ZLNumber65KS,
                             sum(coalesce(Male17NumberKS, 0))           as Male17NumberKS,
                             sum(coalesce(Male18_64NumberKS, 0))        as Male18_64NumberKS,
                             sum(coalesce(Male65NumberKS, 0))           as Male65NumberKS,
                             sum(coalesce(Female17NumberKS, 0))         as Female17NumberKS,
                             sum(coalesce(Female18_64NumberKS, 0))      as Female18_64NumberKS,
                             sum(coalesce(Female65NumberKS, 0))         as Female65NumberKS,
                             sum(coalesce(KS18_64, 0))                  as KS18_64,
                             sum(coalesce(KS18_64SZ, 0))                as KS18_64SZ,
                             sum(coalesce(KS18_64HOBL, 0))              as KS18_64HOBL,
                             sum(coalesce(KS18_64SD, 0))                as KS18_64SD,
                             sum(coalesce(KS18_64ORIT, 0))              as KS18_64ORIT

                      from ufos.DC_RFOMS_F001 f001
                               left join data_all on data_all.codetfoms = f001.tf_kod
                               left join ufos.dc_rfoms_f010 f010
                                         on f001.tf_okato = f010.kod_okato and f010.dateend is null
                      group by
                          rollup (f010.okrug, f010.kod_tf))
select f010.kod_tf                                       as CodeTfoms,
       f010.subname                                      as SubjectName,
       coalesce(KSExpertiseNumber, 0)                    as KSExpertiseNumber,
       coalesce(MainDiseaseCovidKS, 0)                   as MainDiseaseCovidKS,
       null                                              as CovidKSProportion,
       null                                              as AverageTreatNumberKS,
       null                                              as Total89nKS,
       coalesce(KS18_64, 0)                              as KS18_64,
       --null                                              as KS18_64,
       coalesce(KS18_64SZ, 0)                            as KS18_64SZ,
       coalesce(KS18_64HOBL, 0)                          as KS18_64HOBL,
       coalesce(KS18_64SD, 0)                            as KS18_64SD,
       coalesce(app30day, 0)                             as app30day,
       coalesce(KS18_64ORIT, 0)                          as KS18_64ORIT,
       coalesce(deadnumberks, 0)                         as deadnumberks,
       null                                              as deadproportionks,
       coalesce(nontypicallethalks, 0)                   as nontypicallethalks,
       null                                              as nontypicallethalproportionks,
       coalesce(KSNumber, 0)                             as KSNumber,
       coalesce(vacbeforehospit0, 0)                     as vacbeforehospit0,
       coalesce(vachospitall, 0)                         as vachospitall,
       null                                              as vachospitproportion,
       coalesce(AssociatedDiseaseAllKS, 0)               as AssociatedDiseaseAllKS,
       coalesce(AssociatedDisease1KS, 0)                 as AssociatedDisease1KS,
       coalesce(AssociatedDisease2KS, 0)                 as AssociatedDisease2KS,
       coalesce(AssociatedDisease3KS, 0)                 as AssociatedDisease3KS,
       coalesce(treattimingks, 0)::integer               as treattimingks,
       coalesce(to_char(TimeRecept, 'HH24:MI'), '00:00') as TimeRecept,
       coalesce(sevcovidarrivalall1, 0)                  as sevcovidarrivalall1,
       coalesce(approvsevcovidarrival1, 0)               as approvsevcovidarrival1,
       null                                              as sevcovidarrivalproportion1,
       coalesce(sevcovidarrivalall2, 0)                  as sevcovidarrivalall2,
       coalesce(approvsevcovidarrival2, 0)               as approvsevcovidarrival2,
       null                                              as sevcovidarrivalproportion2,
       coalesce(sevcovidarrivalall3, 0)                  as sevcovidarrivalall3,
       coalesce(approvsevcovidarrival3, 0)               as approvsevcovidarrival3,
       null                                              as sevcovidarrivalproportion3,
       coalesce(sevcovidarrivalall4, 0)                  as sevcovidarrivalall4,
       coalesce(approvsevcovidarrival4, 0)               as approvsevcovidarrival4,
       null                                              as sevcovidarrivalproportion4,
       coalesce(maxsevcovidhospitall1, 0)                as maxsevcovidhospitall1,
       coalesce(approvmaxsevcovidhospit1, 0)             as approvmaxsevcovidhospit1,
       null                                              as maxsevcovidhospitproportion1,
       coalesce(maxsevcovidhospitall2, 0)                as maxsevcovidhospitall2,
       coalesce(approvmaxsevcovidhospit2, 0)             as approvmaxsevcovidhospit2,
       null                                              as maxsevcovidhospitproportion2,
       coalesce(maxsevcovidhospitall3, 0)                as maxsevcovidhospitall3,
       coalesce(approvmaxsevcovidhospit3, 0)             as approvmaxsevcovidhospit3,
       null                                              as maxsevcovidhospitproportion3,
       coalesce(maxsevcovidhospitall4, 0)                as maxsevcovidhospitall4,
       coalesce(approvmaxsevcovidhospit4, 0)             as approvmaxsevcovidhospit4,
       null                                              as maxsevcovidhospitproportion4,
       coalesce(severityriseallks, 0)                    as severityriseallks,
       null                                              as severityriseallksproportion,
       coalesce(severityriselightks, 0)                  as severityriselightks,
       null                                              as severityriselightksproportion,
       coalesce(severityrisemediumks, 0)                 as severityrisemediumks,
       null                                              as severityrisemediumksproportion,
       coalesce(severityriseheavyks, 0)                  as severityriseheavyks,
       null                                              as severityriseheavyksproportion,
       coalesce(severityrisenonlethalks, 0)              as severityrisenonlethalks,
       null                                              as severityrisenonlethalksproportion


from ufos.v_icl_report_f010 f010
         left join order_report on f010.kod_tf = order_report.kod_tf or f010.sort = order_report.gr

order by f010.sort
;