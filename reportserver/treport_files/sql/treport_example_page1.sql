select d.docnum, to_char(d.docdate, 'dd.mm.yyyy'), d.docsum, d.docsum2  from treport.documents d
where d.docdate between to_date('{{ p_start_date }}', 'dd.mm.yyyy') and to_date('{{ p_end_date }}', 'dd.mm.yyyy')
order by d.docdate asc;