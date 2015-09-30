#!/usr/bin/env bash

sql=" SELECT COUNT(*)                                                        \
      FROM ${hapinsp_table}                                                  \
      WHERE ${hapinsp_checkcustom_col} <> ${hapinsp_checkcustom_case}(${hapinsp_checkcustom_col})   \
     "

violations=` impala-shell -i ${hapinsp_instance} -d ${hapinsp_database} -B --quiet -q "$sql" `

echo {'"'violations'"':'"'$violations'"', '"'rc'"':'"'0'"'}
#echo `hapinsp_formatter.py --rc 0 --violation-cnt $violations`
