#!/usr/bin/env bash

sql=" SELECT COUNT(*)                                                        \
      FROM ${hapinsp_table}                                                  \
      WHERE ${hapinsp_checkcustom_col} <> ${hapinsp_checkcustom_case}(${hapinsp_checkcustom_col})   \
     "

if [ "${hapinsp_ssl}" = "True" ]; then
    sslopt='--ssl'
else
    sslopt=''
fi

violations=` impala-shell -i ${hapinsp_instance} -d ${hapinsp_database} -B --quiet ${sslopt} -q "$sql" 2>/dev/null`

echo {'"'violations'"':'"'$violations'"', '"'rc'"':'"'0'"'}
#echo `hapinsp_formatter.py --rc 0 --violation-cnt $violations`
