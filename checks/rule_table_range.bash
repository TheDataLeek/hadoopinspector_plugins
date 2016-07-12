#!/usr/bin/env bash

sql=" SELECT COUNT(*)                                               \
      FROM ${hapinsp_table}                                         \
      WHERE ${hapinsp_checkcustom_col} < ${hapinsp_checkcustom_min} \
         OR ${hapinsp_checkcustom_col} > ${hapinsp_checkcustom_max} \
     "

if [ "${hapinsp_ssl}" = "True" ]; then
    sslopt='--ssl'
else
    sslopt=''
fi

violations=` impala-shell -i ${hapinsp_instance} -d ${hapinsp_database} -B ${sslopt} --quiet -q "$sql" 2>/dev/null`

echo {'"'violations'"':'"'$violations'"', '"'rc'"':'"'0'"'}
