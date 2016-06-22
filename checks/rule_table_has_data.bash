#!/usr/bin/env bash

sql=" SELECT 0                  \
      FROM ${hapinsp_table}     \
      LIMIT 1                   \
     "

if [ "${hapinsp_ssl}" = "True" ]; then
    sslopt='--ssl'
else
    sslopt=''
fi

violations=` impala-shell -i ${hapinsp_instance} -d ${hapinsp_database} -B --quiet ${sslopt} -q "$sql" 2>/dev/null`
rc=$?
if [ "$rc" != "0" ]; then
    rc="1"
elif [ "$violations" != "0" ]; then
    violations="1"
fi

echo {'"'violations'"':'"'$violations'"', '"'rc'"':'"'$rc'"'}
#echo `hapinsp_formatter.py --rc 0 --violation-cnt $violations`
