.WIDTH 256;
.LOGON $PARAM{'HOSTNM'}/$PARAM{'USERNM'},$PARAM{'PASSWD'};

DELETE FROM $PARAM{'CMSSDB'}.PZ_LIST_HIS WHERE LD_NBR = '$PARAM{'ld_nbr'}';

-- 导入根据投资者三级分类筛选的一码通。
INSERT INTO $PARAM{'CMSSDB'}.PZ_LIST_HIS (OAP_ACCT_NBR, LD_NBR)
SELECT A.OAP_ACCT_NBR, '$PARAM{'ld_nbr'}'
FROM NsoVIEW.CSDC_INTG_SEC_ACCT A
LEFT join NSPVIEW.ACT_STK_INVST_CLSF_HIS B
ON A.OAP_ACCT_NBR = B.OAP_ACCT_NBR
WHERE B.CLSF_3 in ($PARAM{'prmt_val_quote'})     --投资者三级分类 
AND A.OAP_ACCT_NBR IS NOT NULL
AND TRIM(A.OAP_ACCT_NBR) <> ''
AND A.E_DATE >= CAST('$PARAM{'s_date'}' AS DATE FORMAT 'YYYYMMDD')
GROUP BY A.OAP_ACCT_NBR
;

SELECT * FROM $PARAM{'CMSSDB'}.PZ_LIST_HIS
WHERE LD_NBR = '$PARAM{'ld_nbr'}';

.QUIT;

