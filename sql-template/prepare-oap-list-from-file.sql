.WIDTH 256;
.LOGON $PARAM{'HOSTNM'}/$PARAM{'USERNM'},$PARAM{'PASSWD'};

DELETE FROM $PARAM{'CMSSDB'}.PZ_LIST_HIS WHERE LD_NBR = '$PARAM{'ld_nbr'}';

-- 导入上传文件中包含的一码通。
INSERT INTO $PARAM{'CMSSDB'}.PZ_LIST_HIS (OAP_ACCT_NBR, LD_NBR)
SELECT OAP_ACCT_NBR, '$PARAM{'ld_nbr'}'
FROM	$PARAM{'TEMP_DB'}.$PARAM{'LOAD_TBL'}
WHERE OAP_ACCT_NBR IS NOT NULL
;

SELECT * FROM $PARAM{'CMSSDB'}.PZ_LIST_HIS
WHERE LD_NBR = '$PARAM{'ld_nbr'}';

.QUIT;

