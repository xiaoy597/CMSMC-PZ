#!/usr/bin/perl
##�汾��Ϣ: v1.0
##������  : wxh+lc
##��������: ����
##��������: ���ֹ���
##��������: һ��ͨ�б�(�ļ���) + �������Σ�12λ�ַ���YYYYMMDD+��λ��ţ�+ ͳ����ʼʱ�� + ͳ����ֹʱ��
##��������ʾ��:  perl tst.pl 2.dat 201709190001 20170601 20170731 
##Ŀ���  : PZ_OAP_KPI
##Ƶ��    : ������ִ��		
##��������: 2017-09-08

use strict;
use warnings FATAL => 'all';
use DBI;
#use Config::IniFiles;
use DBD::ODBC;
use Time::localtime;

#����ȫ�ֱ���
#Variable Section
	
	my $LOG_FILE;
	my $START_TIME;
	my $END_TIME;
	my $ret;
	
	my $MAX_DT = '30001231';
	my $PZ_DATA = 'WXHDATA';
	my $HOSTNM = '10.96.4.7';								##���ʼ����Ŀ�����ݿ�
	my $USERNM = 'UAT_WANXH';							##ִ�����ʼ���ű������ݿ��û�
	my $PASSWD = 'yedifwos3A';							##ִ�����ʼ���ű������ݿ��û����롣ƽʱ���գ�ִ��ǰ�������룬�����ִ�У�ִ�к������ա�
	my $PZ_TEMP = 'WXHDATA';
	my $FastldDDL = 'PZ_FASTLD';	
		
#�����������
	my $OAP_FILE = $ARGV[0];	
	my $LD_NBR = $ARGV[1];
	my $S_DT = $ARGV[2];
	my $E_DT = $ARGV[3];	

	my $Fastld_temptbl = 'pz_fastld_'.$LD_NBR;
	my $Fastld_err1 = 'pz_fastld_'.$LD_NBR.'E1';
	my $Fastld_err2 = 'pz_fastld_'.$LD_NBR.'E2';

sub run_fastload_command{

    my $rc = open(FASTLOAD, "| fastload");
    
    #�ж�FASTLOAD���������Ƿ���ȷִ��
    unless ($rc) {
        print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
        print "Could not invoke FASTLOAD command\n";
        return undef;
    }

    #FASTLOAD���ؽű�
    print FASTLOAD <<ENDOFINPUT;
    
    /*���ƴ����¼�� */
    SESSIONS 8;
    ERRLIMIT 1; 
    
    logon $HOSTNM/$USERNM,$PASSWD;
    
    /*ɾ��Ŀ���*/
    DROP TABLE $PZ_TEMP.$Fastld_temptbl;
    
    /*ɾ�������1*/
    DROP TABLE $PZ_TEMP.$Fastld_err1;
    
    /*ɾ�������2*/
    DROP TABLE $PZ_TEMP.$Fastld_err2;
    
    /*����Ŀ���*/
    CREATE TABLE $PZ_TEMP.$Fastld_temptbl AS $PZ_TEMP.$FastldDDL WITH NO DATA ;
    
    SET RECORD VARTEXT "," DISPLAY_ERRORS;
    
    DEFINE
    
     OAP_ACCT_NBR              (varchar(25))
  
    FILE=$OAP_FILE;
  
    begin loading $PZ_TEMP.$Fastld_temptbl errorfiles $PZ_TEMP.$Fastld_err1, $PZ_TEMP.$Fastld_err2;
    insert into $PZ_TEMP.$Fastld_temptbl(
      OAP_ACCT_NBR
     )
     values
	(
	:OAP_ACCT_NBR
	);
end loading;
logoff;
.QUIT 0;

ENDOFINPUT

    #FASTLOAD���ؽű�����
    close(FASTLOAD);
    my $RET_CODE = $? >> 8;
    print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
    print "fastload returned $RET_CODE\n";
    # �������ֵΪ12��������ش����򷵻�1�����򷵻�0
    if ( $RET_CODE == 12 ) {
       return 1;
    }else {
        return 0;
    }
}

#���ʼ���
sub pz_calc {
 
my $rc = open(BTEQ, "| bteq");

unless ($rc) {
    print "Could not invoke BTEQ command\n";
    return -1;
}

print BTEQ <<ENDOFINPUT;

.WIDTH 254;
.LOGON $HOSTNM/$USERNM,$PASSWD;

----------------STEP 1		����ָ��������Σ���־��
------------------------��־������ָ��������Σ��ֶΣ��������Ρ�ͳ����ʼʱ��+ͳ����ֹʱ�䡢��ǰ���ڡ���ǰʱ�䡢��ǰ���ݿ��û���������������UPI
INSERT INTO $PZ_DATA.PZ_LOG(LD_NBR,S_DATE,E_DATE,PRMT_TYPE,PRMT_VAL) 
values('${LD_NBR}'
,CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD')
,CAST('${E_DT}' AS DATE FORMAT 'YYYYMMDD')
,'1'
,'${OAP_FILE}'
);

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------STEP 2		����ָ��������Σ�һ��ͨ
INSERT INTO $PZ_DATA.PZ_LIST_HIS
select OAP_ACCT_NBR,'${LD_NBR}'
from $PZ_TEMP.$Fastld_temptbl;

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------�Ѽ�ͳ����Ϣ

COLLECT STATISTICS COLUMN OAP_ACCT_NBR ON $PZ_DATA.PZ_LIST_HIS;

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------STEP3	һ��ͨ��֤ȯ�˺Ŷ�Ӧ��ϵ
------------------------��ȡʧЧ����>ͳ�����ڿ�ʼʱ��һ��ͨ��֤ȯ�˺���ʷ����ȷ��ACT_STK_INVST_CLSF_HIS����һ��ͨ��֤ȯ�˺š���Ч���ڡ�ʧЧ��������Ψһ��
------------------------ע������ͬһ֤ȯ�˻������������������ڼ�¼�����������ʵ������ͨ��OAP_ACCT_NBRΪ�ջ�''�ų�

CREATE VOLATILE MULTISET TABLE VT_OAP_SEC_ACCT AS(
	SELECT A.OAP_ACCT_NBR
		,A.SEC_ACCT
		,A.SEC_ACCT_NAME
	FROM NsoVIEW.CSDC_INTG_SEC_ACCT A, $PZ_DATA.PZ_LIST_HIS B
	WHERE B.LD_NBR = '${LD_NBR}'
	AND A.OAP_ACCT_NBR = B.OAP_ACCT_NBR
	AND A.OAP_ACCT_NBR IS NOT NULL
	AND TRIM(A.OAP_ACCT_NBR) <> ''
	AND A.E_DATE >= CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD')
	GROUP BY 1,2,3
)WITH DATA PRIMARY INDEX(SEC_ACCT )
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------�Ѽ�ͳ����Ϣ

COLLECT STATISTICS COLUMN SEC_ACCT ON VT_OAP_SEC_ACCT;

.IF ERRORCODE <> 0 THEN .QUIT 12;

COLLECT STATISTICS COLUMN OAP_ACCT_NBR ON VT_OAP_SEC_ACCT;

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------STEP4	��ȡһ��ͨ����֤ȯ�˺�A�ɽ�����ϸ�������뽻����ϸ��ص�����ָ�꣨1~18��

----------------STEP4.1	A�ɹ�Ʊ��ʷ��ʧЧ����>ͳ�����ڿ�ʼʱ�䣩

CREATE VOLATILE MULTISET TABLE VT_SEC_A AS(
	SELECT	mkt_sort
		,sec_cde
	FROM	NSOVIEW.CSDC_INTG_SEC_INFO				--CSDC_J0013_����_֤ȯ��Ϣ��
	WHERE	 e_date > CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD')
	  AND	sec_ctg ='11' 												--A��
	  AND	sec_reg_sts_sort NOT IN ('2','5')					--֤ȯ�Ǽ�״̬��� 2�����С�5������
	  GROUP BY 1,2
)WITH DATA PRIMARY INDEX(sec_cde)
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .QUIT 12;

COLLECT STATISTICS COLUMN sec_cde ON VT_SEC_A;

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------STEP4.2	ͳ����ʼ��������ֹ���ڣ�һ��ͨ�����˻����Ͻ����Ľ��ף����������ڡ�һ��ͨ�˻���֤ȯ�˻�����Ʊ������ܽ��״������걨��Ԫ+�걨���룩�������������ɣ������׽�Ԫ��

CREATE VOLATILE MULTISET TABLE VT_TRAN_A AS(
	SELECT	
			a1.trad_date																												--�ɽ�����
			,a2.OAP_ACCT_NBR 
			,a1.shdr_acct	 AS sec_acct																							--�ɶ��˻�
			,'0'	AS sec_exch_cde																									--����������,����
			,SUBSTR(CAST(a1.sec_cde + 1000000 AS CHAR(7)),2)	AS sec_cde							--֤ȯ����
			,SUM(ABS(CAST(a1.trans_vol *a1.tran_prc AS DECIMAL(24,6)))) AS tran_amt			--���׽��
			,SUM(CAST(ABS(a1.trans_vol) AS DECIMAL(24,0)))	AS	tran_vol							--��������
			,COUNT(distinct SEAT_CDE||APLY_NBR)	AS	tran_cnt												--���ױ���
		FROM	nsoview.CSDC_H_SEC_TRAN a1,																			--CSDC_J1015_��_֤ȯ������ 
				VT_OAP_SEC_ACCT a2,
				VT_SEC_A a3
		WHERE  a1.shdr_acct=a2.SEC_ACCT
			  AND	a1.trad_date between CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD') AND  CAST('${E_DT}' AS DATE FORMAT 'YYYYMMDD')
	  		AND	a1.trans_type IN ('00A', '001') 
	  		and a1.tran_prc <> 0					--�������ͣ�00A:��ͨ���� 001:�����գ�
	  		and a1.trans_vol <> 0	
	  		and a1.CAP_TYPE ='PT' 
	  		and a1.NEGT_TYPE = '0'
	  		AND	a1.equt_type NOT IN ('DF','DX','HL')																		--Ȩ�����ͣ�DF:�Ҹ� DX:��Ϣ HL:������
	  		AND SUBSTR(CAST(a1.sec_cde + 1000000 AS CHAR(7)),2)	= A3.sec_cde
		GROUP by 1,2,3,4,5
		UNION ALL
		SELECT	
			A1.trad_date																												--�ɽ�����
			,A2.OAP_ACCT_NBR 
			,A1.sal_shdr_acct AS sec_acct 																					--֤ȯ�˻�
			,'0'		AS	sec_exch_cde 																							--֤ȯ����������
			,SUBSTR(CAST(A1.sec_cde + 1000000 AS char(7)),2)	AS	sec_cde							--֤ȯ����
			,SUM(CAST((ABS(A1.trad_vol)) * tran_prc AS DECIMAL(24,6)))		AS tran_amt			--���׽��
			,SUM(CAST(ABS(A1.trad_vol)AS DECIMAL(24,0)))		AS tran_vol								--��������
			,COUNT(distinct A1.SAL_SEAT||A1.SAL_DECL_NBR )		AS tran_cnt							--���ױ���
		FROM	NSOVIEW.CSDC_H_SEC_TRAD	A1,  
				VT_OAP_SEC_ACCT A2,
				VT_SEC_A a3
		WHERE A1.sal_shdr_acct=A2.SEC_ACCT 
			AND	A1.trad_date between CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD') 	AND  CAST('${E_DT}' AS DATE FORMAT 'YYYYMMDD')
			AND	A1.sal_shdr_acct LIKE 'E%'																					--�����ɶ��˻�
			AND	SUBSTR(A1.memo,2,1) ='6'
	    AND SUBSTR(CAST(a1.sec_cde + 1000000 AS CHAR(7)),2)	= A3.sec_cde		
		GROUP BY 1,2,3,4,5
		UNION ALL
		SELECT	A1.trad_date																										--�ɽ�����
			,A2.OAP_ACCT_NBR 
			,A1.b_shr_acct	AS	sec_acct																						--֤ȯ�˻�
			,'0'		AS	sec_exch_cde																							--֤ȯ���������� 0���Ͻ���
			,SUBSTR(CAST(A1.sec_cde + 1000000 AS char(7)),2)	AS	sec_cde							--֤ȯ����
			,SUM(CAST((ABS(A1.trad_vol)) * tran_prc AS DECIMAL(24,6)))		AS tran_amt			--���׽��
			,SUM(CAST(ABS(A1.trad_vol) AS DECIMAL(24,0)))	AS	trad_vol								--��������
			,COUNT(distinct A1.BUY_SEAT||A1.BUY_DECL_NBR)	AS	trad_cnt							--���ױ���
		FROM	nsoview.CSDC_H_SEC_TRAD A1,																			--CSDC_J1014_��_֤ȯ�ɽ���
				VT_OAP_SEC_ACCT A2,
				VT_SEC_A a3				
		WHERE A1.b_shr_acct=A2.SEC_ACCT 
			AND	trad_date between CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD') AND  CAST('${E_DT}' AS DATE FORMAT 'YYYYMMDD')
			AND	b_shr_acct LIKE 'E%'																							--�򷽹ɶ��˻�
			AND	SUBSTR(memo,1,1) IN ('5','7')
	  	and SUBSTR(CAST(a1.sec_cde + 1000000 AS CHAR(7)),2)	= A3.sec_cde					
		GROUP by 1,2,3,4,5
)WITH DATA PRIMARY INDEX(sec_acct)
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------STEP4.3	ͳ����ʼ��������ֹ���ڣ�һ��ͨ�����˻�������Ľ��ף����������ڡ�һ��ͨ�˻���֤ȯ�˻�����Ʊ������ܽ��״������걨��Ԫ+�걨���룩�������������ɣ������׽�Ԫ��

INSERT INTO VT_TRAN_A 
	SELECT	A1.trad_date
		,A2.oap_acct_nbr
		,A1.shdr_acct AS sec_acct 																								--֤ȯ�˺�
		,'1'	AS sec_exch_cde																										--�������������
		,SUBSTR(CAST(A1.sec_cde + 1000000 AS CHAR(7)),2)		AS sec_cde							--֤ȯ����
		,SUM(CAST((ABS(trad_vol)) * tran_prc AS DECIMAL(24,6)))		AS tran_amt					--���׽��
		,SUM(CAST(ABS(trad_vol) AS DECIMAL(24,0)))	AS	tran_vol										--��������
		,COUNT(distinct CNTR_NBR)	AS	tran_cnt																	--���ױ���
	FROM	NSOVIEW.CSDC_S_SEC_TRAN A1,																			--CSDC_J2018_��_֤ȯ������
			VT_OAP_SEC_ACCT A2,
			VT_SEC_A A3							
	WHERE A1.shdr_acct=A2.SEC_ACCT 
		AND A1.trad_date between CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD') AND  CAST('${E_DT}' AS DATE FORMAT 'YYYYMMDD')
	  and SUBSTR(CAST(a1.sec_cde + 1000000 AS CHAR(7)),2)	= A3.sec_cde							
	GROUP BY 1,2,3,4,5;

.IF ERRORCODE <> 0 THEN .QUIT 12;

COLLECT STATISTICS COLUMN sec_acct ON VT_TRAN_A;

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------STEP4.4	�����뽻����ϸ��ص�����ָ�꣨1~18��

CREATE VOLATILE MULTISET TABLE VT_PZ_OAP_KPI_1 AS(
SELECT OAP_ACCT_NBR
	--,SUM(A3)														
	,COUNT(TRAD_DATE) TCNT1																										--�������׵�����
	,CAST(SUM(A3)/TCNT				AS DECIMAL(24,4))						AS A01										--ָ��1��һ��ͨ/��������			�վ����״���=��mi/ni
	,CAST(AVG(A3)						AS DECIMAL(24,4))						AS A02										--ָ��2�����״�����ֵ					���״�����ֵ=�ƣ�ÿ�콻�״�����/�������׵�����
	,CAST(MAX(A3)						AS DECIMAL(24,4))						AS A03										--ָ��3������״���					����״���=max��ÿһ��Ľ��״�����
	,CAST(STDDEV_SAMP(A3)		AS DECIMAL(24,4))						AS A04										--ָ��4�����״�����׼��
	,CAST(SKEW(A3)						AS DECIMAL(24,4))						AS A05										--ָ��5�����״���ƫ��
	,CAST(KURTOSIS(A3)+3			AS DECIMAL(24,4))						AS A06										--ָ��6�����״������
	,CAST(SUM(A4)/TCNT				AS DECIMAL(24,4))						AS A07										--ָ��7���վ����׽���Ԫ��		�վ����׽��=�ƣ�ÿ�ս��׽�/������
	,CAST(AVG(A4)						AS DECIMAL(24,4))						AS A08										--ָ��8�����׽���ֵ����Ԫ��		���׽���ֵ=�ƣ�ÿ�ʽ��׽�/�������׵ı���
	,CAST(MAX(A4)						AS DECIMAL(24,4))						AS A09										--ָ��9������׽���Ԫ��		����׽��=max��ÿ�ʽ��׽�
	,CAST(STDDEV_SAMP(A4)		AS DECIMAL(24,4))						AS A10										--ָ��10�����׽���׼��				
	,CAST(SKEW(A4)						AS DECIMAL(24,4))						AS A11										--ָ��11�����׽��ƫ��
	,CAST(KURTOSIS(A4)+3			AS DECIMAL(24,4))						AS A12										--ָ��12�����׽����					matlab�з�Ⱥ�����3Ϊ��λ����teradata�ķ�Ⱥ�����0Ϊ��λ������matlabΪ׼���˴�+3
FROM (                        	
		SELECT 
			OAP_ACCT_NBR
			,TRAD_DATE
			,CAST(COUNT(DISTINCT SEC_ACCT) 						AS DECIMAL(24,4))			A1		--�������׵Ĺ�Ʊ�˻�����/һ��ͨ/��������
			,CAST(COUNT(DISTINCT SEC_CDE) 						AS DECIMAL(24,4))			A2		--�����Ĺ�Ʊ����/һ��ͨ/��������
			,CAST(SUM(TRAN_CNT) 											AS DECIMAL(24,4))			A3		--���״���/һ��ͨ/��������
			,CAST(SUM(TRAN_AMT)/10000 								AS DECIMAL(24,4))			A4		--���׽��/һ��ͨ/��������			
		FROM VT_TRAN_A
		GROUP BY 1,2) A
		,(
			SELECT COUNT(*)  TCNT
			FROM NSPVIEW.PTY_TRAD_CLND
			WHERE IF_TRADDAY =1
        		AND CALENDAR_DATE between CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD') AND  CAST('${E_DT}' AS DATE FORMAT 'YYYYMMDD')
        	) B 
GROUP BY 1)
WITH DATA PRIMARY INDEX(OAP_ACCT_NBR)
ON COMMIT PRESERVE ROWS;		

.IF ERRORCODE <> 0 THEN .QUIT 12;

CREATE VOLATILE MULTISET TABLE VT_PZ_OAP_KPI_2 AS(
SELECT OAP_ACCT_NBR
	,CAST(COUNT(DISTINCT SEC_CDE) AS DECIMAL(24,4))/TCNT 							AS A13		--ָ��13���վ����׹�Ʊֻ��			�վ����׹�Ʊֻ��=�ƣ�ÿ�ս��׹�Ʊֻ����/������
	,MAX(T14)							AS A14																						--ָ��14����������״�����ֵ			max�����ɽ��״�����ֵ��
	,MAX(T15)							AS A15																						--ָ��15����������״�����׼��			max�����ɽ��״�����׼�
FROM (
		SELECT OAP_ACCT_NBR
			,SEC_CDE
			,CAST(AVG(A3)						AS DECIMAL(24,4))		AS T14									
			,CAST(STDDEV_SAMP(A3)		AS DECIMAL(24,4))		AS T15									
		FROM (
				SELECT 
					OAP_ACCT_NBR																--һ��ͨ
					,SEC_CDE																			--��Ʊ����
					,TRAD_DATE																	--��������
					,CAST(SUM(TRAN_CNT)		AS DECIMAL(24,4))		A3	--���״���/һ��ͨ/��Ʊ/��������
				FROM VT_TRAN_A
				GROUP BY 1,2,3 ) A
		GROUP BY 1,2 ) B
		,(
			SELECT COUNT(*)  TCNT
			FROM NSPVIEW.PTY_TRAD_CLND
			WHERE IF_TRADDAY =1
        		AND CALENDAR_DATE between CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD') AND  CAST('${E_DT}' AS DATE FORMAT 'YYYYMMDD')
        	) C
GROUP BY 1)
WITH DATA PRIMARY INDEX(OAP_ACCT_NBR)
ON COMMIT PRESERVE ROWS;			

.IF ERRORCODE <> 0 THEN .QUIT 12;
		
CREATE VOLATILE MULTISET TABLE VT_PZ_OAP_KPI_3 AS(		
SELECT OAP_ACCT_NBR
	,COUNT(*)							AS A16													--ָ��16��һ��ͨ�˻�����
	,MAX(T17)							AS A17													--ָ��17���˻�����״�����ֵ		max���˻����״�����ֵ��
	,MAX(T18)							AS A18													--ָ��18���˻�����״�����׼��		max���˻����״�����׼�
FROM (
		SELECT OAP_ACCT_NBR
			,SEC_ACCT
			,CAST(AVG(A3)						AS DECIMAL(24,4))	AS T17								
			,CAST(STDDEV_SAMP(A3)		AS DECIMAL(24,4))	AS T18								
		FROM (
				SELECT 
					OAP_ACCT_NBR																		--һ��ͨ
					,SEC_ACCT																				--֤ȯ�˻�
					,TRAD_DATE																			--��������
					,CAST(SUM(TRAN_CNT) 	AS DECIMAL(24,4))		A3				--���״���/һ��ͨ/֤ȯ�˻�/��������
				FROM VT_TRAN_A
				GROUP BY 1,2,3 ) A
		GROUP BY 1,2 ) B
GROUP BY 1)
WITH DATA PRIMARY INDEX(OAP_ACCT_NBR)
ON COMMIT PRESERVE ROWS;						

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------STEP5	��ȡһ��ͨ�����ʽ��˻����ʽ��ȡ��¼���������ʽ��ȡ��ص�����ָ�꣨19~24��

create VOLATILE MULTISET TABLE VT_OAP_BANKRL as (
		select C.OAP_ACCT_NBR
			,B.SC_CDE
			,B.BANKRL_ACCT_ACCT_NBR
			,B.SEC_ACCT_ACCT_NBR
			FROM nsoview.IPF_BANKRL_AND_SEC_ACCT_CORR B
			 ,VT_OAP_SEC_ACCT C			
      where B.SEC_ACCT_ACCT_NBR = C.SEC_ACCT
      and B.E_DATE >= CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD')
		QUALIFY RANK() OVER(PARTITION BY SC_CDE,BANKRL_ACCT_ACCT_NBR ORDER BY OAP_ACCT_NBR DESC,SEC_ACCT_ACCT_NBR DESC) = 1
		) with data primary index(BANKRL_ACCT_ACCT_NBR)
		ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN BANKRL_ACCT_ACCT_NBR ON VT_OAP_BANKRL;

.IF ERRORCODE <> 0 THEN .QUIT 12;

CREATE VOLATILE MULTISET TABLE VT_PZ_OAP_KPI_4 AS(
select OAP_ACCT_NBR
	,CAST(SUM(A1)/TCNT						AS DECIMAL(24,4))				AS A19									--ָ��19��һ��ͨ/��������			�վ����״���=��mi/ni
	,CAST(AVG(A1)								AS DECIMAL(24,4))				AS A20									--ָ��20�����״�����ֵ					���״�����ֵ=�ƣ�ÿ�콻�״�����/�������׵�����
	,CAST(MAX(A1)								AS DECIMAL(24,4))				AS A21									--ָ��21������״���					����״���=max��ÿһ��Ľ��״�����
	,CAST(SUM(A2*1.000)/TCNT			AS DECIMAL(24,4))				AS A22									--ָ��22��һ��ͨ/��������			�վ����״���=��mi/ni
	,CAST(AVG(A2*1.000)						AS DECIMAL(24,4))				AS A23									--ָ��23�����״�����ֵ					���״�����ֵ=�ƣ�ÿ�콻�״�����/�������׵�����
	,CAST(MAX(A2*1.000)						AS DECIMAL(24,4))				AS A24									--ָ��24������״���					����״���=max��ÿһ��Ľ��״�����
from (			
      select B.OAP_ACCT_NBR
			,a.TRAD_DATE
			,CAST(sum(a.ocr_amt)/10000 		AS DECIMAL(24,4))				AS A1
			,CAST(count(distinct BNK_SEQ_NBR) AS DECIMAL(24,4))		AS A2			
        from nsoview.IPF_BANKRL_ACCS_DTL A
			   ,VT_OAP_BANKRL B
        where A.SC_CDE = B.SC_CDE			
        and A.BANKRL_ACCT_ACCT_NBR = B.BANKRL_ACCT_ACCT_NBR			
        and A.trad_date between CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD') AND  CAST('${E_DT}' AS DATE FORMAT 'YYYYMMDD')
        and a.crnc = 'CNY'
        group by 1,2 ) A
     	,(
		SELECT COUNT(*)  TCNT
		FROM NSPVIEW.PTY_TRAD_CLND
		WHERE IF_TRADDAY =1
        	AND CALENDAR_DATE between CAST('${S_DT}' AS DATE FORMAT 'YYYYMMDD') AND  CAST('${E_DT}' AS DATE FORMAT 'YYYYMMDD')
     ) B
	GROUP BY 1)
WITH DATA PRIMARY INDEX(OAP_ACCT_NBR)
ON COMMIT PRESERVE ROWS;		

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------STEP6	���ʴ�֣�����һ��ͨ�ͻ������ʸ��ʵ�

----------------STEP6.1	������24��ָ���Ȩ�غ���ֵ�����ݱ�ת�ɺ���ܼ�48���ֶΣ�T��β�ֶ�Ϊ��ֵ�ֶΣ�W��β�ֶ�ΪȨ���ֶ�

CREATE VOLATILE MULTISET TABLE VT_PZ_KPI_YZ AS(
select SUM(case  when IDX_CDE = 'KPI_01' then IDX_THRST_HLD else 0 end)	AS KPI_01_T
	,SUM(case  when IDX_CDE = 'KPI_01' then IDX_WGT else 0 end)         AS KPI_01_W  			
	,SUM(case  when IDX_CDE = 'KPI_02' then IDX_THRST_HLD else 0 end)		AS KPI_02_T
	,SUM(case  when IDX_CDE = 'KPI_02' then IDX_WGT else 0 end)					AS KPI_02_W
	,SUM(case  when IDX_CDE = 'KPI_03' then IDX_THRST_HLD else 0 end)		AS KPI_03_T
	,SUM(case  when IDX_CDE = 'KPI_03' then IDX_WGT else 0 end)					AS KPI_03_W
	,SUM(case  when IDX_CDE = 'KPI_04' then IDX_THRST_HLD else 0 end)		AS KPI_04_T
	,SUM(case  when IDX_CDE = 'KPI_04' then IDX_WGT else 0 end)					AS KPI_04_W
	,SUM(case  when IDX_CDE = 'KPI_05' then IDX_THRST_HLD else 0 end)		AS KPI_05_T 
	,SUM(case  when IDX_CDE = 'KPI_05' then IDX_WGT else 0 end)	        AS KPI_05_W 
	,SUM(case  when IDX_CDE = 'KPI_06' then IDX_THRST_HLD else 0 end)   AS KPI_06_T 
	,SUM(case  when IDX_CDE = 'KPI_06' then IDX_WGT else 0 end)	        AS KPI_06_W 
	,SUM(case  when IDX_CDE = 'KPI_07' then IDX_THRST_HLD else 0 end)   AS KPI_07_T 
	,SUM(case  when IDX_CDE = 'KPI_07' then IDX_WGT else 0 end)		      AS KPI_07_W 
	,SUM(case  when IDX_CDE = 'KPI_08' then IDX_THRST_HLD else 0 end)   AS KPI_08_T   
	,SUM(case  when IDX_CDE = 'KPI_08' then IDX_WGT else 0 end)	        AS KPI_08_W   
	,SUM(case  when IDX_CDE = 'KPI_09' then IDX_THRST_HLD else 0 end)   AS KPI_09_T   
	,SUM(case  when IDX_CDE = 'KPI_09' then IDX_WGT else 0 end)	        AS KPI_09_W   
	,SUM(case  when IDX_CDE = 'KPI_10' then IDX_THRST_HLD else 0 end)   AS KPI_10_T   
	,SUM(case  when IDX_CDE = 'KPI_10' then IDX_WGT else 0 end)         AS KPI_10_W   
	,SUM(case  when IDX_CDE = 'KPI_11' then IDX_THRST_HLD else 0 end)   AS KPI_11_T   
	,SUM(case  when IDX_CDE = 'KPI_11' then IDX_WGT else 0 end)         AS KPI_11_W   
	,SUM(case  when IDX_CDE = 'KPI_12' then IDX_THRST_HLD else 0 end)   AS KPI_12_T   
	,SUM(case  when IDX_CDE = 'KPI_12' then IDX_WGT else 0 end)         AS KPI_12_W   
	,SUM(case  when IDX_CDE = 'KPI_13' then IDX_THRST_HLD else 0 end)   AS KPI_13_T   
	,SUM(case  when IDX_CDE = 'KPI_13' then IDX_WGT else 0 end)         AS KPI_13_W   
	,SUM(case  when IDX_CDE = 'KPI_14' then IDX_THRST_HLD else 0 end)   AS KPI_14_T   
	,SUM(case  when IDX_CDE = 'KPI_14' then IDX_WGT else 0 end)         AS KPI_14_W   
	,SUM(case  when IDX_CDE = 'KPI_15' then IDX_THRST_HLD else 0 end)   AS KPI_15_T   
	,SUM(case  when IDX_CDE = 'KPI_15' then IDX_WGT else 0 end)         AS KPI_15_W   
	,SUM(case  when IDX_CDE = 'KPI_16' then IDX_THRST_HLD else 0 end)   AS KPI_16_T   
	,SUM(case  when IDX_CDE = 'KPI_16' then IDX_WGT else 0 end)         AS KPI_16_W   
	,SUM(case  when IDX_CDE = 'KPI_17' then IDX_THRST_HLD else 0 end)   AS KPI_17_T   
	,SUM(case  when IDX_CDE = 'KPI_17' then IDX_WGT else 0 end)         AS KPI_17_W   
	,SUM(case  when IDX_CDE = 'KPI_18' then IDX_THRST_HLD else 0 end)   AS KPI_18_T   
	,SUM(case  when IDX_CDE = 'KPI_18' then IDX_WGT else 0 end)         AS KPI_18_W   
	,SUM(case  when IDX_CDE = 'KPI_19' then IDX_THRST_HLD else 0 end)   AS KPI_19_T   
	,SUM(case  when IDX_CDE = 'KPI_19' then IDX_WGT else 0 end)         AS KPI_19_W   
	,SUM(case  when IDX_CDE = 'KPI_20' then IDX_THRST_HLD else 0 end)		AS KPI_20_T   
	,SUM(case  when IDX_CDE = 'KPI_20' then IDX_WGT else 0 end)         AS KPI_20_W   
	,SUM(case  when IDX_CDE = 'KPI_21' then IDX_THRST_HLD else 0 end)   AS KPI_21_T   
	,SUM(case  when IDX_CDE = 'KPI_21' then IDX_WGT else 0 end)         AS KPI_21_W   
	,SUM(case  when IDX_CDE = 'KPI_22' then IDX_THRST_HLD else 0 end)   AS KPI_22_T   
	,SUM(case  when IDX_CDE = 'KPI_22' then IDX_WGT else 0 end)         AS KPI_22_W   
	,SUM(case  when IDX_CDE = 'KPI_23' then IDX_THRST_HLD else 0 end)   AS KPI_23_T   
	,SUM(case  when IDX_CDE = 'KPI_23' then IDX_WGT else 0 end)         AS KPI_23_W   
	,SUM(case  when IDX_CDE = 'KPI_24' then IDX_THRST_HLD else 0 end)   AS KPI_24_T   
	,SUM(case  when IDX_CDE = 'KPI_24' then IDX_WGT else 0 end)         AS KPI_24_W   
	,SUM(case  when IDX_CDE = 'PZ_RT_TH' then IDX_THRST_HLD else 0 end)         AS PZ_RT_TH   
	,SUM(IDX_WGT) IDG_WGT_AMT
from $PZ_DATA.PZ_KPI_YZ
where e_date = CAST('${MAX_DT}' AS DATE FORMAT 'YYYYMMDD'))
WITH DATA NO PRIMARY INDEX
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .QUIT 12;

----------------STEP6.2	������24��ָ�����ϵ�һ����ʱ��

CREATE VOLATILE MULTISET TABLE VT_PZ_OAP_KPI AS(
SELECT A.OAP_ACCT_NBR
	,A01
	,A02
	,A03
	,A04
	,A05
	,A06
	,A07
	,A08
	,A09
	,A10
	,A11
	,A12
	,A13
	,A14
	,A15
	,A16
	,A17
	,A18
	,A19
	,A20
	,A21
	,A22
	,A23
	,A24
FROM VT_PZ_OAP_KPI_1 A
INNER JOIN VT_PZ_OAP_KPI_2 B
ON A.OAP_ACCT_NBR = B.OAP_ACCT_NBR
INNER JOIN VT_PZ_OAP_KPI_3 C
ON A.OAP_ACCT_NBR = C.OAP_ACCT_NBR
LEFT JOIN VT_PZ_OAP_KPI_4 D						
ON A.OAP_ACCT_NBR = D.OAP_ACCT_NBR)
WITH DATA PRIMARY INDEX(OAP_ACCT_NBR)
ON COMMIT PRESERVE ROWS;		

.IF ERRORCODE <> 0 THEN .QUIT 12;
            
----------------STEP6.3	��һ��ͨ�ͻ���24��ָ�������ʼ������ֵ�Աȣ���ȡһ��ͨ�ͻ���24��ָ��ֵ�ϵ�Ȩ�ص÷�

INSERT INTO $PZ_DATA.PZ_OAP_KPI_SCR
(
	OAP_ACCT_NBR
	,SEC_ACCT_NAME
	,LD_NBR
	,KPI_01
	,KPI_02
	,KPI_03
	,KPI_04
	,KPI_05
	,KPI_06
	,KPI_07
	,KPI_08
	,KPI_09
	,KPI_10
	,KPI_11
	,KPI_12
	,KPI_13
	,KPI_14
	,KPI_15
	,KPI_16
	,KPI_17
	,KPI_18
	,KPI_19
	,KPI_20
	,KPI_21
	,KPI_22
	,KPI_23
	,KPI_24
	,KPI_01_SCR
	,KPI_02_SCR
	,KPI_03_SCR
	,KPI_04_SCR
	,KPI_05_SCR
	,KPI_06_SCR
	,KPI_07_SCR
	,KPI_08_SCR
	,KPI_09_SCR
	,KPI_10_SCR
	,KPI_11_SCR
	,KPI_12_SCR
	,KPI_13_SCR
	,KPI_14_SCR
	,KPI_15_SCR
	,KPI_16_SCR
	,KPI_17_SCR
	,KPI_18_SCR
	,KPI_19_SCR
	,KPI_20_SCR
	,KPI_21_SCR
	,KPI_22_SCR
	,KPI_23_SCR
	,KPI_24_SCR
	,PZ_SCR_AMT
	,PZ_WGT_AMT
	,PZ_RT
	,PZ_RT_TH
)
select A.OAP_ACCT_NBR
	,C.SEC_ACCT_NAME
	,'${LD_NBR}'
	,A01
	,A02
	,A03
	,A04
	,A05
	,A06
	,A07
	,A08
	,A09
	,A10
	,A11
	,A12
	,A13
	,A14
	,A15
	,A16
	,A17
	,A18
	,A19
	,A20
	,A21
	,A22
	,A23                                                      
	,A24	                                                   
	--update date 20171120	                                                   
	,CASE WHEN A01 >= B.KPI_01_T THEN B.KPI_01_W ELSE 0 END AS S01
	,CASE WHEN A02 >= B.KPI_02_T THEN B.KPI_02_W ELSE 0 END AS S02
	,CASE WHEN A03 >= B.KPI_03_T THEN B.KPI_03_W ELSE 0 END AS S03
	,CASE WHEN A04 >= B.KPI_04_T THEN B.KPI_04_W ELSE 0 END AS S04
	,CASE WHEN ABS(A05) <= ABS(B.KPI_05_T) THEN B.KPI_05_W ELSE 0 END AS S05
	,CASE WHEN ABS(A06) <= ABS(B.KPI_06_T) THEN B.KPI_06_W ELSE 0 END AS S06
	,CASE WHEN A07 >= B.KPI_07_T THEN B.KPI_07_W ELSE 0 END AS S07
	,CASE WHEN A08 >= B.KPI_08_T THEN B.KPI_08_W ELSE 0 END AS S08
	,CASE WHEN A09 >= B.KPI_09_T THEN B.KPI_09_W ELSE 0 END AS S09
	,CASE WHEN A10 >= B.KPI_10_T THEN B.KPI_10_W ELSE 0 END AS S10
	,CASE WHEN ABS(A11) <= ABS(B.KPI_11_T) THEN B.KPI_11_W ELSE 0 END AS S11
	,CASE WHEN ABS(A12) <= ABS(B.KPI_12_T) THEN B.KPI_12_W ELSE 0 END AS S12
	,CASE WHEN A13 >= B.KPI_13_T THEN B.KPI_13_W ELSE 0 END AS S13
	,CASE WHEN A14 >= B.KPI_14_T THEN B.KPI_14_W ELSE 0 END AS S14
	,CASE WHEN A15 >= B.KPI_15_T THEN B.KPI_15_W ELSE 0 END AS S15
	,CASE WHEN A16 >= B.KPI_16_T THEN B.KPI_16_W ELSE 0 END AS S16
	,CASE WHEN A17 >= B.KPI_17_T THEN B.KPI_17_W ELSE 0 END AS S17
	,CASE WHEN A18 >= B.KPI_18_T THEN B.KPI_18_W ELSE 0 END AS S18
	,CASE WHEN A19 >= B.KPI_19_T THEN B.KPI_19_W ELSE 0 END AS S19
	,CASE WHEN A20 >= B.KPI_20_T THEN B.KPI_20_W ELSE 0 END AS S20
	,CASE WHEN A21 >= B.KPI_21_T THEN B.KPI_21_W ELSE 0 END AS S21
	,CASE WHEN A22 >= B.KPI_22_T THEN B.KPI_22_W ELSE 0 END AS S22
	,CASE WHEN A23 >= B.KPI_23_T THEN B.KPI_23_W ELSE 0 END AS S23
	,CASE WHEN A24 >= B.KPI_24_T THEN B.KPI_24_W ELSE 0 END AS S24
	,S01+S02+S03+S04+S05+S06+S07+S08+S09+S10+S11+S12
	+S13+S14+S15+S16+S17+S18+S19+S20+S21+S22+S23+S24 AS PZ_SCR_AMT
	,B.IDG_WGT_AMT
	,PZ_SCR_AMT*1.000/IDG_WGT_AMT AS PZ_RT
	,B.PZ_RT_TH
from VT_PZ_OAP_KPI A
	, VT_PZ_KPI_YZ B,
	(	
		SELECT OAP_ACCT_NBR,SEC_ACCT_NAME 
		FROM VT_OAP_SEC_ACCT
		GROUP BY 1,2
		QUALIFY RANK() OVER(PARTITION BY OAP_ACCT_NBR ORDER BY SEC_ACCT_NAME DESC) = 1
	) C
	WHERE A.OAP_ACCT_NBR = C.OAP_ACCT_NBR;

.IF ERRORCODE <> 0 THEN .QUIT 12;

.LOGOFF;

ENDOFINPUT

close(BTEQ);

my $RET_CODE = $? >> 8;
if ( $RET_CODE == 0 ) {
    return 0;
} else {
    return 1;
} 

}

#ȡϵͳʱ��
sub getTime
{

	my ($ret) = @_;            #��ȡʱ���ʽ

	my $tc = localtime(time());   #��ȡ��ǰʱ��$tc�洢�����ڴ��ַ
	$tc = sprintf("%4d%02d%02d%02d%02d%02d",$tc->year+1900,$tc->mon+1,
		  $tc->mday, $tc->hour, $tc->min,$tc->sec);                    #��ʱ��ƴΪ�ַ���

	#���и�ʽ��
	my $tmp = substr($tc,0,4);
	$ret =~ s/YYYY/$tmp/gi;

	$tmp = substr($tc,4,2);
	$ret =~ s/MM/$tmp/gi;

	$tmp = substr($tc,6,2);
	$ret =~ s/DD/$tmp/gi;

	$tmp = substr($tc,8,2);
	$ret =~ s/HH/$tmp/gi;

	$tmp = substr($tc,10,2);
	$ret =~ s/MI/$tmp/gi;

	$tmp = substr($tc,12,2);
	$ret =~ s/SS/$tmp/gi;

	return $ret;
}

sub main{

#������־�ļ�
my $cdatetime = getTime("yyyymmddhhmiss");
$LOG_FILE = "pz_".$LD_NBR.".log";
print "***��鿴��־�ļ�:		".$LOG_FILE."\n";

#�ж���־�ļ��Ƿ���Դ�
my $rs = open(STDOUT,">>$LOG_FILE");
$START_TIME = getTime("yyyy-mm-dd hh:mi:ss");
print "Begin to write log file:$START_TIME\n";

if ( $rs != 1 ) {
    print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
    print "Open STDOUT failed:$!\n";
} else {
    ##print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
    ##print "Open STDOUT success\n";
}

my $rs = open(STDERR,">>$LOG_FILE");
if ( $rs != 1 ) {
    print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
    print "Open STDERR failed:$!\n";
} else {
    ##print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
    ##print "Open STDERR success\n";
}

print "==================================================================\n";
print "***һ��ͨ�б��ļ�:                          ".$OAP_FILE."\n";
print "***���ʼ�������                               ".$LD_NBR."\n";
print "***���ʼ�����ʼ����:                      ".$S_DT."\n";
print "***���ʼ�����ֹ����                       ".$E_DT."\n\n";
print "***���ʼ��㿪ʼʱ��                       ".$START_TIME."\n";
print "==================================================================\n\n";

print "***��һ��ͨ�б��ļ�".$OAP_FILE."������ʱ��".$PZ_TEMP.".".$Fastld_temptbl."......\n\n";

$ret = run_fastload_command();

if ($ret eq "0") {
    print "==================================================================\n";
    print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
    ##print "Run bteq command success\n\n";
    print "***һ��ͨ�б��ļ�".$OAP_FILE."�ɹ�������ʱ��".$PZ_TEMP.".".$Fastld_temptbl."!\n\n";
	print "==================================================================\n";    
} else {
    print "==================================================================\n";
    print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
    print "***һ��ͨ�б��ļ�".$OAP_FILE."������ʱ��".$PZ_TEMP.".".$Fastld_temptbl."ʧ��!\n";
    print "==================================================================\n";
    close(STDOUT);
    close(STDERR);    
    return $ret;
}

print "***��ʼ����һ��ͨ�б��ļ�".$OAP_FILE."������ָ��......\n";

$ret = pz_calc();

if ($ret eq "0") {
	print "==================================================================\n";	
    	print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
    	##print "Run bteq command success\n\n";
    	print "***���ʼ���ɹ����!\n\n";
	print "==================================================================\n";    
} else {
	print "==================================================================\n";
    	print "[". getTime("yyyy-mm-dd hh:mi:ss")."]";
    	print "���ʼ���ʧ��!\n\n";
	print "==================================================================\n";    	
}

$END_TIME = getTime("yyyy-mm-dd hh:mi:ss");

print "==================================================================\n\n";
print "���ʼ������κ�      ".$LD_NBR."���ʼ��㿪ʼʱ��".$START_TIME."\n";
print "���ʼ������κ�      ".$LD_NBR."	���ʼ������ʱ��".$END_TIME."\n";
print "==================================================================\n\n";

close(STDOUT);
close(STDERR);

return $ret;

}

my $ret = main();

exit($ret);
