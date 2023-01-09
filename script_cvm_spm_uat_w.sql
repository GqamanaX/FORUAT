WITH SPM AS(
    SELECT
        accountbalance
        ,accountnumber
        ,customerstatuscode
        ,opendate
        ,closedate
        ,marbleentityid
        ,absanaturalkeyid
        ,enceladus_info_date
        ,productcode
        ,accounttype
        FROM parquet.`/bigdatahdfs/datalake/publish/esta/tblHadoopClientData/enceladus_info_date=#value1#`
        /*WHERE enceladus_info_date >= '2022-07-02' */
),
SPM_REMOVE_DUPLICATES AS (
    SELECT
        DISTINCT *
    FROM 
        SPM
),
SPM_DATASET AS(
    SELECT 
        *
        ,CAST('SPM'        AS VARCHAR(10))     AS SOURCE_CODE
        ,UPPER(CAST('0000' AS VARCHAR(10)))    AS SITE_CODE
    FROM SPM_REMOVE_DUPLICATES
    /*WHERE enceladus_info_date = (SELECT MX_DATE FROM MAX_DATE ) */
),
RESULTS AS(
SELECT  CAST(CDA.accountbalance                            AS DECIMAL(38,18)       )      AS ACCOUNT_BALANCE
     ,  RTRIM(LTRIM(UPPER(LPAD(CAST(CDA.accountnumber      AS VARCHAR(25)), 25, '0'))))   AS ACCOUNT_NUMBER
     ,  RTRIM(LTRIM(UPPER(CAST(CDA.customerstatuscode      AS VARCHAR(100)         ))))   AS SOURCE_ACCOUNT_STATUS_CODE
     ,  RTRIM(LTRIM(UPPER(CAST(SPC.SUB_PRODUCT_CODE        AS VARCHAR(30)          ))))   AS SOURCE_ACCOUNT_TYPE_CODE
     ,  CAST(CASE
                WHEN (UPPER(TRIM(REGEXP_REPLACE(CDA.customerstatuscode,' ', ''))) = 'ACTIVE'
                 AND CDA.opendate = CDA.closedate)  THEN    CAST(NULL AS DATE)
                ELSE CDA.closedate
             END                                           AS DATE                 )     AS CLOSED_DATE
     ,  CAST(NULL                                          AS DATE                 )     AS MATURITY_DATE
     ,  CAST(NULL                                          AS VARCHAR(10))               AS CIF_CUSTOMER_KEY
     ,  RTRIM(LTRIM(UPPER(CAST(CDA.marbleentityid          AS VARCHAR(20)          ))))  AS SOURCE_CUSTOMER_KEY
     ,  UPPER(CAST(CDA.absanaturalkeyid                    AS VARCHAR(20)          ))    AS ID_NUMBER
     ,  CAST(NULL                                          AS VARCHAR(20)          )     AS SINGLE_UNIQUE_CUSTOMER_KEY
     ,  RTRIM(LTRIM(UPPER(CAST(CDA.marbleentityid          AS VARCHAR(20)          ))))  AS NON_CIF_CUSTOMER_KEY
     ,  CAST(CDA.enceladus_info_date                       AS DATE                 )     AS INFORMATION_DATE
     ,  CAST(CDA.opendate                                  AS DATE                 )     AS OPEN_DATE
     ,  CDA.SITE_CODE                                                                    AS SITE_CODE
     ,  CDA.SOURCE_CODE                                                                  AS SOURCE_CODE
     ,  RTRIM(LTRIM(UPPER(CAST(SS.SOURCE                   AS VARCHAR(100)         ))))  AS SOURCE
     ,  RTRIM(LTRIM(UPPER(CAST(CDA.productcode             AS VARCHAR(10)          ))))  AS PRODUCT_CODE
     ,  RTRIM(LTRIM(UPPER(CAST(SPC.SUB_PRODUCT_CODE        AS VARCHAR(70)          ))))  AS SUB_PRODUCT_CODE
     ,  RTRIM(LTRIM(UPPER(CAST(CDA.accounttype             AS VARCHAR(100)         ))))  AS SUB_PRODUCT
     ,  RTRIM(LTRIM(UPPER(CAST(SPC.SUB_PRODUCT_CODE        AS VARCHAR(30)          ))))  AS ACCOUNT_TYPE_CODE
     ,  RTRIM(LTRIM(UPPER(CAST(CDA.accounttype             AS VARCHAR(100)         ))))  AS ACCOUNT_TYPE
     ,  RTRIM(LTRIM(UPPER(CAST(SPSG.ACCOUNT_STATUS_CODE    AS VARCHAR(6)           ))))  AS ACCOUNT_STATUS_CODE
     ,  RTRIM(LTRIM(UPPER(CAST(ST.SITE                     AS VARCHAR(100)         ))))  AS SITE
     ,  RTRIM(LTRIM(UPPER(CAST(PD.PRODUCT                  AS VARCHAR(100)         ))))  AS PRODUCT
     ,  RTRIM(LTRIM(UPPER(CAST(SPSG.ACCOUNT_STATUS         AS VARCHAR(100)         ))))  AS ACCOUNT_STATUS
     ,  RTRIM(LTRIM(UPPER(CAST(SPSG.PRODUCT_STATUS         AS VARCHAR(70)          ))))  AS PRODUCT_STATUS
     ,  CAST(NULL                                          AS VARCHAR(35)          )     AS PRODUCT_SUB_STATUS
     ,  CAST(NULL                                          AS DECIMAL(38,18)       )     AS PRE_PAYMENT_AMOUNT
     ,  CAST(NULL                                          AS BIGINT               )     AS OVERDRAFT_INDICATOR
     ,  CAST(NULL                                          AS VARCHAR(10)          )     AS COLLECTIONS_STATUS_CODE
     ,  CAST(NULL                                          AS VARCHAR(150)         )     AS COLLECTIONS_STATUS
    FROM      SPM_DATASET AS CDA
    LEFT JOIN  (SELECT  _c1     AS ACCOUNT_TYPE
                 ,  _c3     AS SUB_PRODUCT_CODE
              FROM  csv.`hdfs:///bigdatahdfs/project/cvm/Staging/vLOOKUP/reference_data/Mapping_SPM_Prod_Sprod_GEN.csv`    ) AS SPC
    ON          UPPER(TRIM(REGEXP_REPLACE(CDA.accounttype,' ','')))   = UPPER(TRIM(REGEXP_REPLACE(SPC.ACCOUNT_TYPE,' ','')))
LEFT JOIN (SELECT  _c0     AS SOURCE_ACCOUNT_STATUS_CODE
                 , _c1     AS ACCOUNT_STATUS_CODE
                 , _c2     AS PRODUCT_STATUS
                 , _c3     AS ACCOUNT_STATUS
              FROM  csv.`hdfs:///bigdatahdfs/project/cvm/Staging/vLOOKUP/reference_data/LOOKUP_SPM_ACCOUNT_STATUS.csv`    )    AS SPSG
    ON         UPPER(TRIM(REGEXP_REPLACE(CDA.customerstatuscode,' ','')))  = UPPER(TRIM(REGEXP_REPLACE(SPSG.SOURCE_ACCOUNT_STATUS_CODE,' ', '')))
LEFT JOIN (
           SELECT 
		        _c0  AS SOURCE_CODE
			   ,_C1  AS SOURCE
		   FROM csv.`hdfs:///bigdatahdfs/project/cvm/Staging/vLOOKUP/reference_data/Lookup_Gen_Source.csv`
         ) SS
	ON CDA.SOURCE_CODE = SS.SOURCE_CODE
LEFT JOIN (
           SELECT 
		        _c0  AS SITE_CODE
			   ,_C1  AS SITE
		   FROM csv.`hdfs:///bigdatahdfs/project/cvm/Staging/vLOOKUP/reference_data/Lookup_Gen_Site.csv`
         ) ST
	ON CDA.SITE_CODE = ST.SITE_CODE
LEFT JOIN (
           SELECT 
		        _c0  AS PRODUCT_CODE
			   ,_C1  AS PRODUCT                  /*Product_code from source is EQT and lookup says EQY*/
		   FROM csv.`hdfs:///bigdatahdfs/project/cvm/Staging/vLOOKUP/reference_data/Lookup_Gen_Product.csv`
         ) PD
	ON CDA.productcode = PD.PRODUCT_CODE
)
SELECT DISTINCT * FROM RESULTS 
