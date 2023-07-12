# Databricks notebook source


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_NPPES.columns

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Grouping by MONTH.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW V_COL_mortalityCaseNumberCurrent_GROUPED_BY_MONTHS_TEMP AS
# MAGIC SELECT PROVIDER_NPI,
# MAGIC        CAST(COUNT(DISTINCT FORIAN_PATIENT_ID) AS INT) AS mortalityCaseNumberCurrent,
# MAGIC        YearOfService,
# MAGIC        MonthOfService
# MAGIC FROM (SELECT V_FC_PROVIDER_PAYMENT.PROVIDER_NPI,
# MAGIC              V_FC_HEADER.FORIAN_PATIENT_ID,
# MAGIC              YEAR(V_FC_HEADER.DATE_OF_SERVICE) AS YearOfService,
# MAGIC              MONTH(V_FC_HEADER.DATE_OF_SERVICE) AS MonthOfService
# MAGIC       FROM V_FC_HEADER
# MAGIC       INNER JOIN V_FC_PROVIDER_PAYMENT ON V_FC_HEADER.CLAIM_NUMBER = V_FC_PROVIDER_PAYMENT.CLAIM_NUMBER
# MAGIC       WHERE PROVIDER_NPI IS NOT NULL AND PROVIDER_NPI != 0 AND PROVIDER_NPI != 1 AND (L_DISCHARGE_STATUS = "20" OR L_DISCHARGE_STATUS = "40" OR L_DISCHARGE_STATUS = "41" OR L_DISCHARGE_STATUS = "42") AND V_FC_HEADER.FORIAN_PATIENT_ID IS NOT NULL)
# MAGIC GROUP BY PROVIDER_NPI, YearOfService, MonthOfService;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW V_DATES_FOR_GROUPING_BY_MONTH AS
# MAGIC SELECT YearOfService,
# MAGIC        MonthOfService,
# MAGIC        CAST(CONCAT(YearOfService, "-", MonthOfService, "-", startDay) AS DATE) AS periodStartDate,
# MAGIC        CAST(CONCAT(YearOfService, "-", MonthOfService, "-", endDay) AS DATE) AS periodEndDate
# MAGIC FROM (SELECT YEAR(DATE_OF_SERVICE) AS YearOfService,
# MAGIC              MONTH(DATE_OF_SERVICE) AS MonthOfService,
# MAGIC              MIN(DAY(DATE_OF_SERVICE)) AS startDay,
# MAGIC              MAX(DAY(DATE_OF_SERVICE)) AS endDay
# MAGIC       FROM V_FC_HEADER
# MAGIC       GROUP BY YearOfService, MonthOfService);
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW V_COL_mortalityCaseNumberCurrent_GROUPED_BY_MONTHS AS
# MAGIC SELECT V_COL_mortalityCaseNumberCurrent_GROUPED_BY_MONTHS_TEMP.PROVIDER_NPI,
# MAGIC        V_DATES_FOR_GROUPING_BY_MONTH.periodStartDate,
# MAGIC        V_DATES_FOR_GROUPING_BY_MONTH.periodEndDate,
# MAGIC        V_COL_mortalityCaseNumberCurrent_GROUPED_BY_MONTHS_TEMP.mortalityCaseNumberCurrent    
# MAGIC FROM V_COL_mortalityCaseNumberCurrent_GROUPED_BY_MONTHS_TEMP
# MAGIC LEFT JOIN V_DATES_FOR_GROUPING_BY_MONTH ON V_COL_mortalityCaseNumberCurrent_GROUPED_BY_MONTHS_TEMP.YearOfService = V_DATES_FOR_GROUPING_BY_MONTH.YearOfService AND V_COL_mortalityCaseNumberCurrent_GROUPED_BY_MONTHS_TEMP.MonthOfService = V_DATES_FOR_GROUPING_BY_MONTH.MonthOfService;
# MAGIC
# MAGIC
# MAGIC -- Grouping by Sliding Year.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW V_COL_mortalityCaseNumberCurrent_SLIDING_YEAR AS
# MAGIC SELECT PROVIDER_NPI,
# MAGIC        add_months(CAST(CONCAT(YEAR(current_date()), "-", MONTH(current_date())-1, "-", 15) AS DATE), -12) AS periodStartDate,
# MAGIC        CAST(CONCAT(YEAR(current_date()), "-", MONTH(current_date())-1, "-", 15) AS DATE) AS periodEndDate,
# MAGIC        CAST(COUNT(DISTINCT FORIAN_PATIENT_ID) AS INT) AS mortalityCaseNumberCurrent
# MAGIC FROM (SELECT V_FC_PROVIDER_PAYMENT.PROVIDER_NPI,
# MAGIC              V_FC_HEADER.FORIAN_PATIENT_ID
# MAGIC       FROM V_FC_HEADER
# MAGIC       INNER JOIN V_FC_PROVIDER_PAYMENT ON V_FC_HEADER.CLAIM_NUMBER = V_FC_PROVIDER_PAYMENT.CLAIM_NUMBER
# MAGIC       WHERE PROVIDER_NPI IS NOT NULL AND PROVIDER_NPI != 0 AND PROVIDER_NPI != 1 AND (L_DISCHARGE_STATUS = "20" OR L_DISCHARGE_STATUS = "40" OR L_DISCHARGE_STATUS = "41" OR L_DISCHARGE_STATUS = "42") AND DATE_OF_SERVICE > add_months(CAST(CONCAT(YEAR(current_date()), "-", MONTH(current_date())-1, "-", 15) AS DATE), -12) AND V_FC_HEADER.FORIAN_PATIENT_ID IS NOT NULL)
# MAGIC GROUP BY PROVIDER_NPI;
# MAGIC
# MAGIC
# MAGIC -- Grouping by Whole Period.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW V_COL_mortalityCaseNumberCurrent_WHOLE_PERIOD_TEMP AS
# MAGIC SELECT PROVIDER_NPI,
# MAGIC        CAST(COUNT(DISTINCT FORIAN_PATIENT_ID) AS INT) AS mortalityCaseNumberCurrent,
# MAGIC        "0" AS columnForJoin
# MAGIC FROM (SELECT V_FC_PROVIDER_PAYMENT.PROVIDER_NPI,
# MAGIC              V_FC_HEADER.FORIAN_PATIENT_ID
# MAGIC       FROM V_FC_HEADER
# MAGIC       INNER JOIN V_FC_PROVIDER_PAYMENT ON V_FC_HEADER.CLAIM_NUMBER = V_FC_PROVIDER_PAYMENT.CLAIM_NUMBER
# MAGIC       WHERE PROVIDER_NPI IS NOT NULL AND PROVIDER_NPI != 0 AND PROVIDER_NPI != 1 AND (L_DISCHARGE_STATUS = "20" OR L_DISCHARGE_STATUS = "40" OR L_DISCHARGE_STATUS = "41" OR L_DISCHARGE_STATUS = "42") AND V_FC_HEADER.FORIAN_PATIENT_ID IS NOT NULL)
# MAGIC GROUP BY PROVIDER_NPI;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW V_DATES_FOR_GROUPING_BY_WHOLE AS
# MAGIC SELECT "0" AS columnForJoin,
# MAGIC        MIN(DATE_OF_SERVICE) AS periodStartDate,
# MAGIC        CAST(CONCAT(YEAR(current_date()), "-", MONTH(current_date())-1, "-", 15) AS DATE) AS periodEndDate
# MAGIC FROM V_FC_HEADER
# MAGIC GROUP BY '*';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW V_COL_mortalityCaseNumberCurrent_WHOLE_PERIOD AS
# MAGIC SELECT V_COL_mortalityCaseNumberCurrent_WHOLE_PERIOD_TEMP.PROVIDER_NPI,
# MAGIC        V_DATES_FOR_GROUPING_BY_WHOLE.periodStartDate,
# MAGIC        V_DATES_FOR_GROUPING_BY_WHOLE.periodEndDate,
# MAGIC        V_COL_mortalityCaseNumberCurrent_WHOLE_PERIOD_TEMP.mortalityCaseNumberCurrent
# MAGIC FROM V_COL_mortalityCaseNumberCurrent_WHOLE_PERIOD_TEMP
# MAGIC LEFT JOIN V_DATES_FOR_GROUPING_BY_WHOLE ON V_COL_mortalityCaseNumberCurrent_WHOLE_PERIOD_TEMP.columnForJoin = V_DATES_FOR_GROUPING_BY_WHOLE.columnForJoin;
# MAGIC
# MAGIC
# MAGIC -- Union of three tables.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW V_COL_mortalityCaseNumberCurrent AS
# MAGIC SELECT * FROM V_COL_mortalityCaseNumberCurrent_GROUPED_BY_MONTHS
# MAGIC UNION
# MAGIC SELECT * FROM V_COL_mortalityCaseNumberCurrent_SLIDING_YEAR
# MAGIC UNION
# MAGIC SELECT * FROM V_COL_mortalityCaseNumberCurrent_WHOLE_PERIOD;

# COMMAND ----------



# COMMAND ----------

df_NPPES = spark.read.format('delta').load("/mnt/processing/GovOpenData/CMS/NPPES/npidata_pfile/Full/")
df_NPPES.createOrReplaceTempView("V_NPPES")

# COMMAND ----------

df_ExpertSpecialty = spark.read.parquet("/mnt/produced/Specialty/ExpertSpecialty/")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM V_EXPERT_SPECIALTY
# MAGIC INNER JOIN V_SPECIALTY_SUBSPECIALTY ON V_EXPERT_SPECIALTY.ryteCompositeCode = V_SPECIALTY_SUBSPECIALTY.ryteCompositeCode
# MAGIC WHERE NPI != "-"

# COMMAND ----------

df_ExpertSpecialty.columns

# COMMAND ----------

df_ProviderPayment = spark.read.parquet("/mnt/usa/processing/Claims/Forian/open_medical_provider_with_claim_number_and_payment/")  
# Эта таблица, а не "/mnt/usa/processing/Claims/Forian/open_medical_hospital_remittance_claim_payment_H/"
df_ProviderPayment.createOrReplaceTempView("V_FC_PROVIDER_PAYMENT")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT PROVIDER_NPI) FROM V_FC_PROVIDER_PAYMENT

# COMMAND ----------

Institution = spark.read.format('parquet').option('header',True).option('multiLine', True).option('unescapedQuoteHandling', 'STOP_AT_CLOSING_QUOTE').option('escape', '"').load('/mnt/produced/NewDataModelProduced/I01/Institution')

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

Institution = Institution.filter(col("institutionType").isNotNull())

# COMMAND ----------

df_InstDep = spark.read.parquet("/mnt/produced/NewDataModelProduced/I02/InstitutionDepartment/")

# COMMAND ----------

df_InstDep.display()

# COMMAND ----------

df_InstDep = df_InstDep.withColumnRenamed("institutionId", "institutionId_j")

# COMMAND ----------

Institution.join(df_InstDep, col("institutionId")==col("institutionId_j"), "left").filter(  (col("addressFull").isNotNull()) & (col("addressFull")=="")  ).display()

# COMMAND ----------

Institution.join(df_InstDep, col("institutionId")==col("institutionId_j"), "left").count()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

aha = spark.read.parquet('/mnt/refined/ParsedWebsites/AHAData/Facility/FacilityMain/')

# COMMAND ----------

aha.count()

# COMMAND ----------

aha.display()

# COMMAND ----------

aha.filter(col("facilityLocationAddressStreet").isNull()).count()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

