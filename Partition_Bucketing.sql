--Step-1: Create a Hive table
CREATE TABLE PATIENT(PATIENT_ID INT,PATIENT_NAME STRING,DRUG STRING,GENDER STRING,TOTAL_AMOUNT INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

--Step-2: Load value into the Hive table
LOAD DATA LOCAL INPATH '/home/cloudera/Sumit_Works/Hadoop/Hive/DataSets/patient.csv' INTO TABLE PATIENT;

--Partition Using existing column
--Step-1: Create a Hive partition table
CREATE TABLE P_PATIENT1(PATIENT_ID INT,PATIENT_NAME STRING,GENDER STRING,TOTAL_AMOUNT INT) PARTITIONED BY (DRUG STRING);

--Step-2: Insert value into the Partitioned table
INSERT OVERWRITE TABLE P_PATIENT1 PARTITION(DRUG='metacin') SELECT PATIENT_ID, PATIENT_NAME, GENDER, TOTAL_AMOUNT FROM PATIENT WHERE DRUG='metacin';

--Using new column:
--Step-1: Create a Partitioned Hive table
CREATE TABLE P_PATIENT(PATIENT_ID INT, PATIENT_NAME STRING, DRUG STRING, GENDER STRING, TOTAL_AMOUNT INT) PARTITIONED BY (NEW STRING);

--Step-2: Insert the value into the table
INSERT OVERWRITE TABLE P_PATIENT PARTITION(NEW='metacin') SELECT * FROM PATIENT WHERE DRUG='metacin';


--Dynamic Partition:

1.columns whose values are only known at EXECUTION TIME.

2.We use dynamic partition while loading from an existing table that is not partitioned.

3.We use dynamic partition while unknown values for partition columns.

4.Usually dynamic partition load the data from non partitioned table.

5.Dynamic Partition takes more time in loading data compared to static partition.

6.There is no required where clause to use limit.

7.We can’t perform alter on Dynamic partition.

8.Perform dynamic partition on hive external table and managed table.

In dynamic partition the partitioned column of the partitioned hive table is must present in the last column of the existing hive table.

--Step-1: Create a hive table
CREATE TABLE PATIENT1(PATIENT_ID INT, PATIENT_NAME STRING, GENDER STRING, TOTAL_AMOUNT INT, DRUG STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

--Step-2: Load data into the hive table
LOAD DATA LOCAL INPATH '/home/cloudera/Sumit_Works/Hadoop/Hive/DataSets/patient1.csv' INTO TABLE PATIENT1;

--Step-3: Before creating Partitioned table in hive first we set the properties for dynamic partition
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;

--Step-4: Create a partitioned table in hive
CREATE TABLE DYNAMIC_PARTITION_PATIENT (PATIENT_ID INT,PATIENT_NAME STRING, GENDER STRING, TOTAL_AMOUNT INT) PARTITIONED BY (DRUG STRING);

--Step-5: Insert value into the partitioned table
INSERT INTO TABLE DYNAMIC_PARTITION_PATIENT PARTITION(DRUG) SELECT * FROM PATIENT1;


--Step-6: View the value of the Partitioned table using hadoop command
hadoop fs -ls /user/hive/warehouse/health_db.db/dynamic_partition_patient

--Bucketing
--Step-1: Create a Hive table
CREATE TABLE PATIENT2(PATIENT_ID INT, PATIENT_NAME STRING, DRUG STRING, GENDER STRING, TOTAL_AMOUNT INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

--Step-2: Load value into the Hive table
LOAD DATA LOCAL INPATH '/home/cloudera/Sumit_Works/Hadoop/Hive/DataSets/patient.csv' INTO TABLE PATIENT2;

--Step-3: First set the property before create bucketing table in hive
set hive.enforce.bucketing =true;

--Step-4: Create a bucketing table in Hive
CREATE TABLE BUCKET_PATIENT(PATIENT_ID INT, PATIENT_NAME STRING, DRUG STRING,GENDER STRING, TOTAL_AMOUNT INT) CLUSTERED BY (DRUG) INTO 4 BUCKETS;

--Step-5: Insert the value into the bucketing table
INSERT OVERWRITE TABLE BUCKET_PATIENT SELECT * FROM PATIENT2;

--Step-6: View the value of first bucket in the bucketing table
SELECT * FROM BUCKET_PATIENT TABLESAMPLE(BUCKET 1 OUT OF 4 ON DRUG);

--Step-7: View the 10% of value in the bucketing table
SELECT * FROM BUCKET_PATIENT TABLESAMPLE(10 PERCENT);

--Step-8: View the value in limit 5
SELECT * FROM BUCKET_PATIENT LIMIT 5;







