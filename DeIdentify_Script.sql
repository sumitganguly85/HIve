ADD JAR /home/cloudera/Sumit_Works/Hadoop/Hive/Hive_UDFS/JAVA/UDFJars/MyUDF.jar;

CREATE DATABASE healthDB;
USE healthDB;

CREATE TABLE healthCareSampleDS(PatientID INT,Name STRING,DOB STRING,PhoneNumber String,EmailAddress STRING,SSN STRING,Gender STRING,Disease STRING,weight FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/cloudera/Sumit_Works/Hadoop/Hive/DataSets/healthcare_Sample_dataset1.csv' INTO TABLE healthCareSampleDS;

CREATE TEMPORARY FUNCTION deIdentify AS 'com.cloudera.myudfs.Deidentify';

CREATE TABLE healthCareSampleDSDeidentified AS SELECT PatientID,deIdentify(Name),deIdentify(DOB),deIdentify(PhoneNumber),deIdentify(EmailAddress),deIdentify(SSN),deIdentify(Gender)
,deIdentify(Disease),deIdentify(weight) FROM healthCareSampleDS;