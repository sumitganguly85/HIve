use hospital;
truncate table patient_target;
from patients pt
insert overwrite table patient_target
select pt.*
insert overwrite local directory '/home/cloudera/Sumit_Works/Hadoop/Hive/OP/'
select pt.*
insert overwrite directory '/user/cloudera/hiveop/'
select pt.*
;