# sqoop_and_hive_practise

## Running the scripts

### 1. mysql_to_hive.sh

This file sqoops data from mysql to hive and creates two tables: user and activitylog
The usage is as below
```sh
sh mysql_to_hive.sh [--setup or --exec or --del]
```
<ul>
	<li>--setup: creates sqoop jobs and sets up databse in hive if does not exist</li>
	<li>--exec: execute sqoop commands/jobs to copy data from mysql to hive</li>
	<li>--del: remove sqoop jobs</li>
</ul>

### 2. csv_to_hive.sh

This script is used to load the csv files into hdfs. It also creates an external hive table refrencing the 
hdfs file location called user_upload_dump.
The usage is as below:
```sh
sh csv_to_hive.sh
```

### 3. reports.hql

This is a hive query file which generates reports from the above tables. The two tables generated are user_total and user_report.
To run this query file:
```sh
hive -f reports.hql
```