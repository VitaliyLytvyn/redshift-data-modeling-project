# Udacity Data Engineering Nanodegree program
## Project: Data Warehouse

### Intro
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

To achieve the goal we build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

### Files explanation.
- 'dwh.cfg'- the configuration file. Added to .gitignore for security reasons.
Thus to launch the project the file to be created and filled up with Cluster and Iam role credentials.

- 'sql_queries.py' - defines SQL statements, which will be imported into 'create_table.py' and 'etl.py'

- 'create_table.py' - creates a fact and dimension tables for the star schema in Redshift.

- 'etl.py' - loads data from S3 into staging tables on Redshift and then processes that data into analytics tables on Redshift.

### ETL pipeline
To prepare database run file 'create_table.py'. 
It will connect to existing Redshift cluster, drop existing tables and create the new ones.
SQL queries used in 'create_table.py' are defined in 'sql_queries.py' file.

To start etl pipeline run 'etl.py' file.
It will connect to existing Redshift cluster and start 2 processing works:
- Extracts data relative to songs and users activities from S3 and saves obtained data to staging tables on Redshift.
- Reads data from staging tables and saves obtained data to analytics tables.

### UML diagram
![UML diagram](/images/uml.png)