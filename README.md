# NEW YORK STATE COUNTY COVID 19 TEST DATA BASE SYSTEM

The designed system enables data team to perform analysis on COVID 19 daily test data. Designed workflow runs everyday at 9:00 AM. and the obtained test data run through an ETL
pipeline and is stored in respective County tables in database.

## Data Source:

Incoming data is an API response from API given below.

https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD

API request is made everyday at 9:00 AM through AirFlow Scheduled jobs.

## TRANSFORM:

Resultant API response is stored in a dataframe with only selected columns as requested by analyst team. A snapshot of the dataframe is as follows:

![Dataframe](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/finalDF.png)

But all the columns in the dataframe are stored as strings. It can be seen from below.

![Datatype](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/datatypes.png)

Therefore all the columns need to changed to appropriate data types to load them into database tables. A snapshot of the changed data types.

![Changed datatypes](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/datatypechanged.png)

Here the database tables are stored in Postgres Database and the batch processing ETL jobs are automated using Apache AirFlow. and Docker is utilized to run the database locally.

Steps to run the application on your database locally:

Pre-requiste:

Make sure you have Docker and Docker compose on the local system.

The project file structure is as below:

![ProjectDirectory](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/project%20Directory.png)

## APPLICATION RUNNING STEPS

Now run this command inside the project directory where yml file present

sudo docker-compose -f docker-compose-LocalExecutor.yml up -d

This should create a postgres Container and a Airflow container in your docker environment. A snapshot of the created containers are as follows:

![containers](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/container%20images.png)

After the containers are created make sure you give the same container Id in the RestAPI call operator code for connecting to PostgreSQL database in Docker.

Then use the following command

docker exec -it 3a3e483a2944 bash

To enter the airflow, use following command

psql -h localhost -U airflow airflow

Now Go to the browser, localhost:8080 or localhost:8081

This should open AirFlow UI as follows, and the steps are given as images

![Step1](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/AirFlow%20UI.png)


![Step2](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/DAG%20UI.png)


![Step3](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/Triggering%20DAG.png)


![Status of the DAG](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/DAG%20status.png)


Ater the AirFlow DAG runs are succesful, the tables are stored in the Postgres on Docker. For each county, a table is created dynamically. A snapshot of the created tables are as follows:

![createdTables](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/tables%20created.png)

Here is a snapshot of the ALbany county table created

Here all the records are stored as county tables in the database


![Table preview](https://github.com/thotamohan/Machine-Learning-coursework/blob/master/Tables_preview.png)

Here an assumption is made that, every day we are receving new data from API call, therefore we are processing all the obtained records from API response, and storing them in County tables.








