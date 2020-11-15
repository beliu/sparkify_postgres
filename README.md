# The Sparkify ETL Pipeline

## Project Overview
Sparkify is an up-and-coming music streaming platform that is (pardon the pun) hitting all the right notes in the tech and music industry. We are building an ETL pipeline for them that will extract song, artist, and log data from JSON files and load the data into their postgresql database. Once this process is complete, Sparkify data analysts will be able to explore the data and gain insights about how the platform's users interact with the app, and which songs and artists they listen to.

<br>

## Project Files
The main files/folders of interest in this repository are:
- data/
- sql_queries.py
- create_tables.py
- etl.py
- etl.ipynb
- test.ipynb

**data/** - This directory contains the JSON files with the data for our database. There are two other directories within this one: 
`log_data/2018/11` contains the JSON files for user songplay log data.
`song_data/` contains JSON files for song and artist data.

**sql_queries.py** - This script contains SQL commands used to DROP, CREATE, and INSERT values into the data tables.

**create_tables.py** - Using the python library `psycopg2`, this script connects to the database and executes the commands from `sql_queries.py` to create and insert values into the data tables.

**etl.py** - This script does the extraction, transforming, and loading of the user-log and song/artist data into the database. I describe the details of the script in the ETL section below.

**etl.ipynb** - This is a Jupyter Notebook that I used to test my etl functions before adding them to `etl.py`.

**test.ipynb** - This notebook contains SQL commands you can execute within the notebook to test if the `etl.py` successfully loaded the data into the database.

<br>

## How to Run the Scripts
If you would like to run the scripts locally, first download and install postgresql to your machine:

[Install Postgresql](https://www.postgresql.org/download/)

You can create the database `studentdb` and the user `student` by operning the command line terminal on your machine and following these steps:

Get into Admin mode:

`$ sudo -u postgres psql`

You will have to enter your password for your machine.

If successful, you should now be in the psql command line. You can tell if you are in psql if you see this in your terminal:

`postgres=#`

Create the database:

`postgres=# create database studentdb;`

Create the user:

`postgres=# create user student with password 'student';`

Give the user privileges:

`postgres=# grant all privileges on database studentdb to student;`

`postgres=# alter user student createdb;`

Before we run the python scripts, open the Jupyter Notebooks and make sure no previous connection to the database has been made. To be sure, you can restart the Kernel for each notebook. Now in your terminal, navigate to the diretory with you python files. Create the database and tables first:

`python create_tables.py`

or

`python3 create_tables.py`

If there are no errors, then run:

`python etl.py`

or

`python3 etl.py`

To check if the data was loaded, open `test.ipynb` and run all the cells to check the data tables.

<br> 

## The Sparkify Data Schema
The Sparkify data schema consists of 5 tables arranged in a Star Schema. The figure below is a representation of the schema:

<img src="sparkify_schema.jpg" alt="Sparkify Schema" width="800"/>

The fact table is the `songplays` table. It references four dimension tables:
- `songs`
- `artists`
- `users`
- `time`

The schema is in 3rd Normal Form. Each row contains attributes that contain only one value (no lists or tuples for any attribute). 
Each row is also uniquely identified by the primary key attribute. Each table contains attributes that describe the table itself. Attributes related to other tables are linked via foreign keys.

<br>

## Creating the Data Tables
The Sparkify data tables are created inside a database called `sparkifydb`. We connect to this database using python, through a python library called `psycopg2`. This library allows us to pass SQL commmands to the postgresql database within python scripts. The script `sql_queries.py` contains the main SQL commands for creating the five tables of the data schema (see the figure for reference), the foreign key constraints among the tables, and the commands for inserting data into the tables. Each command is stored as a string variable that can be passed to a `psycopg2` instance with a connection to our database.

Another script, `create_tables.py`, sets up the connection with the `sparkifydb` using `psycopg2` and passes the commands from `sql_queries.py` to the database. The script drops any existing data tables (a reset of the database, in effect), and then creates the tables.

<br>

## The ETL Pipeline
After the data tables are created, we perform the Extract, Transform, and Load part of the pipeline. 
The script that handles these steps is the aptly-named `etl.py`. 

### Extract
The raw data comes in the form of JSON files that contain song, artist, user, and songplay session data. The procedure for extracting the data from these JSON files is as follows:

For each JSON file `->` Convert JSON data to Pandas Dataframe `->` For each New Record in Pandas Dataframe `->` Append new record to a global Pandas Dataframe

Rather than INSERTing each new record into the postgresql database one at a time, I am creating a Pandas Dataframe for each data table. The columns of each dataframe matches the name and datatype of its respective data table. Each new record from each JSON file is appended to these dataframes. Then, after all JSON files have been processed, I will convert the dataframes into a CSV file in memory, and then COPY them into the postgres database. This is a bulk upload of the data.

### Transform
The data we extract from the JSON files needs to be cleaned up before we can load them into the database. There were two main issues with the data:

1. There are duplicate records. Our data tables use primary keys to insure the uniqueness of their records. Duplicate records would be rejected if we tried to upload them. We can resolve these issues using the `ON CONFLICT` feature of postgresql. Specifically, we specify `ON CONFLICT DO NOTHING`. In this case, if a conflict arises due to duplicate records, we would not insert the record into the data table and move to the next. However, for the `users` table, we use `ON CONFLICT DO UPDATE` because a user could update their information. For example, a user could upgrade from a Free level to a Paid level.

2. Mixed data types. There are certain data attributes that are of type `int` or `numeric`. When we read JSON data into the Pandas Dataframe, they may contain a mix of data types. The most common example is records of type string, int, and float getting mixed together in one column. To maintain data consistency, we should enforce that int/numeric columns only contain int/numeric values. Again, Pandas has built-in functions that allow us to do this.

3. NULL values. The Songplays data table must query the song and artist id data from the songs and artists tables. For this project, there was only one match returned. Therefore, all the other song_id, artist_id values in Songplays are NULL. Yet, there are no NULL records in either songs or artists. Since there is a foreign key constraint between songplays and songs, and between songplays and artists, a constraint violation error will occur. To remedy this, we simply added one NULL record to songs and artists. This is a temporary remedy, and a more professional solution should be developed.

### Load
At this stage, all records have been extracted from JSON files and stored in Pandas Dataframes. Duplicate records are removed, data types are consistent, and foreign key constraints are met. We now convert the Dataframes into a CSV file, in memory, using the `StringIO` python library. There are some nuances here. First, CSV data are normally comma-separated. However, the records for songplays contain strings with commas in them. This will be interpreted as separate columns. To work around this issue, when we write the Dataframe to CSV, we specify that the separator be tab. Then when we copy the data from CSV into tables, we also specifcy that the separator be tab. This resolved the issue.

Second, as mentioned in the previous section, we have to deal with duplicate records in the raw data. We make use of postgresql's `ON CONFLICT` feature. For all tables *except* users, we specify `ON CONFLICT DO NOTHING` so that if a conflict arises from duplicate records, we move on to the next record. We do not UPDATE the records in this case because we are not sure which version of the record is the actual one. Although the subsequent records may be more accurate, I decided to simplify the loading process and give preference to the first record that was inserted and to ignore subsquent duplicate ones.

I wrote a function `copy_expert_from_io()` that contains the steps we just discussed. First, we have to write the data from each dataframe into memory:

```
from io import StringIO

# save dataframe to an in memory buffer
buffer = StringIO()
table.to_csv(buffer, index=False, header=False, sep='\t')
buffer.seek(0)
```

Next, we form the SQL command to COPY the data from memory into the database. There is no ON CONFLICT feature for COPY FROM. The work around is based on a post from StackOverflow:

https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin-with-csv-do-on-conflic-do-update

I create a temporary table into which I copy data from the CSV. Then, can insert the data from the temporary table into the database table and assign the ON CONFLICT resolution.

```
CREATE TEMPORARY TABLE temp_table
(LIKE {tablename})
ON COMMIT DROP;
COPY temp_table FROM STDIN (FORMAT CSV, DELIMITER E'\t');
INSERT INTO {tablename}
SELECT *
FROM temp_table
ON CONFLICT DO NOTHING;
```

I pass this command and the CSV from memory into `psycopg2`'s `copy_expert()` function to execute it in the database.

For the `users` table, we have to use a different approach because a user could udpate their information. We use this instead:
```
    INSERT INTO users
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE
        SET first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            gender = EXCLUDED.gender,
            level = EXCLUDED.level;
```

Here, we are telling the database to update the records when it comes across a duplicate row on `user_id`. However, there is no way to do a bulk upload while allowing for updates. Therefore, we must process each record one at a time.

## Check the Results
After we run `etl.py` with no errors, we use the `test.ipynb` notebook to look at our database. There are a series of commands that allow us to pass SQL commands directly to the database. We focus on checking the head of the data tables, and the counts, to make sure we have the expected columns, data, and number of records.



