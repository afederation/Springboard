
## SQL at Scale with Spark SQL

Welcome to the SQL mini project. For this project, you will use the Domino Data Lab Platform and work through a series of exercises using Spark SQL. The dataset size may not be too big but the intent here is to familiarize yourself with the Spark SQL interface which scales easily to huge datasets, without you having to worry about changing your SQL queries. 

The data you need is present in the mini-project folder in the form of three CSV files. You need to make sure that these datasets are uploaded and present in the same directory as this notebook file, since we will be importing these files in Spark and create the following tables under the __`country_club`__ database using Spark SQL.

1. The __`bookings`__ table,
2. The __`facilities`__ table, and
3. The __`members`__ table.

You will be uploading these datasets shortly into Spark to understand how to create a database within minutes! Once the database and the tables are populated, you will be focusing on the mini-project questions.

In the mini project, you'll be asked a series of questions. You can solve them using the Domino platform, but for the final deliverable, please download this notebook as an IPython notebook (__`File -> Export -> IPython Notebook`__) and upload it to your GitHub.

# Checking Existence of Spark Environment Variables

Make sure your notebook is loaded using a PySpark Workspace. If you open up a regular Jupyter workspace the following variables might not exist


```python
spark
```





            <div>
                <p><b>SparkSession - hive</b></p>
                
        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://172.17.0.3:4040">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v2.2.1</code></dd>
              <dt>Master</dt>
                <dd><code>local[*]</code></dd>
              <dt>AppName</dt>
                <dd><code>PySparkShell</code></dd>
            </dl>
        </div>
        
            </div>
        




```python
sqlContext
```




    <pyspark.sql.context.SQLContext at 0x7faec32d9470>



### Run the following if you failed to open a notebook in the PySpark Workspace

This will work assuming you are using Spark in the cloud on domino or you might need to configure with your own spark instance if you are working offline


```python
if 'sc' not in locals():
    from pyspark.context import SparkContext
    from pyspark.sql.context import SQLContext
    from pyspark.sql.session import SparkSession
    
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc)
```

# Create a utility function to run SQL commands

Instead of typing the same python functions repeatedly, we build a small function where you can just pass your query to get results.

- Remember we are using Spark SQL in PySpark
- We can't run multiple SQL statements in one go (no semi-colon ';' separated SQL statements)
- We can run multi-line SQL queries (but still has to be a single statement)


```python
def run_sql(statement):
    try:
        result = sqlContext.sql(statement)
    except Exception as e:
        print(e.desc, '\n', e.stackTrace)
        return
    return result
```

# Creating the Database

We will first create our database in which we will be creating our three tables of interest


```python
run_sql('drop database if exists country_club cascade')
run_sql('create database country_club')
dbs = run_sql('show databases')
dbs.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>databaseName</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>country_club</td>
    </tr>
    <tr>
      <th>1</th>
      <td>default</td>
    </tr>
  </tbody>
</table>
</div>



# Creating the Tables

In this section, we will be creating the three tables of interest and populate them with the data from the CSV files already available to you.

To get started, first make sure you have already uploaded the three CSV files and they are present in the same directory as the notebook.

Once you have done this, please remember to execute the following code to build the dataframes which will be saved as tables in our database


```python
# File location and type
file_location_bookings = "./Bookings.csv"
file_location_facilities = "./Facilities.csv"
file_location_members = "./Members.csv"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
bookings_df = (spark.read.format(file_type) 
                    .option("inferSchema", infer_schema) 
                    .option("header", first_row_is_header) 
                    .option("sep", delimiter) 
                    .load(file_location_bookings))

facilities_df = (spark.read.format(file_type) 
                      .option("inferSchema", infer_schema) 
                      .option("header", first_row_is_header) 
                      .option("sep", delimiter) 
                      .load(file_location_facilities))

members_df = (spark.read.format(file_type) 
                      .option("inferSchema", infer_schema) 
                      .option("header", first_row_is_header) 
                      .option("sep", delimiter) 
                      .load(file_location_members))
```

### Viewing the dataframe schemas

We can take a look at the schemas of our potential tables to be written to our database soon


```python
print('Bookings Schema')
bookings_df.printSchema()
print('Facilities Schema')
facilities_df.printSchema()
print('Members Schema')
members_df.printSchema()
```

    Bookings Schema
    root
     |-- bookid: integer (nullable = true)
     |-- facid: integer (nullable = true)
     |-- memid: integer (nullable = true)
     |-- starttime: timestamp (nullable = true)
     |-- slots: integer (nullable = true)
    
    Facilities Schema
    root
     |-- facid: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- membercost: double (nullable = true)
     |-- guestcost: double (nullable = true)
     |-- initialoutlay: integer (nullable = true)
     |-- monthlymaintenance: integer (nullable = true)
    
    Members Schema
    root
     |-- memid: integer (nullable = true)
     |-- surname: string (nullable = true)
     |-- firstname: string (nullable = true)
     |-- address: string (nullable = true)
     |-- zipcode: integer (nullable = true)
     |-- telephone: string (nullable = true)
     |-- recommendedby: integer (nullable = true)
     |-- joindate: timestamp (nullable = true)
    


# Create permanent tables
We will be creating three permanent tables here in our __`country_club`__ database as we discussed previously with the following code


```python
permanent_table_name_bookings = "country_club.Bookings"
bookings_df.write.format("parquet").saveAsTable(permanent_table_name_bookings)

permanent_table_name_facilities = "country_club.Facilities"
facilities_df.write.format("parquet").saveAsTable(permanent_table_name_facilities)

permanent_table_name_members = "country_club.Members"
members_df.write.format("parquet").saveAsTable(permanent_table_name_members)
```

### Refresh tables and check them


```python
run_sql('use country_club')
run_sql('REFRESH table bookings')
run_sql('REFRESH table facilities')
run_sql('REFRESH table members')
tbls = run_sql('show tables')
tbls.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>database</th>
      <th>tableName</th>
      <th>isTemporary</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>country_club</td>
      <td>bookings</td>
      <td>False</td>
    </tr>
    <tr>
      <th>1</th>
      <td>country_club</td>
      <td>facilities</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>country_club</td>
      <td>members</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
</div>



# Test a sample SQL query

__Note:__ You can use multi-line SQL queries (but still a single statement) as follows


```python
result = run_sql('''
                    SELECT * 
                    FROM bookings 
                    LIMIT 3
                 ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>bookid</th>
      <th>facid</th>
      <th>memid</th>
      <th>starttime</th>
      <th>slots</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>3</td>
      <td>1</td>
      <td>2012-07-03 11:00:00</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>4</td>
      <td>1</td>
      <td>2012-07-03 08:00:00</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>6</td>
      <td>0</td>
      <td>2012-07-03 18:00:00</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python
result = run_sql('''
                    SELECT * 
                    FROM facilities 
                    LIMIT 3
                 ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>facid</th>
      <th>name</th>
      <th>membercost</th>
      <th>guestcost</th>
      <th>initialoutlay</th>
      <th>monthlymaintenance</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Tennis Court 1</td>
      <td>5.0</td>
      <td>25.0</td>
      <td>10000</td>
      <td>200</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Tennis Court 2</td>
      <td>5.0</td>
      <td>25.0</td>
      <td>8000</td>
      <td>200</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Badminton Court</td>
      <td>0.0</td>
      <td>15.5</td>
      <td>4000</td>
      <td>50</td>
    </tr>
  </tbody>
</table>
</div>




```python
result = run_sql('''
                    SELECT * 
                    FROM members 
                    LIMIT 3
                 ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>memid</th>
      <th>surname</th>
      <th>firstname</th>
      <th>address</th>
      <th>zipcode</th>
      <th>telephone</th>
      <th>recommendedby</th>
      <th>joindate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>GUEST</td>
      <td>GUEST</td>
      <td>GUEST</td>
      <td>0</td>
      <td>(000) 000-0000</td>
      <td>None</td>
      <td>2012-07-01 00:00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Smith</td>
      <td>Darren</td>
      <td>8 Bloomsbury Close, Boston</td>
      <td>4321</td>
      <td>555-555-5555</td>
      <td>None</td>
      <td>2012-07-02 12:02:05</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Smith</td>
      <td>Tracy</td>
      <td>8 Bloomsbury Close, New York</td>
      <td>4321</td>
      <td>555-555-5555</td>
      <td>None</td>
      <td>2012-07-02 12:08:23</td>
    </tr>
  </tbody>
</table>
</div>



# Your Turn: Solve the following questions with Spark SQL

- Make use of the `run_sql(...)` function as seen in the previous example
- You can write multi-line SQL queries but it has to be a single statement (no use of semi-colons ';')
- Make use of the `toPandas()` function as depicted in the previous example to display the query results

#### Q1: Some of the facilities charge a fee to members, but some do not. Please list the names of the facilities that do.


```python
result = run_sql('''
                    SELECT DISTINCT name, membercost
                    FROM facilities 
                    WHERE membercost > 0
                 ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>membercost</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Massage Room 2</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Massage Room 1</td>
      <td>9.9</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Squash Court</td>
      <td>3.5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Tennis Court 2</td>
      <td>5.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Tennis Court 1</td>
      <td>5.0</td>
    </tr>
  </tbody>
</table>
</div>



####  Q2: How many facilities do not charge a fee to members?


```python
result = run_sql('''
                    SELECT COUNT(DISTINCT name)
                    FROM facilities 
                    WHERE membercost = 0
                 ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>count(DISTINCT name)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



#### Q3: How can you produce a list of facilities that charge a fee to members, where the fee is less than 20% of the facility's monthly maintenance cost? 
#### Return the facid, facility name, member cost, and monthly maintenance of the facilities in question.


```python
result = run_sql('''
                 SELECT facid, name, membercost, monthlymaintenance 
                 FROM facilities
                 WHERE (CAST(membercost as DECIMAL) / monthlymaintenance < 0.2) AND
                         (membercost > 0)
                ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>facid</th>
      <th>name</th>
      <th>membercost</th>
      <th>monthlymaintenance</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Tennis Court 1</td>
      <td>5.0</td>
      <td>200</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Tennis Court 2</td>
      <td>5.0</td>
      <td>200</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4</td>
      <td>Massage Room 1</td>
      <td>9.9</td>
      <td>3000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>5</td>
      <td>Massage Room 2</td>
      <td>9.9</td>
      <td>3000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>6</td>
      <td>Squash Court</td>
      <td>3.5</td>
      <td>80</td>
    </tr>
  </tbody>
</table>
</div>



#### Q4: How can you retrieve the details of facilities with ID 1 and 5? Write the query without using the OR operator.


```python
result = run_sql('''
                   SELECT * 
                   FROM facilities
                   WHERE facid IN (1,5)
                ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>facid</th>
      <th>name</th>
      <th>membercost</th>
      <th>guestcost</th>
      <th>initialoutlay</th>
      <th>monthlymaintenance</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Tennis Court 2</td>
      <td>5.0</td>
      <td>25.0</td>
      <td>8000</td>
      <td>200</td>
    </tr>
    <tr>
      <th>1</th>
      <td>5</td>
      <td>Massage Room 2</td>
      <td>9.9</td>
      <td>80.0</td>
      <td>4000</td>
      <td>3000</td>
    </tr>
  </tbody>
</table>
</div>



#### Q5: How can you produce a list of facilities, with each labelled as 'cheap' or 'expensive', depending on if their monthly maintenance cost is more than $100? 
#### Return the name and monthly maintenance of the facilities in question.


```python
result = run_sql('''
                SELECT name, CASE
                          WHEN monthlymaintenance > 100 
                             THEN 'expensive'
                          ELSE 'cheap'
                             END
                    as label
                FROM facilities
                   
                 ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>label</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Tennis Court 1</td>
      <td>expensive</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Tennis Court 2</td>
      <td>expensive</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Badminton Court</td>
      <td>cheap</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Table Tennis</td>
      <td>cheap</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Massage Room 1</td>
      <td>expensive</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Massage Room 2</td>
      <td>expensive</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Squash Court</td>
      <td>cheap</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Snooker Table</td>
      <td>cheap</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Pool Table</td>
      <td>cheap</td>
    </tr>
  </tbody>
</table>
</div>



#### Q6: You'd like to get the first and last name of the last member(s) who signed up. Do not use the LIMIT clause for your solution.


```python
result = run_sql('''
                    SELECT firstname, surname, joindate
                    FROM members
                    ORDER BY joindate DESC
                 ''')
result.toPandas()

## Not sure how to only return the closest without LIMIT
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>firstname</th>
      <th>surname</th>
      <th>joindate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Darren</td>
      <td>Smith</td>
      <td>2012-09-26 18:08:45</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Erica</td>
      <td>Crumpet</td>
      <td>2012-09-22 08:36:38</td>
    </tr>
    <tr>
      <th>2</th>
      <td>John</td>
      <td>Hunt</td>
      <td>2012-09-19 11:32:45</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Hyacinth</td>
      <td>Tupperware</td>
      <td>2012-09-18 19:32:05</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Millicent</td>
      <td>Purview</td>
      <td>2012-09-18 19:04:01</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Henry</td>
      <td>Worthington-Smyth</td>
      <td>2012-09-17 12:27:15</td>
    </tr>
    <tr>
      <th>6</th>
      <td>David</td>
      <td>Farrell</td>
      <td>2012-09-15 08:22:05</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Henrietta</td>
      <td>Rumney</td>
      <td>2012-09-05 08:42:35</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Douglas</td>
      <td>Jones</td>
      <td>2012-09-02 18:43:05</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Ramnaresh</td>
      <td>Sarwin</td>
      <td>2012-09-01 08:44:42</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Joan</td>
      <td>Coplin</td>
      <td>2012-08-29 08:32:41</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Anna</td>
      <td>Mackenzie</td>
      <td>2012-08-26 09:32:05</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Matthew</td>
      <td>Genting</td>
      <td>2012-08-19 14:55:55</td>
    </tr>
    <tr>
      <th>13</th>
      <td>David</td>
      <td>Pinker</td>
      <td>2012-08-16 11:32:47</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Timothy</td>
      <td>Baker</td>
      <td>2012-08-15 10:34:25</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Florence</td>
      <td>Bader</td>
      <td>2012-08-10 17:52:03</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Jack</td>
      <td>Smith</td>
      <td>2012-08-10 16:22:05</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Jemima</td>
      <td>Farrell</td>
      <td>2012-08-10 14:28:01</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Anne</td>
      <td>Baker</td>
      <td>2012-08-10 14:23:22</td>
    </tr>
    <tr>
      <th>19</th>
      <td>David</td>
      <td>Jones</td>
      <td>2012-08-06 16:32:55</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Charles</td>
      <td>Owen</td>
      <td>2012-08-03 19:42:37</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Ponder</td>
      <td>Stibbons</td>
      <td>2012-07-25 17:09:05</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Tim</td>
      <td>Boothe</td>
      <td>2012-07-25 16:02:35</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Nancy</td>
      <td>Dare</td>
      <td>2012-07-25 08:59:12</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Burton</td>
      <td>Tracy</td>
      <td>2012-07-15 08:52:55</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Gerald</td>
      <td>Butters</td>
      <td>2012-07-09 10:44:09</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Janice</td>
      <td>Joplette</td>
      <td>2012-07-03 10:25:05</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Tim</td>
      <td>Rownam</td>
      <td>2012-07-03 09:32:15</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Tracy</td>
      <td>Smith</td>
      <td>2012-07-02 12:08:23</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Darren</td>
      <td>Smith</td>
      <td>2012-07-02 12:02:05</td>
    </tr>
    <tr>
      <th>30</th>
      <td>GUEST</td>
      <td>GUEST</td>
      <td>2012-07-01 00:00:00</td>
    </tr>
  </tbody>
</table>
</div>



####  Q7: How can you produce a list of all members who have used a tennis court?
- Include in your output the name of the court, and the name of the member formatted as a single column. 
- Ensure no duplicate data
- Also order by the member name.


```python
result = run_sql('''   
                  SELECT CONCAT(firstname, ' ', surname) as name, CASE
                              WHEN facid = 1
                                  THEN 'Tennis Court 1'
                               ELSE 
                                   'Tennis Court 2' END AS courtname
                    FROM members 
                    INNER JOIN(
                        SELECT DISTINCT facid, memid
                        FROM bookings
                        WHERE CAST(facid AS INT) IN (1,2)
                        )
                        USING(memid)
                    ORDER BY surname
                 ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>courtname</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Florence Bader</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Florence Bader</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Anne Baker</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Anne Baker</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Timothy Baker</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Timothy Baker</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Tim Boothe</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Tim Boothe</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Gerald Butters</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Gerald Butters</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Erica Crumpet</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Nancy Dare</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Nancy Dare</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Jemima Farrell</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Jemima Farrell</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>15</th>
      <td>David Farrell</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>16</th>
      <td>GUEST GUEST</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>17</th>
      <td>GUEST GUEST</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>18</th>
      <td>John Hunt</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>19</th>
      <td>John Hunt</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>20</th>
      <td>David Jones</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>21</th>
      <td>David Jones</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Douglas Jones</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Janice Joplette</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Anna Mackenzie</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Charles Owen</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Charles Owen</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>27</th>
      <td>David Pinker</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Millicent Purview</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Millicent Purview</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>30</th>
      <td>Tim Rownam</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>31</th>
      <td>Tim Rownam</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>32</th>
      <td>Henrietta Rumney</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>33</th>
      <td>Ramnaresh Sarwin</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>34</th>
      <td>Ramnaresh Sarwin</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>35</th>
      <td>Darren Smith</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>36</th>
      <td>Darren Smith</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>37</th>
      <td>Tracy Smith</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>38</th>
      <td>Tracy Smith</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>39</th>
      <td>Jack Smith</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>40</th>
      <td>Jack Smith</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>41</th>
      <td>Ponder Stibbons</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>42</th>
      <td>Ponder Stibbons</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>43</th>
      <td>Burton Tracy</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>44</th>
      <td>Burton Tracy</td>
      <td>Tennis Court 1</td>
    </tr>
    <tr>
      <th>45</th>
      <td>Hyacinth Tupperware</td>
      <td>Tennis Court 2</td>
    </tr>
    <tr>
      <th>46</th>
      <td>Henry Worthington-Smyth</td>
      <td>Tennis Court 2</td>
    </tr>
  </tbody>
</table>
</div>



#### Q8: How can you produce a list of bookings on the day of 2012-09-14 which will cost the member (or guest) more than $30? 

- Remember that guests have different costs to members (the listed costs are per half-hour 'slot')
- The guest user's ID is always 0. 

#### Include in your output the name of the facility, the name of the member formatted as a single column, and the cost.

- Order by descending cost, and do not use any subqueries.


```python
result = run_sql('''
                SELECT f.name as facility, CONCAT(firstname, ' ', surname) as name, total_cost
                FROM members AS m
                RIGHT JOIN(
                   SELECT b.facid, f.name, b.memid, b.slots, f.membercost, f.guestcost, 
                       (CASE WHEN memid = 0
                           THEN b.slots*f.guestcost
                           ELSE b.slots*f.membercost END) AS total_cost
                   FROM bookings AS b
                       LEFT JOIN facilities AS f
                       ON b.facid = f.facid
                   WHERE CAST(b.starttime AS DATE) = CAST('2012-09-14' AS DATE)
                   )
                   USING(memid)
                   WHERE total_cost > 30
                   ORDER BY total_cost DESC
                 ''')
result.toPandas()

# QUESTION: I tried not using subqueries, but was getting an error that wouldn't let me ust the calculated total_cost
# Other suggestions?
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>facility</th>
      <th>name</th>
      <th>total_cost</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Massage Room 2</td>
      <td>GUEST GUEST</td>
      <td>320.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Massage Room 1</td>
      <td>GUEST GUEST</td>
      <td>160.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Massage Room 1</td>
      <td>GUEST GUEST</td>
      <td>160.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Massage Room 1</td>
      <td>GUEST GUEST</td>
      <td>160.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Tennis Court 2</td>
      <td>GUEST GUEST</td>
      <td>150.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Tennis Court 1</td>
      <td>GUEST GUEST</td>
      <td>75.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Tennis Court 1</td>
      <td>GUEST GUEST</td>
      <td>75.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Tennis Court 2</td>
      <td>GUEST GUEST</td>
      <td>75.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Squash Court</td>
      <td>GUEST GUEST</td>
      <td>70.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Massage Room 1</td>
      <td>Jemima Farrell</td>
      <td>39.6</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Squash Court</td>
      <td>GUEST GUEST</td>
      <td>35.0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Squash Court</td>
      <td>GUEST GUEST</td>
      <td>35.0</td>
    </tr>
  </tbody>
</table>
</div>



#### Q9: This time, produce the same result as in Q8, but using a subquery.


```python
result = run_sql('''
                SELECT f.name as facility, CONCAT(firstname, ' ', surname) as name, total_cost
                FROM members AS m
                RIGHT JOIN(
                   SELECT b.facid, f.name, b.memid, b.slots, f.membercost, f.guestcost, 
                       (CASE WHEN memid = 0
                           THEN b.slots*f.guestcost
                           ELSE b.slots*f.membercost END) AS total_cost
                   FROM bookings AS b
                       LEFT JOIN facilities AS f
                       ON b.facid = f.facid
                   WHERE CAST(b.starttime AS DATE) = CAST('2012-09-14' AS DATE)
                   )
                   USING(memid)
                   WHERE total_cost > 30
                   ORDER BY total_cost DESC
                 ''')
result.toPandas()

# QUESTION: I tried not using subqueries, but was getting an error that wouldn't let me ust the calculated total_cost
# Other suggestions?
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>facility</th>
      <th>name</th>
      <th>total_cost</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Massage Room 2</td>
      <td>GUEST GUEST</td>
      <td>320.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Massage Room 1</td>
      <td>GUEST GUEST</td>
      <td>160.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Massage Room 1</td>
      <td>GUEST GUEST</td>
      <td>160.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Massage Room 1</td>
      <td>GUEST GUEST</td>
      <td>160.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Tennis Court 2</td>
      <td>GUEST GUEST</td>
      <td>150.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Tennis Court 1</td>
      <td>GUEST GUEST</td>
      <td>75.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Tennis Court 1</td>
      <td>GUEST GUEST</td>
      <td>75.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Tennis Court 2</td>
      <td>GUEST GUEST</td>
      <td>75.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Squash Court</td>
      <td>GUEST GUEST</td>
      <td>70.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Massage Room 1</td>
      <td>Jemima Farrell</td>
      <td>39.6</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Squash Court</td>
      <td>GUEST GUEST</td>
      <td>35.0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Squash Court</td>
      <td>GUEST GUEST</td>
      <td>35.0</td>
    </tr>
  </tbody>
</table>
</div>



#### Q10: Produce a list of facilities with a total revenue less than 1000.
- The output should have facility name and total revenue, sorted by revenue. 
- Remember that there's a different cost for guests and members!


```python
# Revenue how often?

result = run_sql('''
                   SELECT f.name as Facility,
                       SUM(CASE WHEN memid = 0
                           THEN b.slots*f.guestcost
                           ELSE b.slots*f.membercost END) AS Revenue
                   FROM bookings AS b
                       LEFT JOIN facilities AS f
                       ON b.facid = f.facid
                    WHERE Revenue < 1000
                    GROUP BY b.facid, f.name
                    ORDER BY Revenue
                 ''')
result.toPandas()

# This is the same error as above. Can't figure out why I can't use Revenue.. Google makes it seem like a Spark quirk?
```

    cannot resolve '`Revenue`' given input columns: [membercost, initialoutlay, facid, name, facid, slots, starttime, monthlymaintenance, guestcost, memid, bookid]; line 9 pos 26;
    'Sort ['Revenue ASC NULLS FIRST], true
    +- 'Aggregate ['b.facid, 'f.name], ['f.name AS Facility#634, 'SUM(CASE WHEN ('memid = 0) THEN ('b.slots * 'f.guestcost) ELSE ('b.slots * 'f.membercost) END) AS Revenue#635]
       +- 'Filter ('Revenue < 1000)
          +- Join LeftOuter, (facid#220 = facid#230)
             :- SubqueryAlias b
             :  +- SubqueryAlias bookings
             :     +- Relation[bookid#219,facid#220,memid#221,starttime#222,slots#223] parquet
             +- SubqueryAlias f
                +- SubqueryAlias facilities
                   +- Relation[facid#230,name#231,membercost#232,guestcost#233,initialoutlay#234,monthlymaintenance#235] parquet
     
     org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
    	 at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:88)
    	 at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:85)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)
    	 at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:288)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:286)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:286)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$4.apply(TreeNode.scala:306)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:304)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:286)
    	 at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:268)
    	 at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:268)
    	 at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression$1(QueryPlan.scala:279)
    	 at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1(QueryPlan.scala:289)
    	 at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$6.apply(QueryPlan.scala:298)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)
    	 at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:298)
    	 at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsUp(QueryPlan.scala:268)
    	 at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:85)
    	 at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:78)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:127)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)
    	 at scala.collection.immutable.List.foreach(List.scala:381)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:126)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)
    	 at scala.collection.immutable.List.foreach(List.scala:381)
    	 at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:126)
    	 at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.checkAnalysis(CheckAnalysis.scala:78)
    	 at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:91)
    	 at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:52)
    	 at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:67)
    	 at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:632)
    	 at sun.reflect.GeneratedMethodAccessor82.invoke(Unknown Source)
    	 at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    	 at java.lang.reflect.Method.invoke(Method.java:498)
    	 at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
    	 at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
    	 at py4j.Gateway.invoke(Gateway.java:280)
    	 at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
    	 at py4j.commands.CallCommand.execute(CallCommand.java:79)
    	 at py4j.GatewayConnection.run(GatewayConnection.java:214)
    	 at java.lang.Thread.run(Thread.java:748)



    ---------------------------------------------------------------------------

    AttributeError                            Traceback (most recent call last)

    <ipython-input-127-f4b9e04ec3e1> in <module>()
         13                     ORDER BY Revenue
         14                  ''')
    ---> 15 result.toPandas()
    

    AttributeError: 'NoneType' object has no attribute 'toPandas'



```python
# Revenue how often?

result = run_sql('''
                   SELECT f.name as Facility,
                       SUM(CASE WHEN memid = 0
                           THEN b.slots*f.guestcost
                           ELSE b.slots*f.membercost END) AS Revenue
                   FROM bookings AS b
                       LEFT JOIN facilities AS f
                       ON b.facid = f.facid
                    GROUP BY b.facid, f.name
                    ORDER BY Revenue
                    
                 ''')
result.toPandas()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Facility</th>
      <th>Revenue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Table Tennis</td>
      <td>180.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Snooker Table</td>
      <td>240.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Pool Table</td>
      <td>270.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Badminton Court</td>
      <td>1906.5</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Squash Court</td>
      <td>13468.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Tennis Court 1</td>
      <td>13860.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Tennis Court 2</td>
      <td>14310.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Massage Room 2</td>
      <td>14454.6</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Massage Room 1</td>
      <td>50351.6</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
