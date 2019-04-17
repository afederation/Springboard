# Exercise for "Build a data pipeline with Luigi"

For this exercise, you will build a simple data pipeline by taking some data from Amazon S3, cleaning it, manipulating it, and putting it into a SQL database.

You have two types of log files in a bucket in Amazon S3:
1. The **UserUpdates** folder contains CSV files. The rows in the CSV represent updates to information about users of a web application. The columns are `updated_at, user_id', email, name, environment`:

  ```
  "2017-04-01T00:00:42","1572","XWxBPKqjkeV@example.com","Cheri Honey","production"
  "2017-04-01T00:12:29","320","IOwCgGiXKW@example.com","Maisha Goosby","production"
  "2017-04-01T00:23:19","802","sQSGyRmbyM@example.com","Love Vizcarra","production"
  ```
2. The **PageViews** folder contains log files. Each row is a JSON blob which represents a page view event - a specific page that a user visited at a specific time. Here are some examples:

  ```
  {"ts": "2017-04-15T17:09:09", "url": "/products/916/", "environment": "production", "user_id": 2083}
  {"ts": "2017-04-15T17:09:18", "url": "/products/834/", "environment": "production", "user_id": 2280}
  {"ts": "2017-04-15T17:09:48", "url": "/products/6/", "environment": "production", "user_id": 2177}
  ```

Using this data, please create a table in a SQL database called `user_page_views`. This table represents a summary of each page that a user has visited. Note that the columns included should be `user_id, email, url, count, last_viewed`:
  
  ```
  # select * from user_page_views limit 1;
   user_id | email           | url        | count | last_viewed
  ---------+-----------------+------------+-------+--------------------
   123     | new@example.com | /pages/123 | 10    | 2017-05-02 00:01:23
   123     | new@example.com | /pages/456 | 10    | 2017-05-02 00:02:34
  ```

### Notes: 
- This will involve joining data from both UserUpdates (for the latest email address) and PageViews.
- Imagine that this is something that you want to run daily with a reasonably large set of data. You should be able to re-populate the data by re-running the job.
- There is some duplication of data across the log files! You'll need to remove duplicates as part of your pipeline.
- There is an “environment” in each of the logs. We only want the data for the 'production' environment.
- If you don’t have a database installed on your machine and don't want to install one, you can export the data into CSV files instead.

### Downloading the data:
- A URL for the AWS credentials needed to download the data will be shared with you.

### Example:
The example.py file demonstrates a few important steps of this exercise. It has jobs that demonstrate:
- Downloading the files from Amazon S3
- Executing a shell script in a Luigi task
- Writing data from a CSV file to a SQL database table

If you have Postgres installed, you can run this example script by doing the following:
1. Create a database called 'luigi_exercise': `createdb luigi_exercise`
2. Install the packages in the requirements.txt file
3. Rename the 'luigi.cfg.tmp' file to 'luigi.cfg' and fill in the missing credentials (database user and AWS key/secret)
4. From a shell terminal, run `python example.py WriteUserUpdatesToSQL --id=1`

### Extra credit:
- Create tables in a separate database for staging data.
- Output the results to both SQL tables and CSV files.
- Create a `page_recommendations` table that shows the top 3 recommendations for each user by comparing the pages they have viewed with other users' page views.

  ```
  # select * from page_recommendations limit 1;
   user_id | email         | recommended_url   | rank 
  ---------+---------------+-------------------+-------
   123     | a@example.com | /products/456     | 1
   123     | a@example.com | /products/789     | 2
   123     | a@example.com | /products/123     | 3
  ```
  Use any algorithm for determining 'recommended' urls that you can come up with!
