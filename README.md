# STQD6324_DataManagement_Assignment4
## Instruction:
Write a python script that acts as a wrapper function to execute Cassandra Query Language (CQL) and Spark2 Structured 
Query Language (SQL) in order to answer the following questions [display only the top ten results for each question]: 
1. Calculate the average rating for each movie.
2. Identify the top ten movies with the highest average ratings.
3. Find the users who have rated at least 50 movies and identify their favourite movie genres.
4. Find all the users with age that is less than 20 years old.
5. Find all the users who have the occupation “scientist” and their age is between 30 and 40 years old.

#### 1. Create table in Cassandra using CQLSH(Cassandra Query Language Shell)
```
cqlsh:movielens> CREATE TABLE ratings(user_id int, movie_id int, rating int, time int, PRIMARY KEY (user_id, movie_id);
cqlsh:movielens> CREATE TABLE names (movie_id int, title text, release_date text, vid_release_date text, URL text, unknown int, action int, adventure int, animation int, children int, comedy int, crime int, documentary int, drama int, fantasy int, film_noir int, horror int, musical int, mystery int, romance int, sci_fi int, thriller int, war int, western int, PRIMARY KEY(movie_id));
cqlsh:movielens> CREATE TABLE users (user_id int, age int, gender text, occupation text, zip text, PRIMARY KEY (user_id));
```
#### 2. Set the Spark SQL running environment
```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
```
#### 3. Define function to parse u.data, u.item and u.user
```
def parseInput1(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

def parseInput2(line):
    fields = line.split("\t")
    return Row(user_id = int(fields[0]), movie_id = int(fields[1]), rating = int(fields[2]), time = int(fields[3]))

def parseInput3(line):
    fields = line.split("|")
    return Row(movie_id = int(fields[0]), title = fields[1], release_date = fields[2], vid_release_date = fields[3], url = fields[4],
    unknown = int(fields[5]), action = int(fields[6]), adventure = int(fields[7]), animation = int(fields[8]), children =int(fields[9]),
    comedy = int(fields[10]), crime = int(fields[11]), documentary = int(fields[12]), drama = int(fields[13]), fantasy = int(fields[14]),
    film_noir = int(fields[15]), horror = int(fields[16]), musical = int(fields[17]), mystery = int(fields[18]), romance = int(fields[19]),
    sci_fi = int(fields[20]), thriller = int(fields[21]), war = int(fields[22]), western = int(fields[23]))
```
#### 4. Initialize spark session connection with cassandra
```
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Assignment4").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()
```

#### 5. Get raw data from HDFS
```
    u_user = spark.sparkContext.textFile("hdfs:///user/maria_dev/athirah/ml-100k/u.user")
    u_data = spark.sparkContext.textFile("hdfs:///user/maria_dev/athirah/ml-100k/u.data")
    u_item = spark.sparkContext.textFile("hdfs:///user/maria_dev/athirah/ml-100k/u.item")
```
#### 6. Convert raw data to a RDD of row objects
```
    users = u_user.map(parseInput1)
    ratings = u_data.map(parseInput2)
    names = u_item.map(parseInput3)
```
### 7. Convert into a spark dataframe
```
    users_df = spark.createDataFrame(users)
    ratings_df = spark.createDataFrame(ratings)
    names_df = spark.createDataFrame(names)
```
#### 8. Write into Cassandra
```
    users_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="users", keyspace="movielens")\
        .save()

    ratings_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="ratings", keyspace="movielens")\
        .save()

    names_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="names", keyspace="movielens")\
        .save()
```
### 9. Read it back from Cassandra into new dataframe
```
    readUsers = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="users", keyspace="movielens")\
    .load()
    
    readRatings = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="ratings", keyspace="movielens")\
    .load()
    
    readNames = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="names", keyspace="movielens")\
    .load()
```
#### 10. Create temporary view table for spark.sql queary
```
    readUsers.createOrReplaceTempView("users")
    readRatings.createOrReplaceTempView("ratings")
    readNames.createOrReplaceTempView("names")
```
### Q
