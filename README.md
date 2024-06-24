# Analysis on MovieLens 100k Dataset using Cassandra and Spark

## Introduction
The MovieLens 100k dataset serves as a rich tapestry of user preferences and movie ratings, offering valuable insights into viewer behavior and preferences. 
In this analysis, we delve into the MovieLens 100k dataset using Cassandra for persistent data storage and Spark for agile data processing. By combining these technologies, we able to gain insights on user movie preference in the realm of movie ratings.

## Objectives:
Write a python script that acts as a wrapper function to execute Cassandra Query Language (CQL) and Spark2 Structured 
Query Language (SQL) in order to answer the following questions [display only the top ten results for each question]: 
1. Calculate the average rating for each movie.
2. Identify the top ten movies with the highest average ratings.
3. Find the users who have rated at least 50 movies and identify their favourite movie genres.
4. Find all the users with age that is less than 20 years old.
5. Find all the users who have the occupation “scientist” and their age is between 30 and 40 years old.

## Step: Preparation for Spark SQL Query Execution
#### 1. Create table in Cassandra using CQLSH(Cassandra Query Language Shell)
```
cqlsh:movielens> CREATE TABLE ratings(user_id int, movie_id int, rating int, time int, PRIMARY KEY (user_id, movie_id);
```
>[!NOTE] 
>The biggest challenge I faced during my assignment was when I stored ratings dataframe from u.data into a Cassandra Keyspace and tried to read it back. Instead of the expected 10,000 rows from the original u.data file, Cassandra only returned 994 rows. This discrepancy occurred because initially, I set user_id as the only PRIMARY KEY in Cassandra. However, user_id values are not unique and appear multiple times in the u.data file.<br>
In Cassandra, the PRIMARY KEY uniquely identifies each row within a table partition. When multiple rows in the CSV file share the same user_id, Cassandra stores them as updates to the same row rather than as separate rows. To address this issue, I changed the PRIMARY KEY to include both user_id and movie_id. This composite primary key ensures that each rating entry is unique within the partition. This adjustment not only guarantees data integrity but also facilitates efficient querying of ratings by both user and movie.

```
cqlsh:movielens> CREATE TABLE names (movie_id int, title text, release_date text, vid_release_date text, URL text, unknown int, action int, adventure int, animation int, children int, comedy int, crime int, documentary int, drama int, fantasy int, film_noir int, horror int, musical int, mystery int, romance int, sci_fi int, thriller int, war int, western int, PRIMARY KEY(movie_id));
cqlsh:movielens> CREATE TABLE users (user_id int, age int, gender text, occupation text, zip text, PRIMARY KEY (user_id));
```
#### In putty session:
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
## Questions and Results:
#### Q1: Calculate the average rating for each movie. 
```
    q1 = spark.sql("""SELECT n.title, AVG(rating) AS avgRating  FROM ratings r
                   JOIN names n ON r.movie_id = n.movie_id
                   GROUP BY n.title""")

    print("Table Average Rating for each Movie")
    q1.show(10)
```
![Screenshot 2024-06-24 015049](https://github.com/athirah-o/STQD6324_DataManagement_Assignment4/assets/152348953/4aa7548a-8a47-4e9d-874b-8706687927d9)
#### Q2: Identify the top ten movies with the highest average ratings.
```
    q2 = spark.sql("""SELECT n.title, AVG(rating) AS avgRating, COUNT(*) as rated_count
                   FROM ratings r
                   JOIN names n on r.movie_id = n.movie_id
                   GROUP BY n.title
                   HAVING rated_count > 10
                   ORDER BY avgRating DESC""")

    print("Top 10 Movie with Highest Average Rating with More than 10 being Rated")
    q2.show(10)
```
![Screenshot 2024-06-24 015251](https://github.com/athirah-o/STQD6324_DataManagement_Assignment4/assets/152348953/7fe3c126-0787-45b1-b77c-af348b8f80e5)
#### Q3: Find the users who have rated at least 50 movies and identify their favourite movie genres.
1. Extract user who have rated more than 50 movies
```
    users_50 = spark.sql("""SELECT user_id, COUNT(movie_id) AS rated_count
                FROM ratings
                GROUP BY user_id
                HAVING COUNT(movie_id) >= 50
                ORDER BY user_id ASC""")

    # Create temporary table
    users_50.createOrReplaceTempView("users_50")

    print("Table Users have Rated atleast 50 Movies")
    users_50.show(10)
```
![Screenshot 2024-06-24 015717](https://github.com/athirah-o/STQD6324_DataManagement_Assignment4/assets/152348953/d0951423-dbef-4cb5-87e6-7f199340e720)

2. Identify ratings given by user for each genres: For each user, sum the ratings for each genre
```
    user_genre_ratings = spark.sql("""SELECT
        r.user_id,
        CASE
            WHEN n.action = 1 THEN 'Action'
            WHEN n.adventure = 1 THEN 'Adventure'
            WHEN n.animation = 1 THEN 'Animation'
            WHEN n.children = 1 THEN 'Children'
            WHEN n.comedy = 1 THEN 'Comedy'
            WHEN n.crime = 1 THEN 'Crime'
            WHEN n.documentary = 1 THEN 'Documentary'
            WHEN n.drama = 1 THEN 'Drama'
            WHEN n.fantasy = 1 THEN 'Fantasy'
            WHEN n.film_noir = 1 THEN 'Film-Noir'
            WHEN n.horror = 1 THEN 'Horror'
            WHEN n.musical = 1 THEN 'Musical'
            WHEN n.mystery = 1 THEN 'Mystery'
            WHEN n.romance = 1 THEN 'Romance'
            WHEN n.sci_fi = 1 THEN 'Sci-Fi'
            WHEN n.thriller = 1 THEN 'Thriller'
            WHEN n.war = 1 THEN 'War'
            WHEN n.western = 1 THEN 'Western'
            ELSE 'Unknown'
        END AS genre,
        SUM(r.rating) AS total_rating
    FROM ratings r
    JOIN names n ON r.movie_id = n.movie_id
    JOIN users_50 u ON r.user_id = u.user_id
    GROUP BY r.user_id, genre
    ORDER BY user_id ASC""")

    #Create temporary table
    user_genre_ratings.createOrReplaceTempView("user_genre_ratings")
```
3. Identify favourite genre based on the highest amount of ratings given by user
```
    q3 = spark.sql("""
    SELECT user_id, genre, total_rating
    FROM (
        SELECT user_id, genre, total_rating,
               ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY total_rating DESC) AS row_num
        FROM user_genre_ratings
    ) AS ranked_genres
    WHERE row_num = 1
    ORDER BY user_id
    """)
    print("User's Favourite Genre")
    q3.show(10)
```
![Screenshot 2024-06-24 015730](https://github.com/athirah-o/STQD6324_DataManagement_Assignment4/assets/152348953/5480cb0e-bdd5-402a-a91c-27b01ff04b20)

#### Q4: Find all the users with age that is less than 20 years old. 
```
    q4 = spark.sql("SELECT * FROM users WHERE age < 20")
    print("Users less than 20 years old")
    q4.show(10)
```
![Screenshot 2024-06-24 020221](https://github.com/athirah-o/STQD6324_DataManagement_Assignment4/assets/152348953/cfdbf66e-98b5-4111-a090-a285faba7f09)
#### Q5: Find all the users who have the occupation “scientist” and their age is between 30 and 40 years old.
```
    q5 = spark.sql("SELECT * FROM users WHERE occupation = 'scientist' AND age BETWEEN 30 AND 40")
    print("Scientist and Age between 30 and 40")
    q5.show(10)
```
![Screenshot 2024-06-24 020230](https://github.com/athirah-o/STQD6324_DataManagement_Assignment4/assets/152348953/6d304d7b-a85a-4973-b3a5-444805b51b13)

## Conclusion
The integration of Cassandra and Spark provides a robust framework for analyzing large-scale datasets like MovieLens 100k. Cassandra's distributed architecture complements Spark's processing power, enabling efficient storage, retrieval, and analysis of vast amounts of movie rating data.
