from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

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

if __name__ == "__main__":
    #Create a SparkSession
    spark = SparkSession.builder.appName("Assignment4").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()

    #Get the raw data
    u_user = spark.sparkContext.textFile("hdfs:///user/maria_dev/athirah/ml-100k/u.user")
    u_data = spark.sparkContext.textFile("hdfs:///user/maria_dev/athirah/ml-100k/u.data")
    u_item = spark.sparkContext.textFile("hdfs:///user/maria_dev/athirah/ml-100k/u.item")

    #Convert it to a RDD of Row objects
    users = u_user.map(parseInput1)
    ratings = u_data.map(parseInput2)
    names = u_item.map(parseInput3)

    #Convert into a DataFrame
    users_df = spark.createDataFrame(users)
    ratings_df = spark.createDataFrame(ratings)
    names_df = spark.createDataFrame(names)

    #Write into Cassandra
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

    #Read it back from Cassandra into a new DataFrame
    readUsers = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="users", keyspace="movielens")\
    .load()

    #Read it back from Cassandra into a new DataFrame
    readRatings = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="ratings", keyspace="movielens")\
    .load()

    #Read it back from Cassandra into a new DataFrame
    readNames = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="names", keyspace="movielens")\
    .load()

    readUsers.createOrReplaceTempView("users")
    readRatings.createOrReplaceTempView("ratings")
    readNames.createOrReplaceTempView("names")

    #Q1. Calculate the average rating for each movie
    q1 = spark.sql("""SELECT n.title, AVG(rating) AS avgRating  FROM ratings r
                   JOIN names n ON r.movie_id = n.movie_id
                   GROUP BY n.title""")

    print("Table Average Rating for each Movie")
    q1.show(10)

    #Q2. Identify the top ten movies with the highest average ratings.
    q2 = spark.sql("""SELECT n.title, AVG(rating) AS avgRating, COUNT(*) as rated_count
                   FROM ratings r
                   JOIN names n on r.movie_id = n.movie_id
                   GROUP BY n.title
                   HAVING rated_count > 10
                   ORDER BY avgRating DESC""")

    print("Top 10 Movie with Highest Average Rating with More than 10 being Rated")
    q2.show(10)

    #Q3. Find the users who have rated at least 50 movies and identify their favourite movie genres

    #1. Extract user who have rated more than 50 movies
    users_50 = spark.sql("""SELECT user_id, COUNT(movie_id) AS rated_count
                FROM ratings
                GROUP BY user_id
                HAVING COUNT(movie_id) >= 50
                ORDER BY user_id ASC""")

    print("Table Users have Rated atleast 50 Movies")
    users_50.show(10)

    # Create temporary table
    users_50.createOrReplaceTempView("users_50")
    
	#2. Identify ratings given by user for each genres: For each user, sum the ratings for each genre and determine the favorite genre.
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

    #3. Identify favourite genre based on the highest amount of ratings given by user
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

    #Find all users that is less than 20 years old
    q4 = spark.sql("SELECT * FROM users WHERE age < 20")
    print("Users less than 20 years old")
    q4.show(10)

    #Find all the users who have the occupation scientist and their age is between 30 and 40 years old
    q5 = spark.sql("SELECT * FROM users WHERE occupation = 'scientist' AND age BETWEEN 30 AND 40")
    print("Scientist and Age between 30 and 40")
    q5.show(10)

    #Stop spark session
    spark.stop()
