import pandas as pd
from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

genres = "Art", "Biography", "Business", "Chick Lit",\
         "Children's", "Christian", "Classics",\
         "Comics", "Contemporary", "Cookbooks",\
         "Crime", "Ebooks", "Fantasy", "Fiction",\
         "Gay and Lesbian", "Graphic Novels", "Historical Fiction",\
         "History", "Horror", "Humor and Comedy", "Manga", "Memoir",\
         "Music", "Mystery", "Nonfiction", "Paranormal", "Philosophy",\
         "Poetry", "Psychology", "Religion", "Romance", "Science",\
         "Science Fiction", "Self Help", "Suspense", "Spirituality",\
         "Sports", "Thriller", "Travel", "Young Adult"

if __name__ == '__main__':
    #carica MongoDB
    client = MongoClient()
    db = client.mydb

    #Trasforma da csv a pandas

    df_book_tags = pd.read_csv("/home/ene/PycharmProjects/Tesi/book_tags.csv")
    #df_books = pd.read_csv("/home/ene/PycharmProjects/Tesi/books.csv")
    #df_ratings = pd.read_csv("/home/ene/PycharmProjects/Tesi/ratings.csv")
    df_tags = pd.read_csv("/home/ene/PycharmProjects/Tesi/tags.csv")
    #df_toRead = pd.read_csv("/home/ene/PycharmProjects/Tesi/to_read.csv")

    #sistema campi che danno errori
#    df_books.language_code = df_books.language_code.astype(str)
#    df_books.original_title = df_books.original_title.astype(str)
#    df_books.isbn = df_books.isbn.astype(str)
#    df_books.isbn13 = df_books.isbn13.astype(float)
#    df_books.isbn13 = df_books.isbn13.astype(str)

    # crea una spark session
    spark = SparkSession.builder.getOrCreate()

    # converte da pandas a i spark dataframe

    spark_df_book_tags = spark.createDataFrame(df_book_tags)
    #spark_df_books = spark.createDataFrame(df_books)
    #spark_df_ratings = spark.createDataFrame(df_ratings)
    spark_df_tags = spark.createDataFrame(df_tags)
    #spark_df_toRead = spark.createDataFrame(df_toRead)

    # Add the spark data frame to the catalog

    #spark_df_book_tags.createOrReplaceTempView('book_tags')
    #spark_df_books.createOrReplaceTempView('books')
    #spark_df_ratings.createOrReplaceTempView('ratings')
    #spark_df_tags.createOrReplaceTempView('tags')
    #spark_df_toRead.createOrReplaceTempView('to_read')

    def topNTags4Books2MongoDB():
        from pyspark.sql.functions import rank, col
        maxTagsForBook = 10
        window = Window.partitionBy(spark_df_book_tags['goodreads_book_id']).orderBy(spark_df_book_tags['count'].desc())
        top_n_tags_per_books = spark_df_book_tags.select('*', rank().over(window).alias('rank')).filter(col('rank') <= maxTagsForBook).orderBy('goodreads_book_id')\
            .select("goodreads_book_id",col('tag_id').alias('tag_id_alfa'))
        top_n_tags_per_books_with_name = top_n_tags_per_books.join(spark_df_tags,top_n_tags_per_books.tag_id_alfa == spark_df_tags.tag_id).select('goodreads_book_id','tag_id','tag_name')
        pandas_tags_per_books_with_name = top_n_tags_per_books_with_name.toPandas()
        column = db.Books_Tags
        column.insert_many(pandas_tags_per_books_with_name.to_dict('records'))

        #per vedere i tag maggiormente utilizzati
        #pandas_tags_per_books_with_name = top_n_tags_per_books_with_name.select("tag_name").groupBy("tag_name").count()
        #for row in pandas_tags_per_books_with_name.sort("count",ascending=False).collect():
        #    print(row)
    def printone(x):
        print(x)
        print("\n")

    def users2ratings():
        spark_df_ratings\
            .rdd.map(lambda row : (row["user_id"],[[row["book_id"],row["rating"]]]))\
            .reduceByKey(lambda row1,row2 : row1+row2)\
            .foreach(printone)


    def books2ratings():
        spark_df_ratings \
            .rdd.map(lambda row: (row["book_id"], [{"user_id" : row["user_id"],"rating" : row["rating"]}])) \
            .reduceByKey(lambda row1, row2: row1 + row2) \
            .foreach(printone)



    def books2tags():
        # esempio (20405, ((2570856, 33), 'monsters'))
        #(29991719, [{'tag_id': 11743, 'count': 41, 'tag_name': 'fiction'}])
        def saveToMongoDB(row):
            new_row = {"gr_id":row[0],"data":row[1]}

            column = db.BooksTags
            column.insert_one(new_row)

        def filterino(row):
            if(row[1][1] == "fiction" or row[1][1] == "biography"):
                return row
            else:
                return None

        tempview = spark_df_tags.rdd\
            .map(lambda row : (row["tag_id"],(row["tag_name"])))\
            #.filter(lambda row : row[1] == "fiction" or row[1] == "biography")\
            #.foreach(printone)

        #{"goodreads_book_id" : row["goodreads_book_id"] , "count" : row["count"]}
        #(11743, ((33288638, 21), 'fiction'))
        gamma = spark_df_book_tags.rdd\
            .map(lambda row : (row["tag_id"],(row["goodreads_book_id"],row["count"])))\
            .leftOuterJoin(tempview) \
            .filter(filterino) \
            .map(lambda row : (row[1][0][0],[{"tag_id":row[0],"count":row[1][0][1],"tag_name" :str(row[1][1])}])) \
            .reduceByKey(lambda row1, row2: row1 + row2)\
            .collect()
        for row in gamma :
            new_row = {"gr_id": row[0], "data": row[1]}
            print(row[0])
            print(new_row)
            print(type(new_row))
            new_row = {"gr_id": row[0], "data": row[1]}
            column = db.BooksTags
            column.insert_one(new_row)
        print("alfa")

        #.map()
            #.reduceByKey(lambda row1,row2 : row1 + row2)\



    #user2ratings()
    #books2ratings()
    books2tags()
    #topNTags4Books2MongoDB()
    spark.stop()