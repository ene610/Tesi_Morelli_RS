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

    spark_df_book_tags.createOrReplaceTempView('book_tags')
    #spark_df_books.createOrReplaceTempView('books')
    #spark_df_ratings.createOrReplaceTempView('ratings')
    spark_df_tags.createOrReplaceTempView('tags')
    #spark_df_toRead.createOrReplaceTempView('to_read')

    def topNTags4Books2MongoDB():
        from pyspark.sql.functions import rank, col
        maxTagsForBook = 10
        window = Window.partitionBy(spark_df_book_tags['goodreads_book_id']).orderBy(spark_df_book_tags['count'].desc())
        alfa = spark_df_book_tags.select('*', rank().over(window).alias('rank')).filter(col('rank') <= maxTagsForBook).orderBy('goodreads_book_id')\
            .select("goodreads_book_id",col('tag_id').alias('tag_id_alfa'))
        gamma = alfa.join(spark_df_tags,alfa.tag_id_alfa == spark_df_tags.tag_id).select('goodreads_book_id','tag_id','tag_name')
        zeta = gamma.toPandas()
        col = db.Books_Tags
        col.insert_many(zeta.to_dict('records'))


    topNTags4Books2MongoDB()
    spark.stop()