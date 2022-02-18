import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.538,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

def create_spark_session():
    """starts the spark session
    @returns spark: the spark session object"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """reads the song data and executes the partitioning to the dataset then writes the result to s3
    @spark: the spark session object
    @input_data: the path to the s3 where the raw data is storaged
    @output_data: the s3 path where the processed data will be uploaded"""
    song_data = "{}*/*/*/*.json".format(input_data)
    df = spark.read.json(song_data)
    
    df_songs = df.select(col('song_id'), col('title'), col('artist_id'), col('year'), col('duration')).distinct()
    
    df_songs.write.partitionBy('year', 'artist_id').parquet('{}songs/songs_table.parquet'.format(output_data))
    
    df_artists = df.select(col('artist_id'), col('artist_name'), col('artist_location'),
                             col('artist_latitude'), col('artist_longitude')).distinct()
    
    df_artists.write.parquet(f"{output_data}artist/artist_table.parquet")
    
    df.createOrReplaceTempView('song_table')

def process_log_data(spark, input_data, output_data):
    """reads the log data and executes the partitioning to the dataset then writes the result to s3
    @spark: the spark session object
    @input_data: the path to the s3 where the raw data is storaged
    @output_data: the s3 path where the processed data will be uploaded"""
    log_data = "{}*events.json".format(input_data)

    df = spark.read.json(log_data).dropDuplicates()
    
    df = df.filter(df.page == "NextSong")

    df_users = df.select(col('firstName'), col('lastName'), col('gender'), col('level'), col('userId')).distinct()
    
    df_users.write.parquet(f"{output_data}users/users_table.parquet")

    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(col('ts')))
    
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn('start_time', get_timestamp(col('ts')))
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))
    
    df_time = df.select(col("start_time"), col("hour"), col("day"), col("week"), \
                           col("month"), col("year"), col("weekday")).distinct()
    df_time.write.partitionBy('year', 'month').parquet(f"{output_data}time/time_table.parquet")

    song_df = spark.sql('select distinct song_id, artist_id, artist_name from song_table')

    df_songplays = df.join(song_df, song_df.artist_name == df.artist, 'inner') \
        .distinct() \
        .select(col('start_time'), col('userId'), col('level'), col('sessionId'), \
               col('location'), col('userAgent'), col('song_id'), col('artist_id')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    df_songplays.write.partitionBy('year', 'month') \
        .parquet(f"{output_data}songplays/songplays_table.parquet")


def main():
    """orchestrates the whole etl proccess, starting by executing spark, sets up the paths of the raw datasets
    and executes the functions that transform and upload the new datasets"""
    spark = create_spark_session()
    input_songs_data = "s3a://songsaa/songs/songdata/"
    input_logs_data = "s3a://logsa/logs/log-data/"
    output_data = "s3a://aws-logs-134672443780-us-east-1/elasticmapreduce/j-1BXIFDDONFF90/"

    process_song_data(spark, input_songs_data, output_data)
    process_log_data(spark, input_logs_data, output_data)



if __name__ == "__main__":
    main()
