import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
        Description: This function extracts data from S3 and loads it to spark dataframes where it then transforms the data
        creating the songs and artists tables, which it then loads back to S3.

        Input:
            spark      : spark session
            input_data : the location from where the S3 data is loaded from.
            output_data: the location where the processed data is then stored.
        '''
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              col('artist_name').alias('name'),
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')
                             ).distinct()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    '''
    Description: This function extracts data from S3 and loads it to spark dataframes where it then transforms the data
    creating the songs and artists tables, which it then loads back to S3.

    Input:
        spark      : spark session
        input_data : the location from where the S3 data is loaded from.
        output_data: the location where the processed data is then stored.
    '''
    log_data = input_data + 'log_data/*/*'


    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender',
                            'level'
                           ).distinct()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'user.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())

    df = df.withColumn('start_time', get_timestamp('ts'))

    year, month, dayofmonth, hour, weekofyear, date_format

    # extract columns to create time table
    time_table = df.select('start_time').distinct() \
    .withColumn('hour', hour(col('start_time')))\
    .withColumn('day', dayofmonth(col('start_time')))\
    .withColumn('week', weekofyear(col('start_time')))\
    .withColumn('month', month(col('start_time')))\
    .withColumn('year', year(col('start_time')))





    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time_table.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs.parquet')


    # extract columns from joined song and log datasets to create songplays table

    songplays_table = df.join(song_df, df.song == song_df.title, how = 'left')\
    .withColumn('songplay_id', F.monotonically_increasing_id())\
    .select('songplay_id',
            'start_time',
            col('userId').alias('user_id'),
            'level',
            'song_id',
            'artist_id',
            col('sessionId').alias('session_id'),
            'location',
            col('userAgent').alias('user_agent')
           ).withColumn('month', month(col('start_time')))\
    .withColumn('year', year(col('start_time')))\


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplay.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend
    #make sure to add the output S3 bucket location here.
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
