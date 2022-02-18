# Data Lake with Spark
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in s3, in a directory of jason logs on user activity on the app, as well as a directory with json metadata on the songs in their app.

The purpose of this project is that as soon as the data kept being storaged the amount of files got bigger, in that case we need to move to a different architecture to don't lose all the value that the data can give us for the company.

# Index
* [Description](#Data-Lake-with-Spark)
* [Tables](#Tables)
* [Files](#Files)
* [How to run it](#How-to-run-it)

## [Tables](#Index)
The project tables are represented by a star schema where the fact table is **songplays** and the other tables are dimension tables.
The **star schema** is being use because the purpose of the whole database is to analyze the customers behavior inside the platform, in that way we use songplays table that tell us what songs are being listened, best artists, locations and users, that information is gold to keep improving the features of sparkify.

* songplays - records in log data associated with song plays i.e. records with page NextSong: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* users - users in the app: User_id, first_name, last_name, gender, level
* songs - songs in music database: song_id, title, artist_id, year, duration.
* artist - artist in music database: artist_id, name, location, latitude, longitude.
* time - timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, weekday.

## [Files](#Index)
* etl.py: Does the data process, from reading the data from s3, process it with spark and write it to s3.
* dl.cfg: Not here for privacy, but it contains the AWS credentials.
* README.md: A description of the project (this file).

## [How to run it](#Index)
1. Have on AWS an EMR instance and get your Access Key and Secret key, put them on a file, make sure you import those variable in etl.py
2. Run etl.py in console << python etl.py >>

