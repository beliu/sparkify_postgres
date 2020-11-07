import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

time_table = pd.DataFrame()
user_table = pd.DataFrame()
songplay_table = pd.DataFrame()
song_table = pd.DataFrame()
artist_table = pd.DataFrame()
time_records_list = []
user_records_list = []
songplay_records_list = []
song_records_list = []
artist_records_list = []


def add_table_rows(data_table, data_records_list, dataframe, key):

    data_table = pd.concat([data_table, dataframe], ignore_index=True)
    data_records_list += dataframe[key].values.tolist()
    
    return data_table


def process_song_file(cur, conn, filepath):
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    for index, row in df.iterrows():
        # Add the data the song table
        song_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy()
        global song_table
        song_table = add_table_rows(song_table, song_records_list, song_df, 'song_id')
        
        # Add the data for the artist table
        artist_df = df[['artist_id', 'artist_name', 'artist_location', 
                        'artist_latitude', 'artist_longitude']].copy()
        global artist_table
        artist_table = add_table_rows(artist_table, artist_records_list, artist_df, 'artist_id')
    
    
def process_log_file(cur, conn, filepath):
    
    # open log file
    df = pd.read_json(filepath, lines=True)
    print(df.columns)
    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # Create a dictionary to turn into the time dataframe
    time_data_dict = {column_name: data_series for data_series, column_name in zip(time_data, column_labels)}
    time_df = pd.DataFrame(time_data_dict)
    global time_table
    time_table = add_table_rows(time_table, time_records_list, time_df, 'timestamp')
    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    global user_table
    user_table = add_table_rows(user_table, user_records_list, user_df, 'userId')
    
    # build the songplay table dataframe
    songplay_records = []
    for index, row in df.iterrows():
        # Convert ts to timestamp
        start_time = pd.to_datetime(row['ts'], unit='ms')
    
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        if results is not None:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # create a unique id for each record
        songplay_id = str(row['ts']) + str(row.userId) + str(row.sessionId)
        # build up the songplay records
        songplay_data = [songplay_id, start_time, row.userId, row.level, songid, artistid, 
                         row.sessionId, row.location, row.userAgent]
        songplay_records.append(songplay_data)
    
    # Convert the array of records into a dataframe
    column_names = ['songplayId', 'startTime', 'userId', 'level', 'songId', 'artistId',
                    'sessionId', 'location', 'userAgent']
    songplay_records_df = pd.DataFrame(songplay_records, columns=column_names)
    global songplay_table
    songplay_table = add_table_rows(songplay_table, songplay_records_list, songplay_records_df, 'songplayId')

    
def get_files(filepath):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files


def drop_duplicate_records():
    
#     global time_table
    time_table.drop_duplicates(subset=['timestamp'], inplace=True)
    user_table.drop_duplicates(subset=['userId'], inplace=True)
    songplay_table.drop_duplicates(subset=['songplayId'], inplace=True)
    song_table.drop_duplicates(subset=['song_id'], inplace=True)
    artist_table.drop_duplicates(subset=['artist_id'], inplace=True)
    

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = get_files(filepath)
    
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # iterate over files and process
    time_rows = []
    for i, datafile in enumerate(all_files[:1], 1):
        func(cur, conn, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    
    drop_duplicate_records()
   
    



def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()

    
if __name__ == "__main__":
    main()


    
    