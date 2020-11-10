import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# These are global variables used for converting log data into a dataframe
time_table = pd.DataFrame()
user_table = pd.DataFrame()
songplay_table = pd.DataFrame()
song_table = pd.DataFrame()
artist_table = pd.DataFrame()
merged_table = pd.DataFrame()


def process_song_file(cur, conn, filepath):
    '''
        Extract the data from song and artist files,
        and write them to data tables
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    for index, row in df.iterrows():
        # Add the data the song table
        song_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy()
        global song_table
        song_table = pd.concat([song_table, song_df], ignore_index=True)
        
        # Add the data for the artist table
        artist_df = df[['artist_id', 'artist_name', 'artist_location', 
                        'artist_latitude', 'artist_longitude']].copy()
        artist_df.columns = ['artist_id', 'name', 'location', 'latitude', 'longitude']
        global artist_table
        artist_table = pd.concat([artist_table, artist_df], ignore_index=True)
    
    
def process_log_file(cur, conn, filepath):
    '''
        Extract data from log files,
        and write them to data tables
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

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
    
    # Add the new data to the data table
    global time_table
    time_table = pd.concat([time_table, time_df], ignore_index=True)
    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    user_df.columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    
    # Add the new data to the data table
    global user_table
    user_table = pd.concat([user_table, user_df], ignore_index=True)
    
    # build the songplay table dataframe
    songplay_records = []
    for index, row in df.iterrows():       
        result = merged_table.loc[(merged_table['title'] == row.song)
                                   & (merged_table['name'] == row.artist)
                                   & (merged_table['duration'] == row.length)]
        if len(result) > 0:
            songid = result['song_id'].values[0]
            artistid = result['artist_id'].values[0]
        else:
            songid, artistid = None, None

        # create a unique id for each record
        songplay_id = str(row['ts']) + str(row.userId) + str(row.sessionId)
        # build up the songplay records
        songplay_data = [songplay_id, t[index], row.userId, row.level, songid, artistid, 
                         row.sessionId, row.location, row.userAgent]
        songplay_records.append(songplay_data)
    
    # Convert the array of records into a dataframe
    column_names = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id',
                    'session_id', 'location', 'user_agent']
    songplay_records_df = pd.DataFrame(songplay_records, columns=column_names)
    
    # Add the new data to the data table
    global songplay_table
    songplay_table = pd.concat([songplay_table, songplay_records_df], ignore_index=True)

    
def get_files(filepath):
    '''
        Extract all the files under a given directory
        and return as a list.
    '''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    
    return all_files


def drop_duplicate_records():
    '''
        Drop duplicate records from the data tables.
    '''
    
    tables = [song_table,
              artist_table,
              user_table,
              time_table,
              songplay_table]
    
    keys = ['song_id',
            'artist_id',
            'user_id',
            'timestamp',
            'songplay_id']
    
    for key, table in zip(keys, tables):
        table.drop_duplicates(subset=[key], inplace=True)
        

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = get_files(filepath)
    
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # merge the artist and song tables so we can query them
    if len(song_table) > 0 and len(artist_table) > 0:
        global merged_table
        merged_table = song_table.merge(artist_table, on='artist_id')
    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, conn, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    
    drop_duplicate_records()


def create_csv():
    '''
        Create CSV files from the dataframes
    '''
    
    tables = [song_table,
              artist_table,
              user_table,
              time_table,
              songplay_table]
    
    filenames = ['songs.csv',
                 'artists.csv',
                 'users.csv',
                 'time.csv',
                 'songplays.csv']
    
    for table, filename in zip(tables, filenames):
        table.to_csv(filename, index=False, header=False, sep='\t')

        
def copy_csv_to_table(cur, conn):
    '''
        Copy data from the CSV files to the database
    '''
    
    tablenames = ['songs',
                 'artists',
                 'users',
                 'time',
                 'songplays']
    
    filenames = ['songs.csv',
                 'artists.csv',
                 'users.csv',
                 'time.csv',
                 'songplays.csv']
    
    for tablename, filename in zip(tablenames, filenames):
        with open(filename, 'r') as f:
            try:
                cur.copy_from(f, tablename, sep="\t")
                conn.commit()
                print(f'Data from {filename} copied to table')
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conn.rollback()
                cur.close()
                return 1
            
    print("copy_to_table() done")    
    cur.close()
        
            
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    create_csv()
    copy_csv_to_table(cur, conn)
    
    conn.close()

    
if __name__ == "__main__":
    main()


    
    