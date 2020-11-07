import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

class SparkifyDataFiles:
    '''
        This class is for keeping a global record of data
        from the log files, separated into the tables that
        will be copied into the postgresql database.
    '''
    def __init__(self):
        self.song_data = pd.DataFrame()
        self.artist_data = pd.DataFrame()
        self.time_data = pd.DataFrame()
        self.user_data = pd.DataFrame()
        self.songplay_data = pd.DataFrame()
        
        # Set dataframe names so they can be referenced
        self.song_data.index.set_names('song', inplace=True)
        self.artist_data.index.set_names('artist', inplace=True)
        self.time_data.index.set_names('time', inplace=True)
        self.user_data.index.set_names('user', inplace=True)
        self.songplay_data.index.set_names('songplay', inplace=True)
    
    
    def concat_df(self, data_table, new_records):
        '''
            Add new records to the global data frame.
        '''

        # Ensure that the new records can be appended
        # correctly to the data frame
        if len(data_table.columns) > 0:
            assert all(data_table.columns == new_records.columns), 'Data and New Records must have the same columns'

        return pd.concat([data_table, new_records], ignore_index=True)
    
    
    def drop_duplicates(self, data_table, key):
        '''
            Drop the duplicated values from the table based on the unique key.
        '''
        
        data_table_name = data_table.index.name
        
        
        
    
    def build_time_table(self, dataframe, columns):
        '''
            Create the time table based on the data provided.
        '''
        
        # convert timestamp column to datetime
        t = pd.to_datetime(dataframe['ts'], unit='ms')
        # insert time data records
        time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
        # Create a dictionary to turn into the time dataframe
        time_data_dict = {column_name: data_series for data_series, column_name in zip(time_data, columns)}
        new_records = pd.DataFrame(time_data_dict)
        self.time_data = self.concat_df(self.time_data, new_records)
        
        return new_records
    
    
    def build_songplay_table(self, dataframe, columns):
        '''
            Create the songplay table based on the data provided.
        '''
        new_records_list = []
        
        for index, row in dataframe.iterrows():
            # Convert ts to timestamp
            start_time = pd.to_datetime(row['ts'], unit='ms')

            # get songid and artistid from song and artist tables
            results = cur.execute(song_select, (row.song, row.artist, row.length))
            if results is not None:
                songid, artistid = results
            else:
                songid, artistid = None, None
            
            new_records_list.append([index, start_time, row.userId, row.level, songid, artistid, 
                                     row.sessionId, row.location, row.userAgent])
             
        # Create a dictionary to turn into the dataframe
        new_records = pd.DataFrame(new_records_list, columns=columns)
        self.songplay_data = self.concat_df(self.songplay_data, new_records)

        return new_records
        
        
    def build_data_table(self, data_table, dataframe, columns):
        '''
            Create the data table based on the data provided.
        '''
        data_table_name = data_table.index.name
        new_records = dataframe[columns].copy()
        
#         if data_table_name == 'song':
#             self.song_data = self.concat_df(self.song_data, new_records)
#         elif data_table_name == 'artist':
#             self.artist_data = self.concat_df(self.artist_data, new_records)
#         elif data_table_name == 'user':
#             self.user_data = self.concat_df(self.user_data, new_records)
        
        data_table = self.concat_df(data_table, new_records)
        
        return new_records
        
    
    def build_data_frame(self, data_table, columns, filepath, key, filter_df=None):
        '''
            For a specified database table, extract data from log files 
            and write them into a CSV file. Remove duplicate records using the unique key.
        '''

        # get all files matching extension from directory
        all_files = get_files(filepath)
        records_list = []
        data_table_name = data_table.index.name
        
        for i, datafile in enumerate(all_files[:3], 1):
            df = pd.read_json(datafile, lines=True)
            # Filter the database records if a filter is specified
            if filter_df is not None:
                assert isinstance(filter_df, tuple) or isinstance(filter_df, list), 'Filter must be a tuple or list'
                df = df.loc[df[filter_df[0]] == filter_df[1]]
            
            # Extract the data for the specified database table
            # Extracting the time data follows a different procedure than the rest
            if data_table_name == 'time':
                new_records = self.build_time_table(df, columns)
                records_list += new_records[key].values.tolist()    
            elif data_table_name == 'songplay':
                new_records = self.build_songplay_table(df, columns)
                records_list += new_records[key].values.tolist()  
            else:
                new_records = self.build_data_table(data_table, df, columns)
                if filepath == 'data/song_data':
                    records_list.append(df[key].values[0])
                elif filepath == 'data/log_data':
                    records_list += new_records[key].values.tolist()
        
        data_table.drop_duplicates(subset=[key], inplace=True)
        

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy()
    try:
        cur.execute(song_table_insert, song_data.values[0])
    except:
        print(song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 
                      'artist_longitude']].copy()
    try:
        cur.execute(artist_table_insert, artist_data.values[0])
    except:
        print(artist_data)

        
def process_log_file(cur, conn, filepath):
#     results_dict = {}
    
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
    time_df = time_df.drop_duplicates(subset=['timestamp'])
    
    # load user table
#     user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
#     user_df = user_df.drop_duplicates(subset=['userId'])
    
    

#     # insert songplay records
#     for index, row in df.iterrows():
        
#         # Convert ts to timestamp
#         start_time = pd.to_datetime(row['ts'], unit='ms')
    
#         # get songid and artistid from song and artist tables
#         results = cur.execute(song_select, (row.song, row.artist, row.length))
#         if results is not None:
#             songid, artistid = results
#         else:
#             songid, artistid = None, None

#             # insert songplay record
#             songplay_data = (index, start_time, row.userId, row.level, songid, artistid, 
#                              row.sessionId, row.location, row.userAgent)
#             cur.execute(songplay_table_insert, songplay_data)


def get_files(filepath):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = get_files(filepath)

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # iterate over files and process
    time_rows = []
    for i, datafile in enumerate(all_files, 1):
        func(cur, conn, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
#         time_rows += new_row
#         print(len(time_rows))
#         print(len(set(time_rows)))
    
#     print(time_rows)
#     print(f'Number of time rows: {len(set(time_rows))}')
        
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
#     process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()

    
if __name__ == "__main__":
#     main()
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    sdf = SparkifyDataFiles()
#     sdf.build_data_frame(sdf.songplay_data, ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 
#                                                'session_id', 'location', 'user_agent'], 'data/log_data', 'songplay_id', ('page', 'NextSong'))
#     print(sdf.songplay_data.head())


    sdf.build_data_frame(sdf.song_data, ['song_id', 'title', 'artist_id', 'year', 'duration'], 'data/song_data', 'song_id')
    print(len(sdf.song_data))
    conn.close()


    
    