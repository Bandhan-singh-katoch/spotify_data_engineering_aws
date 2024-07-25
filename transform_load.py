import json
import boto3
from io import StringIO
from datetime import datetime
import pandas as pd

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify'] 
        album_element =  {'album_id':album_id,'name':album_name,'release_date':album_release_date,'total_tracks':album_total_tracks,'url':album_url}
        album_list.append(album_element)
    
    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
        if row['track'] and row['track']['artists']:
            for artist in row['track']['artists']:
                artist_element = {'artist_id': artist['id'],'artist_name':artist['name'], 'external_url': artist['href']}
                artist_list.append(artist_element)
    return artist_list

def song(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)
    return song_list

def lambda_handler(event, context):
    Bucket='spotify-etl-bandhan'
    Key='raw_data/to_processed/'
    
    s3 = boto3.client('s3')
    res = s3.list_objects(Bucket=Bucket, Prefix=Key)
    # print(res)
    spotify_data = []
    spotify_keys = []
    for content in res['Contents']:
        # print(content['Key'].split('.'))
        key = content['Key']
        if key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=key)
            content = response['Body']
            json_data = json.loads(content.read())
            spotify_data.append(json_data)
            spotify_keys.append(key)
            
    # print(spotify_data)
    for data in spotify_data:
        artist_list = artist(data)
        album_list = album(data)
        song_list = song(data)
        # print('album_list====================>',album_list)
        
        song_list_df = pd.DataFrame.from_dict(song_list)
        song_list_df['song_added1'] = pd.to_datetime(song_list_df['song_added'], errors='coerce', format='%Y-%m-%d')
        song_list_df['song_added'] = song_list_df['song_added1'].fillna(pd.to_datetime(song_list_df['song_added'], errors='coerce', format='%Y'))
        song_list_df = song_list_df.drop(columns=['song_added1'])
        
        album_list_df = pd.DataFrame.from_dict(album_list)
        album_list_df = album_list_df.drop_duplicates(subset=['album_id'])
        album_list_df['release_date1'] = pd.to_datetime(album_list_df['release_date'], errors='coerce', format='%Y-%m-%d')
        album_list_df['release_date'] = album_list_df['release_date1'].fillna(pd.to_datetime(album_list_df['release_date'], errors='coerce', format='%Y'))
        album_list_df = album_list_df.drop(columns=['release_date1'])
        # print(album_list_df['release_date'])

        artist_list_df = pd.DataFrame.from_dict(artist_list)
        artist_list_df = artist_list_df.drop_duplicates(subset=['artist_id'])
        
        storeProcessedData('songs_data', song_list_df, Bucket)
        storeProcessedData('album_data', album_list_df, Bucket)
        storeProcessedData('artist_data', artist_list_df, Bucket)
    
    print(spotify_keys)
    # move and delete files
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/'+key.split('/')[-1] )
        s3_resource.Object(Bucket,key).delete()
        
def storeProcessedData(path, dataframe, Bucket):
    s3 = boto3.client('s3')
    key = f'transformed_data/{path}/transformed_file_'+ str(datetime.now())+ '.csv'
    buffer = StringIO()
    dataframe.to_csv(buffer, index=False) 
    content = buffer.getvalue()
    s3.put_object(Bucket=Bucket, Key=key, Body=content)
