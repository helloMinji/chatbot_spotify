import sys
import os
import logging
import boto3
import requests
import base64
import json
import pymysql
from datetime import datetime # datetime 추가: 파티션에서 key로 쓰려고
import pandas as pd           # parquet 사용 위해
import jsonpath               # 설치: pip3 install jsonpath --user

client_id = ""
client_secret = ""

host = ""
port = 
username = ""
database = ""
password = ""



def main():

    	# DB Connect
        try:
            conn = pymysql.connect(host=host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
            cursor = conn.cursor()
        except:
            logging.error("could not connect to RDS")
            sys.exit(1)



        headers = get_headers(client_id, client_secret)

        # 1. RDS에서 artist id 가져오기
        cursor.execute("SELECT id FROM artists limit 1")

        # 2. Top tracks
        top_track_keys = {
            "id": "id",
            "name": "name",
            "popularity": "popularity",
            "external_url": "external_urls.spotify"
        }

        top_tracks = []

        for (id, ) in cursor.fetchall():
            # 2.1 api로 데이터 불러오기
            URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(id)
            params = {
                'country': 'US'
            }
            r = requests.get(URL, params=params, headers=headers)
            raw = json.loads(r.text)

            # 2.2 데이터 업데이트
            for i in raw['tracks']:
                top_track = {}
                for k,v in top_track_keys.items():
                    top_track.update({k: jsonpath.jsonpath(i,v)})
                    top_track.update({'artist_id': id})
                    top_tracks.append(top_track)

        track_ids = [i['id'][0] for i in top_tracks]

        # 2.3 DataFrame -> Parquet
        top_tracks = pd.DataFrame(top_tracks)
        top_tracks.to_parquet('top-tracks.parquet', engine='pyarrow', compression="snappy")

        # 2.4 s3에 저장
        dt = datetime.utcnow().strftime("%Y-%m-%d")

        s3 = boto3.resource('s3')
        object = s3.Object('spotify-artists', 'top-tracks/dt={}/top-tracks.parquet'.format(dt))
        data = open('top-tracks.parquet','rb')
        object.put(Body=data)

        
        # 3. Audio features
        # 3.1 배치 만들기
        tracks_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)] # audio features는 100개까지만 가능

        audio_features = []
        for i in tracks_batch:

            # 3.2 batch 활용하여 데이터 불러오기
            ids = ','.join(i)
            URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)
            r = requests.get(URL, headers=headers)
            raw = json.loads(r.text)
            # audio feature에 대한 데이터 불러오기 완료

            # 3.3 dictionary 형태니까 바로 extend
            audio_features.extend(raw['audio_features'])

        # 3,4 DataFrame -> Parquet
        audio_features = pd.DataFrame(audio_features)
        audio_features.to_parquet('audio-features.parquet', engine='pyarrow', compression='snappy')

        # 3.5 s3에 저장
        s3 = boto3.resource('s3')
        object = s3.Object('spotify-artists', 'audio-features/dt={}/audio-features.parquet'.format(dt))
        data = open('audio-features.parquet','rb')
        object.put(Body=data)
        
        ### s3에 데이터 잘 들어갔는지 AWS 확인



def get_headers(client_id, client_secret):
        endpoint = "https://accounts.spotify.com/api/token"
        encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

        headers = {
            "Authorization": "Basic {}".format(encoded)
        }

        payload = {
            "grant_type": "client_credentials"
        }

        r = requests.post(endpoint, data=payload, headers=headers)

        access_token = json.loads(r.text)['access_token']

        headers = {
            "Authorization": "Bearer {}".format(access_token)
        }

        return headers





if __name__ == '__main__':
    main()
