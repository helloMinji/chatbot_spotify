import sys
import os
import boto3	# 파이썬에서 dynamodb를 쓰기 위한 sdk(패키지)(aws 제공)

# spotify api 사용 위한 import 추가
import requests
import base64
import json
import logging
import pymysql

# spotify api 사용 위한 정보 추가
client_id = ""
client_secret = ""


host = ""
port = 
username = ""
database = ""
password = ""



def main():
	
	# dynamodb Connect
	try:
		dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2',
			endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
	except:
		logging.error('could not connect to dynamodb')
		sys.exit(1)
		
	print('Success')
	
	# mysql Connect
	try:
	    conn = pymysql.connect(host=host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
	    cursor = conn.cursor()
	except:
	    logging.error("could not connect to RDS")
	    sys.exit(1)

	headers = get_headers(client_id, client_secret)

	# Table define: dynamodb에서 사용할 table 지정
	table = dynamodb.Table('top_tracks')

	# 데이터 하나만 가져오기
	cursor.execute('SELECT id FROM artists LIMIT 1')

	for (artist_id, ) in cursor.fetchall():

		# 테이블에서 가져온 artist_id로 api에서 top_tracks 데이터 가져오기
		URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id) # endpoint
		params = {
			'country': 'US'
		}

		r = requests.get(URL, params=params, headers=headers)

		raw = json.loads(r.text)

		for track in raw['tracks']:

			# 가져온 데이터에는 우리의 key값이 없기 때문에 key를 추가
			data = {
				'artist_id': artist_id
			}

			# 가져온 데이터를 data에 업데이트해서 key + track를 만듦
			data.update(track)

			# Single item INSERT: track을 id값으로 나눠서 저장하겠다
			# 테이블에 데이터가 들어간 것을 확인 가능!
			table.put_item(
				Item=data
			)

			# batch 형식으로 item을 불러올 수도 있다.
			# with table.batch_writer() as batch:
			# 	batch.put_item(
			#		Item=data
			#	)	
	
	
	
	
	
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





if __name__=='__main__':
	main()
