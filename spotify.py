import sys
import requests
import base64
import json
import logging
import pymysql
import csv



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
    ### print(headers)
    ### sys.exit()



		# Insert할 데이터 가져오기 - 파일 (실행X)
#        artists = []
#        with open('artist_list.csv', encoding='utf-8') as f:
#            raw = csv.reader(f)
#            for row in raw:
#                artists.append(row[0])
#                ### print(artists)
#        ### sys.exit()
#                for a in artists:
#
#			# Spotify API - Search
#            params = {
#                "q": a,
#                "type": "artist",
#                "limit": "1"
#            }
#                    r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)
#            ### print(r.text)
#            ### sys.exit()
#            raw = json.loads(r.text)
#       
#            artist = {}
#            try:
#                    artist_raw = raw['artists']['items'][0]
#                    
#                    if artist_raw['name'] == params['q']:
#                        artist.update(
#                            {
#                                  'id': artist_raw['id'],
#                                  'name': artist_raw['name'],
#                                  'followers': artist_raw['followers']['total'],
#                                  'popularity': artist_raw['popularity'],
#                                  'url': artist_raw['external_urls']['spotify'],
#                                  'image_url': artist_raw['images'][0]['url']
#                            }
#                        )
#                        insert_row(cursor, artist, 'artists')
#
#            except:
#                logging.error('NO ITEMS FROM SEARCH API')
#                continue
#       
#        conn.commit()



	# Insert할 데이터 가져오기 - batch
    cursor.execute("SELECT id FROM artists")
    artists = []
    for (id, ) in cursor.fetchall():
        artists.append(id)

    # artist를 50개씩 끊고 싶다
    artist_batch = [artists[i: i+50] for i in range(0,len(artists), 50)]
    ### print(artist_batch)

    artist_genres = []

    for i in artist_batch:
        ids = ','.join(i)
        URL = "https://api.spotify.com/v1/artists/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)
        ### print(raw)
        ### print(len(raw['artists']))
        ### sys.exit()

        for artist in raw['artists']:
            for genre in artist['genres']:
                artist_genres.append(
                    {
                        'artist_id': artist['id'],
                        'genre': genre
                    }
                )
    for data in artist_genres:
        insert_row(cursor, data, 'artist_genres')

    conn.commit()
    cursor.close()
    ### sys.exit()



	# Error Handling
	# 기본 방법
    try:
        r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)
    except:
        logging.error(r.text)
        sys.exit(1)

	# status code에 따른 처리 방법
    if r.status_code != 200:
        logging.error(r.text)

        if r.status_code == 429:
            retry_after = json.loads(r.headers)['Retry-After']
            time.sleep(int(retry_after))

            r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

        elif r.status_code == 401:
            headers = get_headers(client_id, client_secret)
            r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

        else:
            sys.exit(1)



    # Get an Artist's Album
    r = requests.get("https://api.spotify.com/v1/artists/3Nrfpe0tUJi4K4DXYWgMUX/albums", headers=headers)
	raw = json.loads(r.text)

    total = raw['total']
    offset = raw['offset']
    limit = raw['limit']
    next = raw['next']

    albums = []
    albums.extend(raw['items'])

    ## 앨범 100개 추출
    while count < 100 or not next:

        r = requests.get(raw['next'], headers=headers)
        raw = json.loads(r.text)
        next = raw['next']
        print(next)

        albums.extend(raw['items'])
        count = len(albums)

    print(len(albums))





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


def insert_row(cursor, data, table):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values())*2)





if __name__ == '__main__':
    main()
