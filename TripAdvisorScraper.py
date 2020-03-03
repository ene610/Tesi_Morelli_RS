from googleplaces import GooglePlaces, types, lang
import json
import time
API_KEY = 'AIzaSyA1lROcHZF-wSzEyaaaQzVGXAq9R6AmHHM'

import googlemaps
gmaps = googlemaps.Client(key=API_KEY)
search_location = gmaps.places_nearby(location=('41.902782, 12.496366'),radius = 100,type = "art_gallery")
json_string = json.dumps(search_location)
print (json_string)
while(search_location.get("next_page_token")):
    time.sleep(3)
    search_location = gmaps.places_nearby(location=('41.902782, 12.496366'),radius = 50000,type="art_gallery",page_token = search_location["next_page_token"])
    print(search_location.get("next_page_token"))

#CsQDuwEAABgqJEYEX5feM21nwVqtd0g1R6lIcmeXBgWwHNa6il7fWk4bMzHQkPUclzyRrFgH3wB2dsQuE2k22-jjRdsXIQlqNB60C_EPnII-Ot-RqL8wzWkQWVWCa9ONWBq8XxVhBPNceQMufOs9ST_i2P3-4UZRAjkrOq0tDJeKafUOreW3DX5A4GU2b80pOkm4fZzKDtvcKOHGo1eaaMnNP49GhuuR8n5h1obJSOTIO97M7Izio0UwL9W2YuVxcWKM-aQQXyMqHh2DV3baSyHHJ_Cmk659y0-Kie-xgmnwsQM-4-bXdZLFmJoBNcvymSvrPy1PrPmoClau0TV88LXLbMeaqSDOOh3VYVx-lgLds9vE4Catejw-3ZM7yxYjJRAO8phkA3k-rlEGi4Z1DIeUv7tEmCYF3Kx5RhHZhyTiHl3a-dUWyD8RAO7vuPx9WEImoX2F4fbG46nBbh3W5O33txsa4fd9Nivd08zuXhvO6SSZWQ5GBmQFZEsAjOTCxlmQJOhmp5K6jy4lfRUiMsn6Vc1Y0utQhb8dYLfckoGYPwge-1-wdZEs96pjbYyFFtp4mYfUuI0hah7d8t_vYxiD6dyI7DgSEPryB0BujBRPUhup-Pb0DnsaFAsp9nSPqCm2-u4EWs1TucZiqEwL