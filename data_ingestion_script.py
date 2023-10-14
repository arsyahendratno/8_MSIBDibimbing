import requests # Untuk melakukan HTTP Request
from bs4 import BeautifulSoup # Untuk pengolahan data HTML atau XML
import pandas as pd #manipulasi data

url = 'https://www.petsecure.com.au/pet-care/a-guide-to-worldwide-pet-ownership/'

response = requests.get(url)

soup = BeautifulSoup(response.content, 'html.parser')

table = soup.find('table',{'class':'cats'})

headers = []

for th in table.find_all('th'):
  headers.append(th.text.strip())

rows = []

for tr in table.find_all('tr'): #loop komponen tr
  row_data = []
  for td in tr.find_all('td'):
    row_data.append(td.text.strip())

  if len(row_data) > 0:
    rows.append(row_data)

population_df = pd.DataFrame(rows,columns=headers)

population_df = population_df.rename(columns={population_df.columns[0]:'country',population_df.columns[1]:'populations'})



url = 'https://api.opencagedata.com/geocode/v1/json'

api_key = 'a34768ccd55d49cfa29fb5753e2d1486'

countries = population_df['country'].to_list()

countries_list = []
for country in countries:
  params = {'q': country, 'key': api_key}
  response = requests.get(url,params=params)

  json_data = response.json()

  components = json_data['results'][0]['components']
  geometry = json_data['results'][0]['geometry']

  country_components = {
      'country': country,
      'country_code': components.get('country_code',''),
      'latitude': geometry.get('lat'),
      'longitude': geometry.get('lng')
  }

  countries_list.append(country_components)

component_df = pd.DataFrame(countries_list)


import psycopg2 # untuk connect ke database postgresql
# Connect to the database
host = '172.21.0.2'
port = '5432'
database = 'day15'
username = 'docker8'
password = 'kel8day15'

# Create database connection
connection = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)


#Make Table country
query = 'CREATE TABLE IF NOT EXISTS public.country(country text, country_code text, latitude float, longitude float)'
cursor = connection.cursor()
cursor.execute(query)
connection.commit()
cursor.close()

#Insert data country to database
for index,row in component_df.iterrows():
  query = f"INSERT INTO public.country(country, country_code, latitude, longitude) VALUES ('{row['country']}','{row['country_code']}',{row['latitude']},{row['longitude']})"
  cursor = connection.cursor()
  cursor.execute(query)
  connection.commit()
  cursor.close()


