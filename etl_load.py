from concurrent import futures
import requests
import psycopg2

url = 'https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json'

data = requests.get(url)
res = data.json()
print(type(res))
data_list = []

def api_data(page):
    return requests.get(url, params={'page': page}).json()

with futures.ThreadPoolExecutor(max_workers=25) as executor:
    future_src = {executor.submit(api_data, page):page for page in range(1, res[0]['pages'] + 1)}
    for src in futures.as_completed(future_src):
        try:
            data = src.result()
            #print(data)
            data_list.append(data[1])   
        except Exception as ex:
            print(ex)
        #if page == 1:      \
        #for i in data[1]:
        #    tuples.append((i['indicator']['id'], i['indicator']['value'], i['country']['id'], i['country']['value'], i['countryiso3code'], i['date'], i['value'], i['unit'], i['obs_status'], i['decimal']))
        #break
print(len(data_list))
#for i in data_list:
#    print(i)

tuples = ({'indicator_id':i['indicator']['id'], 
                   'indicator_value':i['indicator']['value'], 
                   'country_id':i['country']['id'], 
                   'country_value':i['country']['value'], 
                   'countryiso3code':i['countryiso3code'], 
                   'date':i['date'], 
                   'value':i['value'], 
                   'unit':i['unit'], 
                   'obs_status':i['obs_status'], 
                   'decimal':i['decimal']} for data in data_list for i in data)
#print(next(tuples))
params_dic = {
    "host"      : "localhost",
    "database"  : "test_db",
    "user"      : "root",
    "password"  : "root"
}
conn = psycopg2.connect(**params_dic)

tbl = 'market_indicator'
#columns = ['indicator_id', 'indicator_value', 'country_id', 'country_value', 'countryiso3code', 'date', 'value', 'unit', 'obs_status', 'decimal']
#cols = ','.join(list(columns))

query  = """INSERT INTO market_indicator VALUES(
            %(indicator_id)s, 
            %(indicator_value)s, 
            %(country_id)s, 
            %(country_value)s, 
            %(countryiso3code)s, 
            %(date)s, 
            %(value)s, 
            %(unit)s, 
            %(obs_status)s, 
            %(decimal)s
            );
        """

print(query)
cursor = conn.cursor()
try:
    cursor.executemany(query, tuples)
    conn.commit()
    cursor.close()
except (Exception, psycopg2.DatabaseError) as error:
    print("Error: %s" % error)
    conn.rollback()
    cursor.close()
