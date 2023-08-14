from concurrent import futures
import requests
import psycopg2


url = 'https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json'

def get_api_data(page):
    return requests.get(url, params={'page': page}).json()

def load_api():
    print('Start getting API data')
    data = requests.get(url)
    res = data.json()
    data_list = []
    with futures.ThreadPoolExecutor(max_workers=25) as executor:
        future_src = {executor.submit(get_api_data, page):page for page in range(1, res[0]['pages'] + 1)}
        for src in futures.as_completed(future_src):
            try:
                data = src.result()
                data_list.append(data[1])   
            except Exception as ex:
                print(ex)
    print(f'Scanned through a total of {len(data_list)} pages')
    print('End getting API data')
    return data_list


def load_db(data_list):
    print('Start loading into db')
    tbl = 'market_indicator'
    tuples = (
                {
                    'indicator_id':i['indicator']['id'], 
                    'indicator_value':i['indicator']['value'], 
                    'country_id':i['country']['id'], 
                    'country_value':i['country']['value'], 
                    'countryiso3code':i['countryiso3code'], 
                    'date':i['date'], 
                    'value':i['value'], 
                    'unit':i['unit'], 
                    'obs_status':i['obs_status'], 
                    'decimal':i['decimal']
                } 
                for data in data_list for i in data
            )

    params_dic = {
        "host"      : "localhost",
        "database"  : "test_db",
        "user"      : "root",
        "password"  : "root"
    }    

    query  = f"""INSERT INTO {tbl} VALUES(
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
    conn = psycopg2.connect(**params_dic)    
    cursor = conn.cursor()
    try:
        create_db_table(conn, tbl)
        cursor.executemany(query, tuples)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
    print('End loading into db')

def create_db_table(conn, tbl):
    sql = f'''
        create table if not exists test_db.public.{tbl}
        (
            indicator_id varchar(25), 
            indicator_value varchar(100), 
            country_id char(3), 
            country_value varchar(100), 
            countryiso3code char(3), 
            date char(4), 
            value double precision, 
            unit varchar(25), 
            obs_status varchar(25), 
            decimal integer
        )
    '''
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()


def main():
    res = load_api()
    load_db(res)

main()
