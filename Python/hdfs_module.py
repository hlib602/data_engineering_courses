import os, json, requests, logging
from settings import client, http_conn

def loadTablesToBronze(pg_hook, table, date):
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    with client.write(os.path.join('/', 'dshop_datalake', 'bronze', table, table + '_' + date + '.csv')) as csv_file:
      cursor.copy_expert('COPY ' + table +' TO STDOUT WITH HEADER CSV', csv_file)
    logging.info(f"The table {table} for the date {date} loaded to bronze successfully")

def loadAPIDataToBronze(date):
    url = http_conn.host + "/auth"
    headers = {"content-type": "application/json"}
    data = {"username": http_conn.login,
            "password": http_conn.password}

    r = requests.post(url, headers=headers, data=json.dumps(data))
    token = r.json()['access_token']

    url = http_conn.host + "/out_of_stock"
    headers = {"content-type": "application/json", "Authorization": "JWT " + token}
    data = {"date": date}
    r = requests.get(url, headers=headers, data=json.dumps(data))
    json_data = r.json()
    if r.status_code == 200:
        client.write(os.path.join('/', 'api_datalake', 'bronze', date + '.json'), data=json.dumps(json_data), encoding='utf-8')
        logging.info(f"The data from API for the date {date} loaded to bronze successfully")
    else:
        logging.info("There are no data ")

