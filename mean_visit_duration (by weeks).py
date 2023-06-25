# Расчет среднего времени, проведенного на сайте посетителем, по заданным когортам.


# In[0]
# import libraries; set settings


import sys
import pandas as pd
from tapi_yandex_metrika import YandexMetrikaLogsapi
import datetime as dt
from datetime import datetime, timedelta

import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_columns', 500)


# In[1]
# define functions


def get_cabinet_logs(date_from, date_to):
    client = YandexMetrikaLogsapi(
        access_token = ACCESS_TOKEN,
        default_url_params = {'counterId': COUNTER_ID},
        wait_report = True
    )

    params = {
        "fields": "ym:s:date,ym:s:clientID,ym:s:visitDuration",
        "source": "visits",
        "date1": date_from,
        "date2": date_to
    }

    info = client.create().post(params = params)
    request_id = info["log_request"]["request_id"]

    first_dt = dt.datetime.now()
    report = client.download(requestId = request_id).get()
    second_dt = dt.datetime.now()

    # print('Download took:', second_dt - first_dt)

    df = pd.DataFrame(report().to_values(), columns = [col.split(':')[-1] for col in report.columns])

    return df


def str_to_date(date_str):
    date = datetime.strptime(date_str, '%Y-%m-%d').date()
    return datetime(date.year, date.month, date.day)


def date_to_str(date):
    return str(date)[:10]


def pretty_date(s):
    """YYYY-MM-DD to DD.MM.YYYY"""

    return '.'.join(s.split('-')[::-1])


# In[2]
# set constants


ACCESS_TOKEN = ""
COUNTER_ID = ""
START_DATE = "2023-05-01" # YYYY-MM-DD
FINISH_DATE = "2023-06-24" # YYYY-MM-DD
INTERVAL = 7
FILE_NAME = "by_weeks"

START_DATE, FINISH_DATE = str_to_date(START_DATE), str_to_date(FINISH_DATE)

# make interval full
FINISH_DATE -= timedelta((int(str((FINISH_DATE - START_DATE)).split()[0]) + 1) % INTERVAL)


# In[3]
# download data


try:
    data = get_cabinet_logs(date_to_str(START_DATE), date_to_str(FINISH_DATE))
except:
    print('Access denied')
    sys.exit()


# In[4]
# get all visit dates for IDs


id_visit_dates = {}

for el in set(data['clientID']):
    id_visit_dates[el] = set(map(lambda x: str_to_date(x), list(data[data['clientID'] == el]['date'])))


# In[5]
# fill df with mean durations


k_cols = int(str((FINISH_DATE - START_DATE) // INTERVAL).split()[0]) + 1
df = pd.DataFrame({'Date' : [''] * k_cols})

for i in range(k_cols):
    df['Week ' + str(i)] = [None] * k_cols


start = START_DATE
date1 = start
date2 = date1 + timedelta(INTERVAL - 1)
row_i = k_cols - 1

while date2 <= FINISH_DATE:
    df.at[row_i, 'Date'] = str(pretty_date(date_to_str(date1)))

    s1 = 0
    all_ids_valid_visit_date = set()

    for id in id_visit_dates.keys():
        for visit_date in id_visit_dates[id]:
            if date1 <= visit_date <= date1 + timedelta(6):
                all_ids_valid_visit_date.add(id)
                s1 += sum(list(map(lambda x: int(x), data[(data['clientID'] == id) & (data['date'] == date_to_str(visit_date))]['visitDuration'])))

    try:
        df.at[row_i, 'Week 0'] = round(s1 / len(all_ids_valid_visit_date), 2)
    except:
        df.at[row_i, 'Week 0'] = 0

    interval_date1 = date2 + timedelta(1)
    interval_date2 = interval_date1 + timedelta(6)

    for i in range(row_i, 0, -1):
        s2 = 0
        interval_ids_valid_visit_date = set()
        active_users = set(data[data['clientID'].isin(all_ids_valid_visit_date)]['clientID'])

        for id in list(active_users):
            for visit_date in id_visit_dates[id]:
                if interval_date1 <= visit_date <= interval_date2:
                    interval_ids_valid_visit_date.add(id)
                    s2 += sum(list(map(lambda x: int(x), data[(data['clientID'] == id) & (data['date'] == date_to_str(visit_date))]['visitDuration'])))

        try:
            df.at[row_i, 'Week ' + str(row_i - i + 1)] = round(s2 / len(all_ids_valid_visit_date), 2)
        except:
            df.at[row_i, 'Week ' + str(row_i - i + 1)] = 0

        interval_date1 = interval_date2 + timedelta(1)
        interval_date2 = interval_date1 + timedelta(INTERVAL - 1)

    date1 = date2 + timedelta(1)
    date2 = date1 + timedelta(INTERVAL - 1)
    start = date1
    row_i -= 1


# In[6]
# upload df


df.to_csv(FILE_NAME + '.csv', index = False)
print('Result dowloaded into ' + FILE_NAME + '.csv')


# %%
