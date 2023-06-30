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
        "fields": "ym:s:date,ym:s:clientID,ym:s:visitDuration,ym:s:isNewUser",
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
FINISH_DATE = "2023-05-29" # YYYY-MM-DD
INTERVAL = 1
COL_NAME = "Day"

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

data = data[data['clientID'] != '0']
data.loc[:, 'visitDuration'] = data['visitDuration'].apply(lambda x: int(x))
data['dt_date'] = pd.to_datetime(data['date'])


# In[4]
# get all visit dates for IDs


visit_dates = data.groupby('clientID')['dt_date'].agg(set)
first_visit_date = data[data['isNewUser'] == '1'].groupby('clientID')['dt_date'].agg(min)


# In[5]
# fill df with mean durations


k_cols = int(str((FINISH_DATE - START_DATE + timedelta(1)) // INTERVAL).split()[0])
df = pd.DataFrame({'Date' : [''] * k_cols})


date = START_DATE
row_i = k_cols - 1

while date + timedelta(INTERVAL - 1) <= FINISH_DATE:
    df.at[row_i, 'Date'] = str(pretty_date(date_to_str(date)))


    interval_dates = [date + timedelta(i) for i in range(INTERVAL)]
    all_ids_valid_visit_date = set(dict(filter(lambda x: x[1] in interval_dates, first_visit_date.items())).keys())

    cur_date = date

    for i in range(row_i + 1, 0, -1):
        cur_interval_dates = [cur_date + timedelta(i) for i in range(INTERVAL)]
        s = sum(data[(data['clientID'].isin(all_ids_valid_visit_date)) & (data['dt_date'].isin(cur_interval_dates))]['visitDuration'])

        try:
            df.at[row_i, COL_NAME + ' ' + str(row_i - i + 1)] = round(s / len(all_ids_valid_visit_date), 2)
        except:
            df.at[row_i, COL_NAME + ' ' + str(row_i - i + 1)] = 0

        cur_date += timedelta(INTERVAL)


    date += timedelta(INTERVAL)
    row_i -= 1


# In[6]
# upload df


df.to_csv(COL_NAME + '.csv', index = False)
print('Result dowloaded into ' + COL_NAME + '.csv')


# %%
