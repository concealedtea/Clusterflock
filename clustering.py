import pandas as pd
from sklearn import datasets
from sklearn.cluster import KMeans
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
import csv, os, time
from sqlalchemy import create_engine

# Make sure to refresh audience_modeling table in RedShift, query included in folder
# CSV Columns = user_id, ip_address, browser_name, platform, title, event_name, subid4, subid7, imp, date
def initialize_data():
    start = time.time()
    engine = create_engine('REMOVED')
    query = ("""select
                  distinct(user_id) as user_id,
                  browser_name as browser_name,
                  browser_version as browser_version,
                  os_name as  os_name,
                  platform as platform,
                  SUM(CASE WHEN event_name = 'notify_allow'
                    THEN 1
                      ELSE 0 END) AS notify_allow,
                  SUM(CASE WHEN event_name = 'notify_click'
                    THEN 1
                      ELSE 0 END) AS notify_click,
                  SUM(CASE WHEN event_name = 'notify_received'
                    THEN 1
                      ELSE 0 END) AS notify_received,
                  SUM(CASE WHEN event_name = 'notify_register'
                    THEN 1
                      ELSE 0 END) AS notify_register
                from impressions
                where 1 = 1
                      and event_name in ('notify_received', 'notify_click', 'notify_allow', 'notify_register')
                      and imp in ('horoscope_microsite')
                      and date BETWEEN '2018-06-01' AND CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
                group by
                  user_id,
                  browser_name,
                  browser_version,
                  os_name,
                  platform""")
    df = pd.read_sql_query(query, engine)
    end = time.time()
    print('Time Taken To Grab Data:' + str(end - start) + 'seconds')
    start = time.time()
    df_tr = df
    clms = ['notify_received','notify_allow', 'notify_click', 'notify_register']
    df_tr_std = stats.zscore(df_tr[clms])

    # KMeans
    kmeans = KMeans(n_clusters = 4, random_state = 0).fit(df_tr_std)
    print('Clustered! Now Sorting .....')
    labels = kmeans.labels_
    df_tr['clusters'] = labels
    clms.extend(['clusters'])
    end = time.time()
    print('Time Taken To Cluster and Sort ' + str(len(df_tr)) + ' data points: ' + str(end-start) + ' seconds')
    print(kmeans.cluster_centers_)
    df_tr.to_csv('results.csv')

def main():
    initialize_data()
main()
