import os
import logging
import datetime

from config import Config
from modules.dds_loader import DdsControler
from modules.stg_loader import StgControler
from airflow import DAG
from airflow.operators.python import PythonOperator

config = Config()
log = logging.getLogger(__name__)

[os.remove(file) for file in os.listdir(os.getcwd()) if file.endswith('.csv')]
[os.mkdir(name) for name in ["Archives", "Downloads", "Unresolved"] if name not in os.listdir()]


def weather_data(controller: StgControler, lag: int) -> None:
    with controller.pg_connect.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"""TRUNCATE TABLE STAGE.weather_observation""")
        #cursor.execute(f"""SELECT max(DATE) FROM DDS.weather_observation""")
        # a = cursor.fetchone()[0]
        # print(a.year, a.month, a.day)
        # month = datetime.datetime(year=a.year, month=a.month, day=a.day)
        month = datetime.datetime(year=2018, month=1, day=1)
        month_end = month + datetime.timedelta(days=4+lag)

        query = f"""
        SELECT DISTINCT indx_nr, incident_date, incident_time, weather_station
        FROM DDS.aircraft_incidents
        INNER JOIN DDS.incident_station_link link ON aircraft_incidents.indx_nr=link.index_incedent
        WHERE incident_date between '{month}' and '{month_end}'
        AND indx_nr not in (SELECT distinct incident
                            FROM DDS.weather_observation)
        ORDER BY incident_date ASC"""
        cursor.execute(query)
        records = cursor.fetchall()
        print(1, len(records))
        min_date = min([x[1] for x in records])
        max_date = max([x[1] for x in records])
        stations = [x[3] for x in records]

        for i in range(len(records) // 25):
            print(2, len(stations))
            print(3, len(set(stations)))

            controller.receive_weatherstation_data(station_id=','.join(list(set(stations[:25]))),
                                                   start_datetime=min_date - datetime.timedelta(hours=1),
                                                   end_datetime=max_date + datetime.timedelta(hours=1))
            controller.load_weatherstation_data(table_name="weather_observation", rows=records[:25])
            stations = stations[25:]
            records = records[25:]


def animal_incidents_data(controller: StgControler,
                          start_date: str = None,
                          end_date: str = None):
    controller.receive_animal_incidents_data(start_date=start_date, end_date=end_date)
    controller.unzip_data()
    controller.download_incidents(table_name='aircraft_incidents')


def top_airports(controller: StgControler, process_date):
    with config.pg_warehouse_db().connection() as connect:
        cursor = connect.cursor()
        query = """
        with cte as(
        SELECT airport_id, airport, count(*), now()::date as processed_dt FROM DDS.aircraft_incidents
        GROUP BY airport_id, airport ORDER BY 3 DESC LIMIT 11) -- 11 из-за UNKNOWN но стоит обсудить с аналитиками
        SELECT cte.airport_id, airport, bts_name FROM cte
        INNER JOIN DDS.airport_bts_name --UNKNOWN на INNER JOIN отфильтруется
            ON cte.airport_id = airport_bts_name.id;
        """
        cursor.execute(query)
        tuples_airports = [(x[0], x[1], x[2]) for x in cursor.fetchall()]
        controller.top_airports_traffic(table_name='top_ten_airports', airports_data=tuples_airports,
                                        process_date=process_date)


stg_loadings = StgControler(date=datetime.datetime.now().date(),
                            pg_connect=config.pg_warehouse_db(),
                            schema='Stage',
                            logger=log)
dds_uploads = DdsControler(date=datetime.datetime.now().date(),
                           pg_connect=config.pg_warehouse_db(),
                           schema='DDS',
                           logger=log)

with DAG(
        dag_id="example_timetable_dag",
        start_date=datetime.datetime(2018, 1, 1),
        max_active_runs=1,
        schedule="5 4 * * 3",
        catchup=False,
        default_args={
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=3)}
) as dag:
    task_animal_incidents = PythonOperator(
        task_id='download_animal_incidents',
        python_callable=animal_incidents_data,
        op_kwargs={'controller': stg_loadings,
                   'start_date': '2018-01-01',
                   'end_date': '2022-12-31'})
    task_weather_data = PythonOperator(
        task_id='download_weather_data',
        python_callable=weather_data,
        op_kwargs={'controller': stg_loadings,
                   'start_date': '2018-01-01',
                   'end_date': '2022-12-31'})
    # top_airports = PythonOperator(
    #     task_id='download_weather_data',
    #     python_callable=top_airports,
    #     op_kwargs={'controller': stg_loadings,
    #                'process_date': datetime.datetime.now().date()})

# Обновление справочника со станциями #лучше выполнять ежедневно перед запуском других расчетов
# stg_loadings.isd_history(table_name='observation_reference')
# animal_incidents_data(controller=stg_loadings, end_date='2022-12-31')

for i in range(10):
    try:
        print("START")
        dds_uploads.upload_weather_observation(table_name='weather_observation')
        weather_data(controller=stg_loadings, lag=i)
    except Exception as e:
        print(e)
        raise

# [task_animal_incidents, task_weather_data, top_airports]