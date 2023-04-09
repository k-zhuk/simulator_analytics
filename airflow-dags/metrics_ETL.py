import numpy as np
import pandahouse as ph
import seaborn as sns

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'k-zhuk',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 3, 30),
}

schedule_interval = '0 9 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kzhuk_783444_dag_29_03():
    
    @task
    def get_uniqs(connection: dict) -> "DataFrame":
        uniqs_query = """
        select
          user_id,
          os,
          gender,
          multiIf(
            age < 18,
            'до 18',
            age >= 18
            and age < 25,
            'от 18 до 25',
            age >= 25
            and age < 35,
            'от 25 до 35',
            age >= 35
            and age < 45,
            'от 35 до 45',
            'от 45'
          ) as age
        from(
            (
              select
                user_id,
                os,
                If(gender = 1, 'male', 'female') as gender,
                age
              from
                simulator_20230220.feed_actions
            )
            union
              distinct (
                select
                  user_id,
                  os,
                  If(gender = 1, 'male', 'female') as gender,
                  age
                from
                  simulator_20230220.message_actions
              )
          )
        """

        uniqs_df = ph.read_clickhouse(uniqs_query, connection=connection)

        return uniqs_df
    
    @task
    def get_likes_views(connection: dict) -> "DataFrame":
        likes_views_query = """
        select
              user_id,
              countIf(action = 'like') as likes,
              countIf(action = 'view') as views
            from
              simulator_20230220.feed_actions
            where
              toDate(time) == yesterday()
            group by
              user_id
        """

        likes_views_df = ph.read_clickhouse(likes_views_query, connection=connection)

        return likes_views_df
    
    @task
    def get_messages_info(connection: dict) -> "DataFrame":
        messages_info_query = """
        select
              *
            from
              (
                select
                  user_id,
                  count(distinct reciever_id) as users_sent,
                  count() as messages_sent
                from
                  simulator_20230220.message_actions
                where
                  toDate(time) == yesterday()
                group by
                  user_id
              ) t1 full
              outer join (
                select
                  reciever_id as user_id,
                  count(distinct user_id) as users_received,
                  count() as messages_received
                from
                  simulator_20230220.message_actions
                where
                  toDate(time) == yesterday()
                group by
                  reciever_id
              ) t2 using user_id
        """

        messages_info_df = ph.read_clickhouse(messages_info_query, connection=connection)

        return messages_info_df
    
    @task
    def get_dimension_os(df: 'DataFrame') -> 'DataFrame':
        
        dimension_os = df.groupby(by='os').sum().reset_index(drop=False).rename(columns={'os': 'dimension_value'})
        dimension_os['event_date'] = datetime.date(datetime.today()) - timedelta(days=1)
        dimension_os.insert(loc=0, column='dimension', value='os')

        return dimension_os
    
    @task
    def get_dimension_gender(df: 'DataFrame') -> 'DataFrame':

        dimension_gender = df.groupby(by='gender').sum().reset_index(drop=False).rename(columns={'gender': 'dimension_value'})

        dimension_gender['event_date'] = datetime.date(datetime.today()) - timedelta(days=1)
        dimension_gender.insert(loc=0, column='dimension', value='gender')
        
        return dimension_gender
    
    @task
    def get_dimension_age(df: 'DataFrame') -> 'DataFrame':
        
        dimension_age = df.groupby(by='age').sum().reset_index(drop=False).rename(columns={'age': 'dimension_value'})

        dimension_age['event_date'] = datetime.date(datetime.today()) - timedelta(days=1)
        dimension_age.insert(loc=0, column='dimension', value='age')
        
        return dimension_age
    
    @task
    def get_merged_df(likes_views_df: 'DataFrame', messages_info_df: 'DataFrame', uniqs_df: 'DataFrame') -> 'DataFrame':
        
        info_df = likes_views_df.merge(right=messages_info_df,
                                       on='user_id',
                                       how='outer')
        info_df_fill = info_df.fillna(value=0)
        info_df_fill = info_df_fill.merge(right=uniqs_df,
                                          on='user_id',
                                          how='left')
        
        return info_df_fill
    
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'xxx',
        'user': 'xxx',
        'database': 'xxx'
    }
    
    @task
    def get_statistica_yesterday_df(gender: 'DataFrame', os: 'DataFrame', age: 'DataFrame') -> 'DataFrame':
        
        statistica_yesterday = pd.concat([os, gender, age],
                                         ignore_index=True,
                                         axis=0).drop(columns=['user_id'])
        
        return statistica_yesterday
    
    @task
    def update_db(result: 'DataFrame') -> 'DataFrame':
        
        push_connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'xxx',
            'user': 'xxx',
            'database': 'xxx'
        }
        
        try:
            ph.to_clickhouse(df=result, index=False, table='kzhuk_dag_783444', connection=push_connection)
        except Exception as e:
            create_table_query = """
            create table test.kzhuk_dag_783444
            (
                dimension String,
                dimension_value String,
                likes Float64,
                views Float64,
                users_sent Float64,
                messages_sent Float64,
                users_received Float64,
                messages_received Float64,
                event_date String
            )
            engine = MergeTree()
            order by (likes)
            """

            ph.execute(query=create_table_query, connection=push_connection)
            ph.to_clickhouse(df=result, index=False, table='kzhuk_dag_783444', connection=push_connection)
    
    
    # получаем всех уникальных пользователей
    # чтобы сделать срезы по полу, ОС и возрасту
    uniqs_df = get_uniqs(connection=connection)
    
    # получаем лайки и просмотры для каждого пользователя
    likes_views_df = get_likes_views(connection=connection)
    
    # получаем инфо по сообщениям для каждого пользователя
    messages_info_df = get_messages_info(connection=connection)
    
    # получаем таблицу для работы со срезами
    merged_df = get_merged_df(likes_views_df=likes_views_df,
                              messages_info_df=messages_info_df,
                              uniqs_df=uniqs_df)
    
    # получаем срезы
    dimension_gender = get_dimension_gender(df=merged_df)
    dimension_os = get_dimension_os(df=merged_df)
    dimension_age = get_dimension_age(df=merged_df)
    
    # получаем финальную таблицу со статистикой по метрикам
    statistica_yesterday = get_statistica_yesterday_df(os=dimension_os,
                                                       gender=dimension_gender,
                                                       age=dimension_age)
    
    update_db(result=statistica_yesterday)
    
dag_783444 = kzhuk_783444_dag_29_03()
