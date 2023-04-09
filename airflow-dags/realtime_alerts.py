import telegram
import seaborn as sns
import matplotlib.pyplot as plt
import io

import numpy as np

import pandahouse

from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'k-zhuk',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 4, 5),
}

schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kzhuk_alerts_dag():
    
    
    @task
    def get_dau_os(connection: dict) -> 'DataFrame':

        query = '''

            select
              toStartOfFifteenMinutes(toDateTime(time)) as time,
              os,
              uniqExact(user_id) as active_users
            from
              simulator_20230220.feed_actions
            where
              time >= today() - 1
              and time != toStartOfFifteenMinutes(now())
            group by
              time,
              os
            order by
              time

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)

        return df
    

    @task
    def get_likes_views_ctr(connection: dict) -> 'DataFrame':

        query = '''

            select
              toStartOfFifteenMinutes(toDateTime(time)) as time,
              countIf(user_id, action = 'like') as likes,
              countIf(user_id, action = 'view') as views,
              likes / views as ctr
            from
              simulator_20230220.feed_actions
            where
              time >= today() - 1
              and time != toStartOfFifteenMinutes(now())
            group by
              time
            order by
              time

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)

        return df


    @task
    def get_dau_sent_messages(connection: dict) -> 'DataFrame':

        query = '''

            select
              toStartOfFifteenMinutes(toDateTime(time)) as time,
              uniqExact(user_id) as active_users,
              count(user_id) as sent_messages
            from
              simulator_20230220.message_actions
            where
              time >= today() - 1
              and time != toStartOfFifteenMinutes(now())
            group by
              time
            order by
              time

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)

        return df

    
    def send_alert(metrics: str, _slice: str, slide_values: 'np.array',
               target_val: 'np.array', bot: 'telegram bot', time_val: str,
               link_plot: str, link_text: str):
        
        # sigma alert
        is_sigma_alert = False
        
        mean_val = np.mean(a=slide_values)
        std_val = np.std(a=slide_values, ddof=1)
        
        if not (mean_val - 3 * std_val) < target_val < (mean_val + 3 * std_val):
            is_sigma_alert = True
        
        # IQR alert
        is_iqr_alert = False
        
        q_25 = np.quantile(a=slide_values, q=0.25)
        q_75 = np.quantile(a=slide_values, q=0.75)
        IQR = q_75 - q_25
        
        if not (q_25 - 1.5 * IQR) < target_val < (q_75 + 1.5 * IQR):
            is_iqr_alert = True
        
        if is_sigma_alert and is_iqr_alert:
            
            deviation_val = round(((target_val / slide_values[-1]) - 1) * 100, 2)
            
            # pretty output
            if deviation_val > 0:
                deviation_val = f'+{deviation_val}%'
            else:
                deviation_val = f'{deviation_val}%'
            
            alert_text = f'Metrics <b>{metrics}</b> in slice <b>{_slice}</b>.\n'\
                        f'Currrent value <b>{round(target_val, 4)}</b>. Deviation <b>{deviation_val}</b>\n'\
                        f'\nRedash <a href="{link_plot}">{link_text}</a>\n'\
                        f'\nAnomaly time {time_val}'
            
            bot.send_message(chat_id=-951642901,
                            text=alert_text,
                            parse_mode=telegram.ParseMode.HTML)


    @task
    def dau_os_alert(df: 'DataFrame', bot: 'telegram bot',
                    last_intervals: int = 15):
        
        dau_android_df = df[df['os'] == 'Android']
        dau_ios_df = df[df['os'] == 'iOS']
        
        link_plot = "https://redash.lab.karpov.courses/dashboards/3531-alerts-783444-dau-os"
        
        # last n 15-minutes intervals, conf.interval 
        n = last_intervals

        # ========
        # Android
        # ========
        slide_values = dau_android_df['active_users'].values[-n - 1:-1]
        target_val = dau_android_df['active_users'].values[-1]
        
        # event time
        time_val = f"{dau_android_df['time'].iloc[-1].hour}:{dau_android_df['time'].iloc[-1].minute}"
        
        send_alert(metrics='DAU',
                  _slice='OS (Android)',
                  slide_values=slide_values,
                  target_val=target_val,
                  bot=bot,
                  time_val=time_val,
                  link_plot=link_plot,
                  link_text='DAU os graphics')
        
        # ========
        # iOS
        # ========
        slide_values = dau_ios_df['active_users'].values[-n - 1:-1]
        target_val = dau_ios_df['active_users'].values[-1]
        
        send_alert(metrics='DAU',
                  _slice='OS (iOS)',
                  slide_values=slide_values,
                  target_val=target_val,
                  bot=bot,
                  time_val=time_val,
                  link_plot=link_plot, 
                  link_text='DAU os graphics')
        

    @task
    def likes_views_ctr_alert(df: 'DataFrame', bot: 'telegram bot',
                              last_intervals: int = 15):
        
        link_plot_views = "https://redash.lab.karpov.courses/dashboards/3533-alerts-783444-views"
        link_plot_likes = "https://redash.lab.karpov.courses/dashboards/3534-alerts-783444-likes"
        link_plot_ctr = "https://redash.lab.karpov.courses/dashboards/3535-alerts-783444-ctr"
        
        # last n 15-minutes intervals, conf.interval 
        n = last_intervals

        # ========
        # views
        # ========
        slide_values = df['views'].values[-n - 1:-1]
        target_val = df['views'].values[-1]
        
        # event time
        time_val = f"{df['time'].iloc[-1].hour}:{df['time'].iloc[-1].minute}"
        
        send_alert(metrics='Views',
                  _slice='...',
                  slide_values=slide_values,
                  target_val=target_val,
                  bot=bot,
                  time_val=time_val,
                  link_plot=link_plot_views,
                  link_text='Views graphics')
        
        # ========
        # likes
        # ========
        slide_values = df['likes'].values[-n - 1:-1]
        target_val = df['likes'].values[-1]
        
        send_alert(metrics='Likes',
                  _slice='...',
                  slide_values=slide_values,
                  target_val=target_val,
                  bot=bot,
                  time_val=time_val,
                  link_plot=link_plot_likes, 
                  link_text='Likes graphics')
        
        # ========
        # CTR
        # ========
        slide_values = df['ctr'].values[-n - 1:-1]
        target_val = df['ctr'].values[-1]
        
        send_alert(metrics='CTR',
                  _slice='...',
                  slide_values=slide_values,
                  target_val=target_val,
                  bot=bot,
                  time_val=time_val,
                  link_plot=link_plot_ctr, 
                  link_text='CTR graphics')

    
    @task
    def dau_sent_messages_alert(df: 'DataFrame', bot: 'telegram bot',
                                last_intervals: int = 15):
        
        link_plot_dau_messages = "https://redash.lab.karpov.courses/dashboards/3532-alerts-783444-dau-messenger"
        link_plot_sent_messages = "https://redash.lab.karpov.courses/dashboards/3536-alerts-783444-sent_messages"
        
        # last n 15-minutes intervals, conf.interval 
        n = last_intervals

        # ========
        # dau (active_users)
        # ========
        slide_values = df['active_users'].values[-n - 1:-1]
        target_val = df['active_users'].values[-1]
        
        # event time
        time_val = f"{df['time'].iloc[-1].hour}:{df['time'].iloc[-1].minute}"
        
        send_alert(metrics='DAU messenger',
                  _slice='...',
                  slide_values=slide_values,
                  target_val=target_val,
                  bot=bot,
                  time_val=time_val,
                  link_plot=link_plot_dau_messages,
                  link_text='DAU messenger graphics')
        
        # ========
        # sent messages
        # ========
        slide_values = df['sent_messages'].values[-n - 1:-1]
        target_val = df['sent_messages'].values[-1]
        
        send_alert(metrics='Sent messages',
                  _slice='...',
                  slide_values=slide_values,
                  target_val=target_val,
                  bot=bot,
                  time_val=time_val,
                  link_plot=link_plot_sent_messages, 
                  link_text='Sent messages graphics')
    
    connection = {
                    'host': 'https://clickhouse.lab.karpov.courses',
                    'password': 'xxx',
                    'user': 'xxx',
                    'database': 'xxx'
                 }

    bot_token = '6089386976:AAH-TjP3pY7mUI_rAf2xzTyQBXU31BHaPuA'
    bot = telegram.Bot(token=bot_token)
    
    dau_df = get_dau_os(connection=connection)
    dau_os_alert(df=dau_df, bot=bot, last_intervals=15)

    likes_views_ctr_df = get_likes_views_ctr(connection=connection)
    likes_views_ctr_alert(df=likes_views_ctr_df, bot=bot, last_intervals=15)
    
    dau_sent_messages_df = get_dau_sent_messages(connection=connection)
    dau_sent_messages_alert(df=dau_sent_messages_df, bot=bot, last_intervals=15)

dag = kzhuk_alerts_dag()
