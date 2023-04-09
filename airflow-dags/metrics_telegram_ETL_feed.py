import telegram
import seaborn as sns
import matplotlib.pyplot as plt
import io

import pandahouse

from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'k-zhuk',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 4, 2),
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kzhuk_reports_feed_dag():
    
    @task
    def get_metrics_last_7_days() -> 'DataFrame':
        query = '''
        select
          countIf(user_id, action = 'like') as likes,
          countIf(user_id, action = 'view') as views,
          likes / views as ctr,
          count(distinct user_id) as uniq_users,
          toDate(time) as period
        from
          simulator_20230220.feed_actions
        where
          (period >= (today() - 7))
          and (period < today())
        group by
          period
        '''

        connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'xxx',
            'user': 'xxx',
            'database': 'xxx'
        }

        response = pandahouse.read_clickhouse(query=query, connection=connection)

        return response.copy()
    
    @task
    def metrics_plot(df: 'DataFrame'):

        fig, axs = plt.subplots(nrows=4,
                                ncols=1, 
                                figsize=(8, 21),
                                facecolor='white')

        # чтобы значения точек влезли в область
        y_scale = 0.99

        #======================DAU=======================
        ax_dau = sns.lineplot(x=df['period'],
                              y=df['uniq_users'],
                              ax=axs[0],
                              color='black',
                              alpha=0.8,
                              marker='o')

        dau_ylim = ax_dau.get_ylim()

        ax_dau.fill_between(x=df['period'],
                            y1=df['uniq_users'],
                            y2=dau_ylim[0],
                            color='black',
                            alpha=0.2)  

        ax_dau.set_ylim([dau_ylim[0], dau_ylim[1] / y_scale])
        ax_dau.set_xlabel('')
        ax_dau.set_title('DAU last 7 days', fontsize=16)

        xtick_labels = list(map(lambda x: f'{x.day} {x.month_name()}', df['period']))
        ax_dau.set_xticklabels(xtick_labels,
                               rotation=60)

        # отрисовка значений точек
        for x, y in zip(df['period'], df['uniq_users']):
            ax_dau.text(x=x, y=y * 1.002, s=y, color='white',
                        bbox=dict(boxstyle="round",
                                  fc=(0, 0, 255 / 255, 0.7),
                                  ec="black",
                                  lw=1))

        #======================Likes=======================
        ax_likes = sns.lineplot(x=df['period'],
                                y=df['likes'],
                                ax=axs[1],
                                color='black',
                                alpha=0.8,
                                marker='o')

        likes_ylim = ax_likes.get_ylim()

        ax_likes.fill_between(x=df['period'],
                              y1=df['likes'],
                              y2=likes_ylim[0],
                              color='black',
                              alpha=0.2)  

        ax_likes.set_ylim([likes_ylim[0], likes_ylim[1] / y_scale])
        ax_likes.set_xlabel('')
        ax_likes.set_title('Likes last 7 days', fontsize=16)

        ax_likes.set_xticklabels(xtick_labels,
                                 rotation=60)

        for x, y in zip(df['period'], df['likes']):
            ax_likes.text(x=x, y=y * 1.002, s=y, color='white',
                          bbox=dict(boxstyle="round",
                                    fc=(255/255, 94/255, 5/255, 0.8),
                                    ec="black",
                                    lw=1))

        #======================Views=======================
        ax_views = sns.lineplot(x=df['period'],
                                y=df['views'],
                                ax=axs[2],
                                color='black',
                                alpha=0.8,
                                marker='o')

        views_ylim = ax_views.get_ylim()

        ax_views.fill_between(x=df['period'],
                              y1=df['views'],
                              y2=likes_ylim[0],
                              color='black',
                              alpha=0.2)  

        ax_views.set_ylim([views_ylim[0], views_ylim[1] / y_scale])
        ax_views.set_xlabel('')
        ax_views.set_title('Views last 7 days', fontsize=16)

        ax_views.set_xticklabels(xtick_labels,
                                 rotation=60)

        for x, y in zip(df['period'], df['views']):
            ax_views.text(x=x, y=y * 1.002, s=y, color='white',
                          bbox=dict(boxstyle="round",
                                    fc=(255/255, 0/255, 0/255, 0.8),
                                    ec="black",
                                    lw=1))

        #======================CTR=======================
        ax_ctr = sns.lineplot(x=df['period'],
                              y=df['ctr'],
                              ax=axs[3],
                              color='black',
                              alpha=0.8,
                              marker='o')

        ctr_ylim = ax_ctr.get_ylim()

        ax_ctr.fill_between(x=df['period'],
                            y1=df['ctr'],
                            y2=ctr_ylim[0],
                            color='black',
                            alpha=0.2)  

        ax_ctr.set_ylim([ctr_ylim[0], ctr_ylim[1] / y_scale])
        ax_ctr.set_xlabel('')
        ax_ctr.set_title('CTR last 7 days', fontsize=16)

        ax_ctr.set_xticklabels(xtick_labels,
                               rotation=60)

        for x, y in zip(df['period'], df['ctr']):
            ax_ctr.text(x=x, y=y * 1.002, s=round(y, 4), color='black',
                          bbox=dict(boxstyle="round",
                                    fc=(57/255, 255/255, 20/255, 0.8),
                                    ec="black",
                                    lw=1))

        # чтобы заголовки друг на друга не залезали
        fig.tight_layout()

        # формируем данные для отправки
        plot_object = io.BytesIO()
        fig.savefig(plot_object, dpi=150)
        plot_object.seek(0)
        plot_object.name = 'k-zhuk Daily report.png'

        return plot_object
    
    @task
    def send_report(report_doc: 'io.Bytes', metrics_last_day_df: 'DataFrame'):

        dau = metrics_last_day_df['uniq_users'].values[-1]
        likes = metrics_last_day_df['likes'].values[-1]
        views = metrics_last_day_df['views'].values[-1]
        ctr = round(metrics_last_day_df['ctr'].values[-1], 4)
        report_date = metrics_last_day_df['period'].iloc[-1].date()

        caption = f'========\nDaily report\n(last day, k-zhuk)\n{report_date}\n========\n\nDAU = {dau}\nlikes = {likes}\nviews = {views}\nCTR = {ctr}'
        # подключаем бота
        bot_token = '6089386976:AAH-TjP3pY7mUI_fXf2rtKvPDFU31BHaZhA'
        bot = telegram.Bot(token=bot_token)
        bot.send_document(chat_id=-8795116228,
                          document=report_doc,
                          caption=caption)

    sns.set_theme()

    # отрисовка метрик
    metrics_last_7_days = get_metrics_last_7_days()
    report_document = metrics_plot(df=metrics_last_7_days)

    # отправляем отчет с картинкой и текстом
    send_report(report_doc=report_document, metrics_last_day_df=metrics_last_7_days)
    
dag = kzhuk_reports_feed_dag()
