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
    'start_date': datetime(2023, 4, 3),
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kzhuk_reports_app_dag():
    
    
    @task
    def get_msgs_per_gender(connection: dict) -> 'DataFrame':

        query = '''

            select
              count(user_id) as total_msgs,
              uniqExact(user_id) as uniq_users,
              total_msgs / uniq_users as msgs_per_user,
              If(gender = 1, 'male', 'female') as gender,
              toDate(time) as time
            from
              simulator_20230220.message_actions
            where
              (time >= (today() - 14))
              and (time < today())
            group by
              time,
              gender
            order by
              time

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)

        return df

    
    @task
    def get_msgs_per_os(connection: dict) -> 'DataFrame':

        query = '''

            select
              count(user_id) as total_msgs,
              uniqExact(user_id) as uniq_users,
              total_msgs / uniq_users as msgs_per_user,
              os,
              toDate(time) as time
            from
              simulator_20230220.message_actions
            where
              (time >= (today() - 14))
              and (time < today())
            group by
              time,
              os
            order by
              time

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)

        return df

    
    @task
    def get_dau_per_gender(connection: dict) -> 'DataFrame':

        query = '''

            select
              uniqExact(user_id) as uniq_users,
              If(gender = 1, 'male', 'female') as gender,
              toDate(time) as time
            from
              simulator_20230220.feed_actions
            where
              (time >= (today() - 14))
              and (time < today())
            group by
              time,
              gender
            order by
              time

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)

        return df

    
    @task
    def get_ctr_per_source(connection: dict) -> 'DataFrame':

        query = '''

            select
              countIf(user_id, action = 'like') as likes,
              countIf(user_id, action = 'view') as views,
              likes / views as ctr,
              source,
              toDate(time) as time
            from
              simulator_20230220.feed_actions
            where
              (time >= (today() - 14))
              and (time < today())
            group by
              time,
              source
            order by
              time

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)

        return df

    
    @task
    def get_posts_dynamic(connection: dict) -> 'DataFrame':

        query = '''

            select
              count(post_id) as new_posts,
              cur_day
            from
              (
                select
                  post_id,
                  cur_day,
                  If(t2.post_id = 0, 'new', 'retained') as status
                from
                  (
                    select
                      distinct post_id,
                      toDate(time) as cur_day,
                      addDays(cur_day, - 1) as prev_day
                    from
                      simulator_20230220.feed_actions
                    where
                      (cur_day >= (today() - 15))
                      and (cur_day < today())
                  ) t1
                  left join (
                    select
                      distinct post_id,
                      toDate(time) as prev_day
                    from
                      simulator_20230220.feed_actions
                    where
                      (time >= (today() - 15))
                      and (time < today())
                  ) t2 on (t1.post_id = t2.post_id)
                  and (t1.prev_day = t2.prev_day)
                where
                  status = 'new'
                  and cur_day > (today() - 15)
              )
            group by
              cur_day

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)

        return df

    
    @task
    def get_likes_views(connection: dict) -> 'DataFrame':

        query = '''

            select
              countIf(user_id, action = 'like') as likes,
              countIf(user_id, action = 'view') as views,
              toDate(time) as time
            from
              simulator_20230220.feed_actions
            where
              time >= today() - 14
              and time < today()
            group by
              time
            order by
              time

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)

        return df

    
    @task
    def get_app_uniqs(connection: dict) -> 'DataFrame':

        query = '''

            select
              sum(total_users_current) as total_users_current,
              sum(total_users_before) as total_users_before
            from
              (
                select
                  count() as total_users_current
                from
                  (
                    select
                      distinct user_id
                    from
                      simulator_20230220.feed_actions
                    where
                      toDate(time) >= today() - 14
                      and toDate(time) < today()
                  ) t1 full
                  outer join (
                    select
                      distinct user_id
                    from
                      simulator_20230220.message_actions
                    where
                      toDate(time) >= today() - 14
                      and toDate(time) < today()
                  ) t2 on t1.user_id = t2.user_id
              ) t3 full
              outer join (
                select
                  count() as total_users_before
                from
                  (
                    select
                      distinct user_id
                    from
                      simulator_20230220.feed_actions
                    where
                      toDate(time) >= today() - 28
                      and toDate(time) < today() - 14
                  ) t1 full
                  outer join (
                    select
                      distinct user_id
                    from
                      simulator_20230220.message_actions
                    where
                      toDate(time) >= today() - 28
                      and toDate(time) < today() - 14
                  ) t2 on t1.user_id = t2.user_id
              ) t4 on t3.total_users_current = t4.total_users_before

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)
        return df

    
    @task
    def get_msg_uniqs(connection: dict) -> 'DataFrame':

        query = '''

            select
              sum(total_users_current) as total_users_current,
              sum(total_users_before) as total_users_before
            from
              (
                select
                  count() as total_users_current
                from
                  (
                    select
                      distinct user_id
                    from
                      simulator_20230220.message_actions
                    where
                      toDate(time) >= today() - 14
                      and toDate(time) < today()
                  ) t1
                  left join (
                    select
                      distinct user_id
                    from
                      simulator_20230220.feed_actions
                    where
                      toDate(time) >= today() - 14
                      and toDate(time) < today()
                  ) t2 on t1.user_id = t2.user_id
                where
                  user_id != t2.user_id
              ) t3 full
              outer join (
                select
                  count() as total_users_before
                from
                  (
                    select
                      distinct user_id
                    from
                      simulator_20230220.message_actions
                    where
                      toDate(time) >= today() - 28
                      and toDate(time) < today() - 14
                  ) t1
                  left join (
                    select
                      distinct user_id
                    from
                      simulator_20230220.feed_actions
                    where
                      toDate(time) >= today() - 28
                      and toDate(time) < today() - 14
                  ) t2 on t1.user_id = t2.user_id
                where
                  user_id != t2.user_id
              ) t4 on t3.total_users_current = t4.total_users_before

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)
        return df
    
    
    @task
    def get_feed_uniqs(connection: dict) -> 'DataFrame':

        query = '''

            select
          sum(total_users_current) as total_users_current,
          sum(total_users_before) as total_users_before
        from
          (
            select
              count() as total_users_current
            from
              (
                select
                  distinct user_id
                from
                  simulator_20230220.message_actions
                where
                  toDate(time) >= today() - 14
                  and toDate(time) < today()
              ) t1
              right join (
                select
                  distinct user_id
                from
                  simulator_20230220.feed_actions
                where
                  toDate(time) >= today() - 14
                  and toDate(time) < today()
              ) t2 on t1.user_id = t2.user_id
            where
              user_id != t2.user_id
          ) t3 full
          outer join (
            select
              count() as total_users_before
            from
              (
                select
                  distinct user_id
                from
                  simulator_20230220.message_actions
                where
                  toDate(time) >= today() - 28
                  and toDate(time) < today() - 14
              ) t1
              right join (
                select
                  distinct user_id
                from
                  simulator_20230220.feed_actions
                where
                  toDate(time) >= today() - 28
                  and toDate(time) < today() - 14
              ) t2 on t1.user_id = t2.user_id
            where
              user_id != t2.user_id
          ) t4 on t3.total_users_current = t4.total_users_before

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)
        return df

    
    @task
    def get_top_posts(connection: dict) -> 'DataFrame':

        query = '''

            select
              post_id,
              countIf(user_id, action = 'like') as likes,
              countIf(user_id, action = 'view') as views,
              likes / views as ctr
            from
              simulator_20230220.feed_actions
            where
              toDate(time) >= today() - 14
              and toDate(time) < today()
            group by
              post_id
            order by
              likes desc
            limit
              3

        '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)
        return df

    
    @task
    def plot_metrics(msgs_gender_df, msgs_os_df, dau_gender_df, ctr_source_df, posts_dynamic_df, likes_views_df) -> 'io.Bytes':

        fig, axs = plt.subplots(nrows=3,
                                ncols=2,
                                figsize=(16, 21),
                                facecolor='white')

        alpha = 0.15
        alpha_edge = 0.9

        # =================================
        # ------------Messages-------------
        # =================================
        msgs_male = msgs_gender_df.query("gender == 'male'")
        msgs_female = msgs_gender_df.query("gender == 'female'")

        x_values = list(map(lambda x: f'{x.day} {x.month_name()}', msgs_male['time']))

        ax = sns.lineplot(x=x_values,
                          y=msgs_male['msgs_per_user'],
                          color='black', alpha=alpha_edge,
                          label='male',
                          ax=axs[0][0], marker='o')

        sns.lineplot(x=x_values,
                     y=msgs_female['msgs_per_user'],
                     color=(165/255, 0/255, 255/255), alpha=alpha_edge,
                     ax=ax,
                     label='female', marker='o')

        ax.set_xticklabels(x_values,
                           rotation=60)
        ax.legend(fontsize=14)
        ax.set_title('Messages per gender user', fontsize=18)

        ylim = ax.get_ylim()

        ax.fill_between(x=x_values,
                        y1=msgs_male['msgs_per_user'],
                        y2=ylim[0],
                        color='black',
                        alpha=alpha)

        ax.fill_between(x=x_values,
                        y1=msgs_female['msgs_per_user'],
                        y2=ylim[0],
                        color=(165/255, 0/255, 255/255),
                        alpha=alpha)  

        ax.margins(x=0, y=0)

        # =================================
        # ------------OS-------------
        # =================================
        msgs_ios = msgs_os_df.query("os == 'iOS'")
        msgs_android = msgs_os_df.query("os == 'Android'")

        ax = sns.lineplot(x=x_values,
                          y=msgs_ios['msgs_per_user'],
                          color='black', alpha=alpha_edge,
                          label='iOS',
                          ax=axs[0][1], marker='o')

        sns.lineplot(x=x_values,
                     y=msgs_android['msgs_per_user'],
                     color=(255/255, 0/255, 0/255), alpha=alpha_edge,
                     ax=ax,
                     label='Android', marker='o')
        ax.set_xticklabels(x_values,
                           rotation=60)
        ax.legend(fontsize=14)
        ax.set_title('Messages per OS user', fontsize=18)

        ylim = ax.get_ylim()

        ax.fill_between(x=x_values,
                        y1=msgs_ios['msgs_per_user'],
                        y2=ylim[0],
                        color='black',
                        alpha=alpha)

        ax.fill_between(x=x_values,
                        y1=msgs_android['msgs_per_user'],
                        y2=ylim[0],
                        color=(255/255, 0/255, 0/255),
                        alpha=alpha)  

        ax.margins(x=0, y=0)

        # =================================
        # ------------DAU per gender-------------
        # =================================
        dau_male = dau_gender_df.query("gender == 'male'")
        dau_female = dau_gender_df.query("gender == 'female'")

        ax = sns.lineplot(x=x_values,
                          y=dau_male['uniq_users'],
                          color='black', alpha=alpha_edge,
                          label='male',
                          ax=axs[1][0], marker='o')

        sns.lineplot(x=x_values,
                     y=dau_female['uniq_users'],
                     color=(165/255, 0/255, 255/255), alpha=alpha_edge,
                     ax=ax,
                     label='female', marker='o')

        ax.set_xticklabels(x_values,
                           rotation=60)
        ax.legend(fontsize=14)
        ax.set_title('DAU feed per gender', fontsize=18)

        ylim = ax.get_ylim()

        ax.fill_between(x=x_values,
                        y1=dau_male['uniq_users'],
                        y2=ylim[0],
                        color='black',
                        alpha=alpha)

        ax.fill_between(x=x_values,
                        y1=dau_female['uniq_users'],
                        y2=ylim[0],
                        color=(165/255, 0/255, 255/255),
                        alpha=alpha)  

        ax.margins(x=0, y=0)

        # =================================
        # ------------CTR per gender-------------
        # =================================
        ctr_organic = ctr_source_df.query("source == 'organic'")
        ctr_ads = ctr_source_df.query("source == 'ads'")

        ax = sns.lineplot(x=x_values,
                          y=ctr_organic['ctr'],
                          color='black', alpha=alpha_edge,
                          label='organic',
                          ax=axs[1][1], marker='o')

        sns.lineplot(x=x_values,
                     y=ctr_ads['ctr'],
                     color=(255/255, 94/255, 5/255), alpha=alpha_edge,
                     ax=ax,
                     label='ads', marker='o')

        ax.set_xticklabels(x_values,
                           rotation=60)
        ax.legend(fontsize=14)
        ax.set_title('CTR per source', fontsize=18)

        ylim = ax.get_ylim()

        ax.fill_between(x=x_values,
                        y1=ctr_organic['ctr'],
                        y2=ylim[0],
                        color='black',
                        alpha=alpha)

        ax.fill_between(x=x_values,
                        y1=ctr_ads['ctr'],
                        y2=ylim[0],
                        color=(255/255, 94/255, 5/255),
                        alpha=alpha)  

        ax.margins(x=0, y=0)

        # ======================================
        # ------------posts dynamic-------------
        # ======================================
        # будем считать пост новым, если его не было вчера

        ax = sns.lineplot(x=x_values,
                          y=posts_dynamic_df['new_posts'],
                          color='black', alpha=alpha_edge,
                          ax=axs[2][0], marker='o')

        ax.set_xticklabels(x_values,
                           rotation=60)
        ax.set_title('New posts per day', fontsize=18)

        ylim = ax.get_ylim()

        ax.fill_between(x=x_values,
                        y1=posts_dynamic_df['new_posts'],
                        y2=ylim[0],
                        color='black',
                        alpha=alpha)

        ax.margins(x=0, y=0)

        # для отрисовки выходных дней
        weekend_dates = []
        for i, date_val in enumerate(posts_dynamic_df['cur_day']):
            day_of_week = date_val.weekday()
            if day_of_week >= 5:
                weekend_dates.append(i)

        axs[2][0].vlines(x=weekend_dates,
                         ymin=ylim[0],
                         ymax=ylim[1],
                         color='red',
                         alpha=0.9,
                         ls='--', lw=2,
                         label='weekend')
        ax.legend(fontsize=14)

        # ======================================
        # ------------likes and views-------------
        # ======================================

        ax = sns.lineplot(x=x_values,
                          y=likes_views_df['likes'],
                          color='blue', alpha=alpha_edge,
                          ax=axs[2][1], marker='o')

        sns.lineplot(x=x_values,
                     y=likes_views_df['views'],
                     color='black', alpha=alpha_edge,
                     ax=axs[2][1], marker='o')

        ax.set_xticklabels(x_values,
                           rotation=60)
        ax.set_title('Likes and views', fontsize=18)

        ax.margins(x=0)
        ylim = ax.get_ylim()

        ax.fill_between(x=x_values,
                        y1=likes_views_df['likes'],
                        y2=ylim[0],
                        color='blue',
                        alpha=alpha)

        ax.fill_between(x=x_values,
                        y1=likes_views_df['views'],
                        y2=ylim[0],
                        color='black',
                        alpha=alpha)

        ax.set_ylim(ylim)

        # чтобы заголовки друг на друга не залезали
        fig.tight_layout()

        # формируем данные для отправки
        plot_object = io.BytesIO()
        fig.savefig(plot_object, dpi=200)
        plot_object.seek(0)
        plot_object.name = 'k-zhuk Daily App report.jpg'

        return plot_object

    
    @task
    def send_report(report_doc: 'io.Bytes', app_uniqs_df: 'DataFrame',
                    msg_uniqs_df: 'DataFrame',
                    feed_uniqs_df: 'DataFrame',
                    top_posts: 'DataFrame'):

        app_uniqs = app_uniqs_df['total_users_current'].iloc[0]
        app_growth_percent = (app_uniqs_df['total_users_current'].iloc[0] / app_uniqs_df['total_users_before'].iloc[0]) - 1

        msg_uniqs = msg_uniqs_df['total_users_current'].iloc[0]
        msg_growth_percent = (msg_uniqs_df['total_users_current'].iloc[0] / msg_uniqs_df['total_users_before'].iloc[0]) - 1

        feed_uniqs = feed_uniqs_df['total_users_current'].iloc[0]
        feed_growth_percent = (feed_uniqs_df['total_users_current'].iloc[0] / feed_uniqs_df['total_users_before'].iloc[0]) - 1

        # pretty format for app
        if app_growth_percent > 0:
            app_growth_percent_text = f'+{round(app_growth_percent * 100, 2)}%'
        else:
            app_growth_percent_text = f'{round(app_growth_percent * 100, 2)}%'

        # pretty format for msg
        if msg_growth_percent > 0:
            msg_growth_percent_text = f'+{round(msg_growth_percent * 100, 2)}%'
        else:
            msg_growth_percent_text = f'{round(msg_growth_percent * 100, 2)}%'

        # pretty format for feed
        if feed_growth_percent > 0:
            feed_growth_percent_text = f'+{round(feed_growth_percent * 100, 2)}%'
        else:
            feed_growth_percent_text = f'{round(feed_growth_percent * 100, 2)}%'

        posts_caption = ''

        for post in top_posts.values:
            posts_caption += f'------\npost_id {int(post[0])}\nlikes = {int(post[1])}\n'\
            f'views = {int(post[2])}\nCTR = {round(post[3] * 100, 2)}%\n'

        caption = f'Last 14 days report\n======\napp users = {app_uniqs} <b>{app_growth_percent_text}</b>\n======\n'\
                   f'only msg users = {msg_uniqs} <b>{msg_growth_percent_text}</b>\n======\n'\
                   f'only feed users = {feed_uniqs} <b>{feed_growth_percent_text}</b>\n======\n'\
                   f'\nTOP 3 POST\n'\
                   f'{posts_caption}'

        # подключаем бота
        bot_token = '6089386976:AAH-TjP3pY7mUI_fXf2xzKvPBXPq1BHaZhA'
        bot = telegram.Bot(token=bot_token)
        bot.send_document(chat_id=-801539328,
                          document=report_doc,
                          caption=caption,
                          parse_mode=telegram.ParseMode.HTML)

    connection = {
                    'host': 'https://clickhouse.lab.karpov.courses',
                    'password': 'xxx',
                    'user': 'xxx',
                    'database': 'xxx'
                 }
                 
    sns.set_theme()

    # messages
    msgs_per_gender = get_msgs_per_gender(connection=connection)
    msgs_per_os = get_msgs_per_os(connection=connection)

    # DAU
    dau_per_gender = get_dau_per_gender(connection=connection)

    # CTR
    ctr_per_source = get_ctr_per_source(connection=connection)

    # posts dynamic
    posts_dynamic = get_posts_dynamic(connection=connection)

    # likes and views
    likes_views = get_likes_views(connection=connection)

    # app stats
    app_uniqs = get_app_uniqs(connection=connection)

    # msg stats
    msg_uniqs = get_msg_uniqs(connection=connection)

    # feed stats
    feed_uniqs = get_feed_uniqs(connection=connection)

    # top post stats
    top_posts = get_top_posts(connection=connection)

    # get graphics stats
    report_doc = plot_metrics(msgs_gender_df=msgs_per_gender,
                              msgs_os_df=msgs_per_os,
                              dau_gender_df=dau_per_gender,
                              ctr_source_df=ctr_per_source,
                              posts_dynamic_df=posts_dynamic,
                              likes_views_df=likes_views)

    send_report(report_doc=report_doc,
                app_uniqs_df=app_uniqs,
                msg_uniqs_df=msg_uniqs,
                feed_uniqs_df=feed_uniqs,
                top_posts=top_posts)

dag = kzhuk_reports_app_dag()
