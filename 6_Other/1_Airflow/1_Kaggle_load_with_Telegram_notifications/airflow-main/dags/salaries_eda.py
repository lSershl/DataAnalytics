from datetime import datetime, timedelta
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from kaggle.api.kaggle_api_extended import KaggleApi

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def setup_kaggle():
    os.makedirs('/home/airflow/.kaggle', exist_ok=True)

def download_dataset(**kwargs):
    api = KaggleApi()
    api.authenticate()
    
    api.dataset_download_files(
        'varishabatool/cyber-security-salaries-dataset',
        path='/tmp',
        unzip=True
    )
    
    df = pd.read_csv('/tmp/salaries_cyber.csv')
    df_path = '/tmp/salaries_cyber.csv'
    df.to_csv(df_path, index=False)
    
    return df_path

def perform_eda(**kwargs):
    ti = kwargs['ti']
    df_path = ti.xcom_pull(task_ids='download_dataset')
    df = pd.read_csv(df_path)
    
    stats = df.describe().to_string()
    
    plot1_path = '/tmp/plot1.png'
    plt.figure(figsize=(12, 6))
    sns.countplot(data=df, y='job_title', order=df['job_title'].value_counts().index[:10])
    plt.title('Top 10 Cyber Security Jobs Distribution')
    plt.savefig(plot1_path, format='png', bbox_inches='tight')
    plt.close()

    plot2_path = '/tmp/plot2.png'
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x='salary_in_usd', y='experience_level', 
                order=['EN', 'MI', 'SE', 'EX'])
    plt.title('Salary Distribution by Experience Level')
    plt.xlabel('Salary in USD')
    plt.ylabel('Experience Level')
    plt.savefig(plot2_path, format='png', bbox_inches='tight')
    plt.close()
    
    return {
        'stats': stats,
        'plot1_path': plot1_path,
        'plot2_path': plot2_path,
        'top_jobs': df['job_title'].value_counts().head(5).to_dict()
    }

def send_to_telegram(**kwargs):
    ti = kwargs['ti']
    eda_results = ti.xcom_pull(task_ids='perform_eda')

    bot_token = Variable.get("telegram_bot_token")
    chat_id = Variable.get("telegram_chat_id")

    base_url = f'https://api.telegram.org/bot{bot_token}'

    top_jobs = "\n".join([f"{k}: {v}" for k, v in eda_results['top_jobs'].items()])
    message = f"""
🔐 Cyber Security Salaries EDA Results

📊 Top 5 Job Titles:
{top_jobs}

📈 Statistics:
{eda_results['stats']}
    """

    # Отправка текста
    requests.post(
        f'{base_url}/sendMessage',
        data={'chat_id': chat_id, 'text': message}
    )

    # Отправка первого графика
    with open(eda_results['plot1_path'], 'rb') as photo1:
        requests.post(
            f'{base_url}/sendPhoto',
            data={'chat_id': chat_id, 'caption': 'Top 10 Cyber Security Jobs Distribution'},
            files={'photo': photo1}
        )

    # Отправка второго графика
    with open(eda_results['plot2_path'], 'rb') as photo2:
        requests.post(
            f'{base_url}/sendPhoto',
            data={'chat_id': chat_id, 'caption': 'Salary Distribution by Experience Level'},
            files={'photo': photo2}
        )

with DAG(
    'salaries_eda',
    default_args=default_args,
    description='EDA анализ датасета зарплат в Cyber Security',
    # следующая строка задаёт расписание выполнения DAG
    # DAG будет повторяться еженедельно (@weekly), т. е. в полночь воскресенья
    # либо можно вставить cron-выражение, например: */5 * * * *
    # (будет выполняться каждые 5 минут)
    schedule='*/5 * * * *',
    catchup=False,
    tags=['eda', 'cyber-security'],
) as dag:
    
    setup_kaggle_task = PythonOperator(
        task_id='setup_kaggle',
        python_callable=setup_kaggle,
    )
    
    download_task = PythonOperator(
        task_id='download_dataset',
        python_callable=download_dataset,
    )
    
    eda_task = PythonOperator(
        task_id='perform_eda',
        python_callable=perform_eda,
    )
    
    telegram_task = PythonOperator(
        task_id='send_to_telegram',
        python_callable=send_to_telegram,
    )
    
    setup_kaggle_task >> download_task >> eda_task >> telegram_task
