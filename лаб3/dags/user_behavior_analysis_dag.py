"""
DAG для анализа поведения пользователей мобильного приложения
Вариант задания №14

Задача: рассчитать среднее время сессии и средний чек для пользователей,
зарегистрированных в последнем месяце.

Автор: [Ваше Имя]
Дата: 2024
"""

from datetime import datetime, timedelta
import pandas as pd
import json
import sqlite3
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

# Конфигурация DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['test@example.com']
}

# Создание DAG
dag = DAG(
    'user_behavior_analysis',
    default_args=default_args,
    description='Анализ поведения пользователей мобильного приложения',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'user_behavior', 'mobile_app', 'variant_14']
)

# Пути к файлам данных
DATA_DIR = '/opt/airflow/dags/data'
DB_PATH = '/opt/airflow/user_behavior.db'

def extract_users_data(**context):
    """
    Extract: Чтение данных о пользователях из CSV файла
    """
    print("Начинаем извлечение данных о пользователях из CSV...")
    
    csv_path = os.path.join(DATA_DIR, 'users.csv')
    
    try:
        # Чтение CSV файла
        users_df = pd.read_csv(csv_path)
        print(f"Загружено {len(users_df)} записей о пользователях")
        print("Первые 5 записей:")
        print(users_df.head())
        
        # Сохранение данных для следующих задач
        users_data = users_df.to_dict('records')
        context['task_instance'].xcom_push(key='users_data', value=users_data)
        
        print("Данные о пользователях успешно извлечены и сохранены в XCom")
        return f"Извлечено {len(users_df)} записей о пользователях"
        
    except Exception as e:
        print(f"Ошибка при извлечении данных о пользователях: {str(e)}")
        raise

def extract_sessions_data(**context):
    """
    Extract: Чтение данных о сессиях из Excel файла
    """
    print("Начинаем извлечение данных о сессиях из Excel...")
    
    excel_path = os.path.join(DATA_DIR, 'sessions.xlsx')
    
    try:
        # Чтение Excel файла
        sessions_df = pd.read_excel(excel_path)
        print(f"Загружено {len(sessions_df)} записей о сессиях")
        print("Первые 5 записей:")
        print(sessions_df.head())
        
        # Сохранение данных для следующих задач
        sessions_data = sessions_df.to_dict('records')
        context['task_instance'].xcom_push(key='sessions_data', value=sessions_data)
        
        print("Данные о сессиях успешно извлечены и сохранены в XCom")
        return f"Извлечено {len(sessions_df)} записей о сессиях"
        
    except Exception as e:
        print(f"Ошибка при извлечении данных о сессиях: {str(e)}")
        raise

def extract_purchases_data(**context):
    """
    Extract: Чтение данных о покупках из JSON файла
    """
    print("Начинаем извлечение данных о покупках из JSON...")
    
    json_path = os.path.join(DATA_DIR, 'purchases.json')
    
    try:
        # Чтение JSON файла
        with open(json_path, 'r', encoding='utf-8') as f:
            purchases_data = json.load(f)
        
        purchases_df = pd.DataFrame(purchases_data)
        print(f"Загружено {len(purchases_df)} записей о покупках")
        print("Первые 5 записей:")
        print(purchases_df.head())
        
        # Сохранение данных для следующих задач
        context['task_instance'].xcom_push(key='purchases_data', value=purchases_data)
        
        print("Данные о покупках успешно извлечены и сохранены в XCom")
        return f"Извлечено {len(purchases_df)} записей о покупках"
        
    except Exception as e:
        print(f"Ошибка при извлечении данных о покупках: {str(e)}")
        raise

def transform_data(**context):
    """
    Transform: Анализ поведения пользователей
    """
    print("Начинаем трансформацию данных...")
    
    try:
        # Получение данных из предыдущих задач
        users_data = context['task_instance'].xcom_pull(key='users_data', task_ids='extract_users')
        sessions_data = context['task_instance'].xcom_pull(key='sessions_data', task_ids='extract_sessions')
        purchases_data = context['task_instance'].xcom_pull(key='purchases_data', task_ids='extract_purchases')
        
        # Преобразование в DataFrame
        users_df = pd.DataFrame(users_data)
        sessions_df = pd.DataFrame(sessions_data)
        purchases_df = pd.DataFrame(purchases_data)
        
        print("Данные успешно получены из XCom")
        print(f"Пользователи: {len(users_df)} записей")
        print(f"Сессии: {len(sessions_df)} записей")
        print(f"Покупки: {len(purchases_df)} записей")
        
        # Преобразование даты регистрации в datetime
        users_df['registration_date'] = pd.to_datetime(users_df['registration_date'])
        
        # Определение даты "последнего месяца" (от текущей даты)
        current_date = datetime.now()
        one_month_ago = current_date - timedelta(days=30)
        
        print(f"Текущая дата: {current_date}")
        print(f"Анализируем пользователей с датой регистрации после: {one_month_ago}")
        
        # Фильтрация пользователей, зарегистрированных в последнем месяце
        recent_users_df = users_df[users_df['registration_date'] >= one_month_ago]
        recent_user_ids = recent_users_df['user_id'].tolist()
        
        print(f"Найдено {len(recent_user_ids)} пользователей, зарегистрированных в последнем месяце:")
        print(recent_user_ids)
        
        if not recent_user_ids:
            print("Нет пользователей, зарегистрированных в последнем месяце!")
            # Создаем пустой результат для продолжения работы
            result_data = pd.DataFrame(columns=['metric', 'value'])
            context['task_instance'].xcom_push(key='user_behavior_analysis', value=result_data.to_dict('records'))
            return "Нет данных для анализа (нет новых пользователей)"
        
        # Фильтрация сессий и покупок только для новых пользователей
        recent_sessions_df = sessions_df[sessions_df['user_id'].isin(recent_user_ids)]
        recent_purchases_df = purchases_df[purchases_df['user_id'].isin(recent_user_ids)]
        
        print(f"Сессии новых пользователей: {len(recent_sessions_df)} записей")
        print(f"Покупки новых пользователей: {len(recent_purchases_df)} записей")
        
        # Расчет метрик
        print("Начинаем расчет метрик поведения пользователей...")
        
        # 1. Среднее время сессии
        avg_session_duration = recent_sessions_df['session_duration_minutes'].mean()
        
        # 2. Средний чек (Average Order Value)
        if not recent_purchases_df.empty:
            avg_purchase_amount = recent_purchases_df['purchase_amount'].mean()
            total_revenue = recent_purchases_df['purchase_amount'].sum()
            purchase_count = len(recent_purchases_df)
            paying_users_count = recent_purchases_df['user_id'].nunique()
        else:
            avg_purchase_amount = 0
            total_revenue = 0
            purchase_count = 0
            paying_users_count = 0
        
        # Дополнительные метрики
        total_users_count = len(recent_user_ids)
        conversion_rate = (paying_users_count / total_users_count * 100) if total_users_count > 0 else 0
        
        # Создание результата анализа
        analysis_results = [
            {'metric': 'avg_session_duration', 'value': round(avg_session_duration, 2), 'description': 'Среднее время сессии (минуты)'},
            {'metric': 'avg_purchase_amount', 'value': round(avg_purchase_amount, 2), 'description': 'Средний чек (рубли)'},
            {'metric': 'total_revenue', 'value': round(total_revenue, 2), 'description': 'Общая выручка (рубли)'},
            {'metric': 'total_users', 'value': total_users_count, 'description': 'Всего новых пользователей'},
            {'metric': 'paying_users', 'value': paying_users_count, 'description': 'Платящих пользователей'},
            {'metric': 'purchase_count', 'value': purchase_count, 'description': 'Всего покупок'},
            {'metric': 'conversion_rate', 'value': round(conversion_rate, 2), 'description': 'Конверсия (%)'},
            {'metric': 'analysis_period_start', 'value': one_month_ago.strftime('%Y-%m-%d'), 'description': 'Начало анализируемого периода'},
            {'metric': 'analysis_date', 'value': current_date.strftime('%Y-%m-%d'), 'description': 'Дата анализа'}
        ]
        
        results_df = pd.DataFrame(analysis_results)
        print("Результаты анализа поведения пользователей:")
        print(results_df)
        
        # Сохранение результатов для загрузки в БД
        result_data = results_df.to_dict('records')
        context['task_instance'].xcom_push(key='user_behavior_analysis', value=result_data)
        
        # Дополнительно сохраняем детальные данные для отчета
        context['task_instance'].xcom_push(key='recent_users_count', value=total_users_count)
        context['task_instance'].xcom_push(key='avg_session_duration', value=round(avg_session_duration, 2))
        context['task_instance'].xcom_push(key='avg_purchase_amount', value=round(avg_purchase_amount, 2))
        context['task_instance'].xcom_push(key='conversion_rate', value=round(conversion_rate, 2))
        
        print("Трансформация данных завершена успешно")
        return f"Проанализировано поведение {total_users_count} новых пользователей"
        
    except Exception as e:
        print(f"Ошибка при трансформации данных: {str(e)}")
        raise

def load_to_database(**context):
    """
    Load: Загрузка результатов анализа в SQLite базу данных
    """
    print("Начинаем загрузку данных в базу данных...")
    
    try:
        # Получение результатов анализа
        analysis_data = context['task_instance'].xcom_pull(
            key='user_behavior_analysis', 
            task_ids='transform_data'
        )
        
        if not analysis_data:
            raise ValueError("Нет данных для загрузки в базу данных")
        
        # Создание DataFrame из результатов
        analysis_df = pd.DataFrame(analysis_data)
        
        # Подключение к SQLite базе данных
        conn = sqlite3.connect(DB_PATH)
        
        try:
            # Создание таблицы если она не существует
            create_table_query = """
            CREATE TABLE IF NOT EXISTS user_behavior_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric TEXT NOT NULL,
                value REAL NOT NULL,
                description TEXT NOT NULL,
                analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            conn.execute(create_table_query)
            
            # Очистка таблицы перед загрузкой новых данных
            conn.execute("DELETE FROM user_behavior_analysis")
            
            # Загрузка данных в таблицу
            analysis_df.to_sql('user_behavior_analysis', conn, if_exists='append', index=False)
            
            # Подтверждение транзакции
            conn.commit()
            
            print(f"Успешно загружено {len(analysis_df)} записей в базу данных")
            
            # Проверка загруженных данных
            verification_query = "SELECT * FROM user_behavior_analysis"
            result = pd.read_sql_query(verification_query, conn)
            print("Проверка загруженных данных:")
            print(result)
            
        finally:
            conn.close()
        
        print("Загрузка в базу данных завершена успешно")
        return f"Загружено {len(analysis_df)} записей в SQLite базу данных"
        
    except Exception as e:
        print(f"Ошибка при загрузке в базу данных: {str(e)}")
        raise

def generate_report(**context):
    """
    Генерация отчета с результатами анализа
    """
    print("Генерируем отчет с результатами анализа...")
    
    try:
        # Получение данных из предыдущих задач
        recent_users_count = context['task_instance'].xcom_pull(key='recent_users_count', task_ids='transform_data')
        avg_session_duration = context['task_instance'].xcom_pull(key='avg_session_duration', task_ids='transform_data')
        avg_purchase_amount = context['task_instance'].xcom_pull(key='avg_purchase_amount', task_ids='transform_data')
        conversion_rate = context['task_instance'].xcom_pull(key='conversion_rate', task_ids='transform_data')
        
        # Получение полных данных из базы данных
        conn = sqlite3.connect(DB_PATH)
        
        try:
            query = "SELECT * FROM user_behavior_analysis"
            result_df = pd.read_sql_query(query, conn)
            
            # Формирование отчета
            report = f"""ОТЧЕТ ПО АНАЛИЗУ ПОВЕДЕНИЯ ПОЛЬЗОВАТЕЛЕЙ МОБИЛЬНОГО ПРИЛОЖЕНИЯ
=======================================================================

Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Период анализа: пользователи, зарегистрированные за последние 30 дней

КЛЮЧЕВЫЕ МЕТРИКИ:
------------------
• Количество новых пользователей: {recent_users_count}
• Среднее время сессии: {avg_session_duration} минут
• Средний чек: {avg_purchase_amount:.2f} руб.
• Конверсия в покупку: {conversion_rate}%

ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ:
---------------------
"""
            
            for _, row in result_df.iterrows():
                if row['metric'] not in ['analysis_period_start', 'analysis_date']:
                    report += f"- {row['description']}: {row['value']}\n"
            
            # Добавление рекомендаций
            report += f"""
АНАЛИТИЧЕСКИЕ ВЫВОДЫ И РЕКОМЕНДАЦИИ:
------------------------------------
"""
            
            if avg_session_duration > 20:
                report += "✓ Высокое время сессии - пользователи активно используют приложение\n"
            else:
                report += "⚠️ Время сессии можно улучшить - рассмотрите улучшение UX/UI\n"
            
            if conversion_rate > 10:
                report += "✓ Отличная конверсия в покупку - эффективная монетизация\n"
            else:
                report += "⚠️ Низкая конверсия - рассмотрите улучшение воронки продаж\n"
            
            if avg_purchase_amount > 1000:
                report += "✓ Высокий средний чек - ценности продукта соответствует цене\n"
            else:
                report += "⚠️ Средний чек можно увеличить - добавьте премиум функции\n"
            
            print("Отчет сгенерирован:")
            print(report)
            
            # Сохранение отчета в файл
            report_file_path = '/opt/airflow/user_behavior_report.txt'
            with open(report_file_path, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"Отчет сохранен в файл: {report_file_path}")
            
            # Сохранение данных в CSV
            csv_file_path = '/opt/airflow/user_behavior_data.csv'
            result_df.to_csv(csv_file_path, index=False, encoding='utf-8')
            print(f"Данные сохранены в CSV: {csv_file_path}")
            
            # Сохранение данных для email
            context['task_instance'].xcom_push(key='report', value=report)
            context['task_instance'].xcom_push(key='report_file_path', value=report_file_path)
            context['task_instance'].xcom_push(key='csv_file_path', value=csv_file_path)
            context['task_instance'].xcom_push(key='key_metrics', value={
                'recent_users_count': recent_users_count,
                'avg_session_duration': avg_session_duration,
                'avg_purchase_amount': avg_purchase_amount,
                'conversion_rate': conversion_rate
            })
            
        finally:
            conn.close()
            
        return "Отчет успешно сгенерирован и сохранен в файлы"
        
    except Exception as e:
        print(f"Ошибка при генерации отчета: {str(e)}")
        raise

# Определение задач DAG

# Extract задачи
extract_users_task = PythonOperator(
    task_id='extract_users',
    python_callable=extract_users_data,
    dag=dag,
    doc_md="""Извлечение данных о пользователях из CSV файла"""
)

extract_sessions_task = PythonOperator(
    task_id='extract_sessions',
    python_callable=extract_sessions_data,
    dag=dag,
    doc_md="""Извлечение данных о сессиях из Excel файла"""
)

extract_purchases_task = PythonOperator(
    task_id='extract_purchases',
    python_callable=extract_purchases_data,
    dag=dag,
    doc_md="""Извлечение данных о покупках из JSON файла"""
)

# Transform задача
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
    doc_md="""Анализ поведения пользователей: фильтрация новых пользователей и расчет метрик"""
)

# Load задача
load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
    doc_md="""Загрузка результатов анализа в SQLite базу данных"""
)

# Генерация отчета
report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
    doc_md="""Генерация аналитического отчета с результатами"""
)

# Email задача (используем ваш существующий оператор)
email_task = EmailOperator(
    task_id='send_email',
    to='test@example.com',
    subject='Анализ поведения пользователей мобильного приложения - Результаты',
    html_content="""
    <h3>Анализ поведения пользователей мобильного приложения завершен успешно!</h3>
    <p>DAG: user_behavior_analysis</p>
    <p>Дата выполнения: {{ ds }}</p>
    <p>Все задачи выполнены без ошибок.</p>
    <p>Результаты анализа доступны в прикрепленных файлах и в базе данных SQLite.</p>
    """,
    dag=dag
)

# Определение зависимостей между задачами
[extract_users_task, extract_sessions_task, extract_purchases_task] >> transform_task
transform_task >> load_task >> report_task >> email_task