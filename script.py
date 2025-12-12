import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import logging
from kafka import KafkaProducer
import sys

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# КОНФИГУРАЦИЯ

STUDENT_ID = "22B22B1533" 
TARGET_URL = "https://ru.wikipedia.org/wiki/Список_самых_высоких_зданий_мира" 
KAFKA_BROKER = "localhost:9092" 
TOPIC_NAME = f"bonus_{STUDENT_ID}" # bonus_22B22B1533


# ШАГ 1: WEB SCRAPING (requests + BeautifulSoup)

def scrape_data(url):
    """Извлекает данные из первой таблицы 'wikitable'."""
    logging.info(f"Начинается скрапинг с {url}...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при получении URL: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'class': 'wikitable'}) 

    if not table:
        logging.error("Не удалось найти целевую таблицу 'wikitable'.")
        return None

    try:
        headers = [th.text.strip().replace('\n', ' ') for th in table.find('tr').find_all('th')]
    except AttributeError:
        logging.error("Не удалось извлечь заголовки таблицы.")
        return None
    
    data = []
    for row in table.find_all('tr')[1:]: 
        cells = row.find_all(['td', 'th'])
        row_data = [cell.text.strip().replace('\n', ' ') for cell in cells]
        
        if len(row_data) == len(headers):
            data.append(row_data)
        
        if len(data) >= 20: 
            break

    df = pd.DataFrame(data, columns=headers)
    logging.info(f"Скрапинг завершен. Извлечено {len(df)} строк.")
    return df

# ШАГ 2: DATA CLEANING 

def clean_data(df):
    """Выполняет 6 операций очистки данных."""
    logging.info("Начинается очистка данных (6 операций)...")
    cleaned_df = df.copy() 
    
    # Очищаем заголовки, чтобы упростить поиск и переименование
    cleaned_df.columns = cleaned_df.columns.str.replace(r'[\t\xa0\(\)]', ' ', regex=True).str.strip()
    cleaned_df.columns = cleaned_df.columns.str.replace(r'\s+', ' ', regex=True).str.strip()
    
    logging.info(f"Начальные колонки после очистки: {cleaned_df.columns.tolist()}") 

    # 1. ОПЕРАЦИЯ: Переименование столбцов
    renames = {}
    
    # УПРОЩЕННЫЙ ПОИСК столбца высоты
    height_col_original = [col for col in cleaned_df.columns if 'Высота' in col or 'м)' in col]
    if height_col_original:
        # Берем самый вероятный столбец высоты
        renames[height_col_original[0]] = 'Height_Meters'
    
    # Поиск других столбцов
    # ИСПРАВЛЕНО: Теперь ищем '№' для Rank
    for old_name_part, new_name in [('№', 'Rank'), ('Название', 'Building_Name'), ('Город', 'City')]:
        found_cols = [col for col in cleaned_df.columns if old_name_part in col]
        if found_cols:
            renames[found_cols[0]] = new_name

    cleaned_df = cleaned_df.rename(columns=renames, errors='ignore')
    logging.info("ОП1: Переименование столбцов выполнено.")
    
    # ПРОВЕРКА
    if 'Height_Meters' not in cleaned_df.columns:
        logging.error("КРИТИЧЕСКАЯ ОШИБКА ОЧИСТКИ: Столбец 'Height_Meters' не был найден. Пропуск дальнейшей очистки.")
        return pd.DataFrame() 

    # 2. ОПЕРАЦИЯ: Удаление ненужных столбцов (УСИЛЕНО)
    cols_to_drop_keywords = ['фут', 'шпилю', 'крыше', 'постройки', 'Этажность', 'Координаты', 
                             'Фото', 'антенны', 'эксплуатируемого', 'Примечания']
    cols_to_drop = [
        col for col in cleaned_df.columns 
        if any(keyword in col for keyword in cols_to_drop_keywords) and col not in renames.values()
    ]
    cleaned_df = cleaned_df.drop(columns=cols_to_drop, errors='ignore')
    logging.info("ОП2: Удаление лишних столбцов.")
    
    # 3. ОПЕРАЦИЯ: Обработка строк - удаление сносок и обрезка
    for col in ['Building_Name', 'City']:
        if col in cleaned_df.columns:
            cleaned_df[col] = cleaned_df[col].astype(str).str.replace(r'\[.+?\]', '', regex=True).str.strip()
            # 4. ОПЕРАЦИЯ: Приведение к нижнему регистру
            cleaned_df[col] = cleaned_df[col].str.lower()
    logging.info("ОП3/ОП4: Удаление сносок, обрезка и приведение строковых полей к нижнему регистру.")
    
    # 5. ОПЕРАЦИЯ: Преобразование числового столбца 
    # Извлекаем первое числовое значение (для метров)
    cleaned_df['Height_Meters'] = cleaned_df['Height_Meters'].astype(str).str.extract(r'(\d+)').astype(float)
    cleaned_df['Height_Meters'] = pd.to_numeric(cleaned_df['Height_Meters'], errors='coerce')
    logging.info("ОП5: Извлечение и преобразование Height_Meters в число.")

    # 6. ОПЕРАЦИЯ: Удаление строк с пропущенными/неверными значениями
    initial_rows = len(cleaned_df)
    # Используем все ключевые поля для удаления NaN
    cleaned_df = cleaned_df.dropna(subset=['Height_Meters', 'Building_Name', 'City', 'Rank']) 
    rows_dropped = initial_rows - len(cleaned_df)
    logging.info(f"ОП6: Удаление строк с NaN. Удалено {rows_dropped} строк. Осталось {len(cleaned_df)} строк.")
    
    # Финальная очистка и сброс индекса
    cleaned_df = cleaned_df.reset_index(drop=True)
    
    # Оставляем ТОЛЬКО ТРЕБУЕМЫЕ СТОЛБЦЫ: Rank, Building_Name, City, Height_Meters
    final_cols = ['Rank', 'Building_Name', 'City', 'Height_Meters']
    cleaned_df = cleaned_df[[col for col in final_cols if col in cleaned_df.columns]]

    logging.info("Очистка данных завершена.")
    return cleaned_df

# ШАГ 3: СОХРАНЕНИЕ ДАННЫХ


def save_data(df, filename='cleaned_data.json'):
    """Сохраняет очищенный DataFrame в JSON."""
    df.to_json(filename, orient='records', indent=4, force_ascii=False)
    logging.info(f"Данные успешно сохранены в {filename}")



# ШАГ 4: KAFKA PRODUCTION 


def json_serializer(data):
    """Сериализатор JSON для Kafka (обрабатывает NaN и гарантирует UTF-8 для кириллицы)."""
    def convert_nan(obj):
        if isinstance(obj, float) and pd.isna(obj):
            return None
        return obj
        
    # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: ensure_ascii=False!
    # Это говорит Python не экранировать кириллицу (\uXXXX)
    return json.dumps(data, default=convert_nan, ensure_ascii=False).encode('utf-8')

def produce_to_kafka(df, topic, broker):
    """Отправляет каждую строку DataFrame как JSON-сообщение в Kafka."""
    if df.empty:
        logging.warning("DataFrame пуст. Отправка в Kafka пропущена.")
        return

    logging.info(f"Начинается отправка {len(df)} сообщений в топик '{topic}' на {broker}...")

    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=json_serializer
        )
        
        success_count = 0
        
        for index, row in df.iterrows():
            message = row.to_dict()
            producer.send(topic, value=message)
            success_count += 1
            
        producer.flush()
        logging.info(f"Успешно отправлено {success_count} сообщений в Kafka.")

    except Exception as e:
        logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА KAFKA: {e}")
        logging.error(f"Проверьте, что Kafka Broker запущен и доступен по адресу: {broker}.")


# ОСНОВНОЙ СКРИПТ 


if __name__ == "__main__":
    
    scraped_df = scrape_data(TARGET_URL)
    
    if scraped_df is not None and not scraped_df.empty:
        
        cleaned_df = clean_data(scraped_df)
        
        if not cleaned_df.empty:
            save_data(cleaned_df, 'cleaned_data.json')

            produce_to_kafka(cleaned_df, TOPIC_NAME, KAFKA_BROKER)
        else:
            logging.error("Очищенный DataFrame пуст. Нечего отправлять в Kafka.")
    else:
        logging.error("Скрапинг не дал данных. Завершение работы.")