Mini Data Pipeline (Web Scraping → Kafka)

STUDENT_ID: 22B22B1533
Проект: Web Scraping → Data Cleaning → Kafka Producer

Этот проект автоматически извлекает данные о самых высоких зданиях мира с Википедии, очищает их (6 операций) и отправляет в Kafka в виде JSON-сообщений.

1. Источник данных

Скрипт загружает первую таблицу класса wikitable со страницы:

https://ru.wikipedia.org/wiki/Список_самых_высоких_зданий_мира

Обрабатываются первые 20 строк таблицы.

2. Структура проекта
/project
│── script.py |
│── cleaned_data.json |            
│── README.md |

3. Используемые технологии

Python 3.x
requests
BeautifulSoup4
pandas
json
kafka-python
logging

Kafka Producer отправляет JSON в UTF-8 с кириллицей (ensure_ascii=False).

4. Web Scraping (ШАГ 1)

Функция:
scrape_data(url)

Делает следующее:

Загружает HTML через requests

Находит первую таблицу wikitable

Извлекает заголовки

Считывает строки

Формирует DataFrame

Ограничивает результат 20 строками

Результат — pandas.DataFrame со всеми исходными колонками.

5. Data Cleaning (ШАГ 2)

| №   | Операция                          | Детали                                                                                   |
| --- | --------------------------------- | ---------------------------------------------------------------------------------------- |
| ОП1 | Переименование столбцов           | `№ → Rank`, `Название → Building_Name`, `Город → City`, столбец высоты → `Height_Meters` |
| ОП2 | Удаление ненужных столбцов        | Удаляются колонки: футы, шпиль, крыша, этажность, примечания, антенна, координаты        |
| ОП3 | Удаление сносок                   | Убираются `[1]`, `[2]`, `[a]` с помощью regex                                            |
| ОП4 | Приведение к нижнему регистру     | `Building_Name` и `City` приводятся к lower case                                         |
| ОП5 | Преобразование числового значения | Извлекается первое число из `Height_Meters`, переводится в `float`                       |
| ОП6 | Удаление строк с NaN              | Убираются строки без `Rank`, `Building_Name`, `City`, `Height_Meters`                    |


Финальный результат содержит ТОЛЬКО 4 столбца:

Rank
Building_Name
City
Height_Meters

6. Сохранение данных (ШАГ 3)

Очищенные данные сохраняются в:

cleaned_data.json


Формат — JSON array, UTF-8, кириллица не экранируется.

7. Отправка в Kafka (ШАГ 4)

Каждая строка отправляется в Kafka как отдельное JSON-сообщение.

Название топика формируется из STUDENT_ID: bonus_22B22B1533

Kafka Producer:

работает на localhost:9092

сериализует JSON в UTF-8

отправляет каждую строку отдельно

выводит информацию в консоль через logging

8. Пример данных, которые отправляются в Kafka

Скрипт отправляет каждую строку как отдельное JSON-сообщение в топик bonus_22B22B1533.
Ниже приведён реальный набор данных, который формируется после очистки:

```json
[
    {
        "Rank": "1",
        "Building_Name": "бурдж-халифа",
        "City": "дубай",
        "Height_Meters": 828.0
    },
    {
        "Rank": "2",
        "Building_Name": "merdeka 118",
        "City": "куала-лумпур",
        "Height_Meters": 678.0
    },
    {
        "Rank": "3",
        "Building_Name": "шанхайская башня",
        "City": "шанхай",
        "Height_Meters": 634.0
    },
    {
        "Rank": "4",
        "Building_Name": "королевская часовая башня",
        "City": "мекка",
        "Height_Meters": 601.0
    },
    {
        "Rank": "5",
        "Building_Name": "международный финансовый центр ping an",
        "City": "шэньчжэнь",
        "Height_Meters": 599.0
    },
    {
        "Rank": "6",
        "Building_Name": "башня lotte world",
        "City": "сеул",
        "Height_Meters": 554.0
    },
    {
        "Rank": "7",
        "Building_Name": "всемирный торговый центр 1",
        "City": "нью-йорк",
        "Height_Meters": 541.0
    },
    {
        "Rank": "8",
        "Building_Name": "гуанчжоуский финансовый центр ctf",
        "City": "гуанчжоу",
        "Height_Meters": 530.0
    },
    {
        "Rank": "9",
        "Building_Name": "тяньцзиньский финансовый центр ctf",
        "City": "тяньцзинь",
        "Height_Meters": 530.0
    },
    {
        "Rank": "10",
        "Building_Name": "пекинская башня citic",
        "City": "пекин",
        "Height_Meters": 528.0
    },
    {
        "Rank": "11",
        "Building_Name": "тайбэй 101",
        "City": "тайбэй",
        "Height_Meters": 508.0
    },
    {
        "Rank": "12",
        "Building_Name": "шанхайский всемирный финансовый центр",
        "City": "шанхай",
        "Height_Meters": 492.0
    },
    {
        "Rank": "13",
        "Building_Name": "международный коммерческий центр",
        "City": "гонконг",
        "Height_Meters": 484.0
    },
    {
        "Rank": "14",
        "Building_Name": "гринлэнд-центр",
        "City": "ухань",
        "Height_Meters": 475.0
    },
    {
        "Rank": "15",
        "Building_Name": "сентрал-парк-тауэр",
        "City": "нью-йорк",
        "Height_Meters": 472.0
    },
    {
        "Rank": "16",
        "Building_Name": "landmark 81",
        "City": "хошимин",
        "Height_Meters": 469.0
    },
    {
        "Rank": "17",
        "Building_Name": "лахта центр",
        "City": "санкт-петербург",
        "Height_Meters": 462.0
    },
    {
        "Rank": "18",
        "Building_Name": "international land-sea center",
        "City": "чунцин",
        "Height_Meters": 458.0
    },
    {
        "Rank": "19",
        "Building_Name": "the exchange 106",
        "City": "куала-лумпур",
        "Height_Meters": 453.0
    },
    {
        "Rank": "20",
        "Building_Name": "чаншинская башня ifs 1",
        "City": "чанша",
        "Height_Meters": 452.0
    }

]


