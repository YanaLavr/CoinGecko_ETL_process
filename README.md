# CoinGecko ETL Дашборд

## Описание проекта

Полный ETL-пайплайн для анализа криптовалютного рынка. Проект автоматически собирает данные из CoinGecko API, обрабатывает их через Apache Airflow, хранит в ClickHouse с трехслойной архитектурой данных и визуализирует в Apache Superset.

**Результат:** интерактивный дашборд с ключевыми показателями рынка, топ-10 монет по капитализации, динамикой портфеля из 5 монет за 30 дней и детальной таблицей.

---

## Архитектура
CoinGecko API → Airflow (ETL) → ClickHouse (хранилище) → Superset (дашборд)


### Технологический стек

| Слой | Технология | Назначение |
|------|-----------|------------|
| Источник данных | CoinGecko API | Рыночные данные криптовалют |
| Оркестрация | Apache Airflow | Автоматизация ETL-процесса |
| Хранилище данных | ClickHouse | Колоночная аналитическая БД |
| Визуализация | Apache Superset | Интерактивные дашборды |
| Языки | Python, SQL | Извлечение, трансформация, запросы |

---

## Пайплайн данных

### 1. Extract (Извлечение)

- `get_market_data()` — загружает топ-49 монет по капитализации (цена, объем, изменение за 24ч)
- `fetch_multiple_histories()` — загружает историю цен за 30 дней для 5 выбранных монет: Bitcoin, Ethereum, Solana, Cardano, Polkadot

### 2. Transform (Трансформация)

- Конвертация типов данных (числа, даты)
- Очистка пропусков (fillna)
- Добавление технических полей (`loaded_at`)

### 3. Load (Загрузка)

Трёхслойная архитектура хранилища:

| Слой | Таблицы | Движок ClickHouse | Назначение |
|------|---------|-------------------|------------|
| **raw** | `market_data`, `price_history` | MergeTree | Сырые данные API, полная история загрузок |
| **cleaned** | `market_data`, `price_history` | ReplacingMergeTree | Очищенные данные с дедупликацией по `loaded_at` |
| **mart** | `top_coins`, `portfolio_5`, `price_change_pct`, `market_overview` | MergeTree | Витрины для дашборда, пересчитываются ежедневно |

---

## Дашборд

### Структура

| Блок | Чарт | Данные |
|------|------|--------|
| KPI | 3 Big Numbers | Общая капитализация, объём торгов 24ч, количество монет |
| Рынок | Bar Chart | Топ-10 монет по капитализации |
| Портфель | Line Chart | Динамика цен 5 монет за 30 дней (в процентах от начала периода) |
| Детали | Table | Цена, капитализация, объем, изменение 24ч по 5 монетам |

### Фильтры

- Выбор диапазона дат для анализа динамики
- Выбор конкретных монет из портфеля

---

## Как запустить

### Требования

- Docker
- Docker Compose
- 
### Зависимости

Кастомный Docker-образ включает:
- `clickhouse-driver` — подключение к ClickHouse
- `pandas` — трансформация данных
- `requests` — запросы к API

Собирается через `Dockerfile.airflow`:
```dockerfile
FROM apache/airflow:3.2.2
RUN pip install clickhouse-driver pandas requests

### Шаги

```bash
# 1. Клонировать репозиторий
git clone https://github.com/YanaLavr/CoinGecko_ETL_process.git
cd CoinGecko_ETL_process

# 2. Запустить инфраструктуру
docker-compose up -d

# 3. Создать таблицы в ClickHouse
# Подключиться к ClickHouse и выполнить create_tables.sql
docker exec -it clickhouse clickhouse-client
# Вставить содержимое create_tables.sql

# 4. Открыть Airflow UI
# http://localhost:8080
# Логин: airflow / пароль: пароль генерируется автоматически - проверить в логах
# Найти DAG crypto_etl, запустить вручную или дождаться расписания

# 5. Открыть Superset
# http://localhost:8088
# Подключиться к ClickHouse (mart schema)
# Создать чарты из таблиц mart.top_coins, mart.portfolio_5 и т.д.

### Расписание DAG
Запуск ежедневно в 09:00 UTC: 0 9 * * *

### Структура проекта
CoinGecko_ETL_process/
├── docker-compose.yml          # Инфраструктура
├── create_tables.sql           # Создание схем и таблиц
├── README.md                   # Описание проекта
├── dags/
│   └── crypto_pipeline.py      # DAG для Airflow
├── scripts/
│   ├── extract.py              # Запросы к CoinGecko API
│   └── transform.py            # Очистка данных в pandas
└── screenshots/
    └── dashboard.png           # Скрин дашборда

## Описание архитектура

| Решение | Почему |
|---------|--------|
| 3 слоя (raw/cleaned/mart) | Защита от потери данных, возможность пересчёта, скорость для дашборда |
| ReplacingMergeTree в cleaned | Автоматическая дедупликация при повторных загрузках |
| ClickHouse | Колоночное хранение = быстрые аналитические запросы |
| Airflow DAG | Автоматизация: запускается сам, не нужно помнить про обновление |