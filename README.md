# Top 200 Products - Аналитика продаж Ozon

Система автоматического сбора и анализа данных о продажах товаров на маркетплейсе Ozon с интеграцией 1С и выгрузкой в Google Sheets.

## Описание

Проект автоматизирует процесс сбора данных о продажах, остатках и аналитике товаров из нескольких источников:
- **Ozon API** - заказы (постинги), остатки, аналитика по SKU
- **1С** - остатки на складах, себестоимость товаров
- **Google Sheets** - формирование итоговых отчётов с аналитикой

### Основные возможности

- ✅ Сбор данных о заказах (постингах) по периодам с фильтрацией отменённых заказов
- ✅ Получение остатков товаров по складам/кластерам
- ✅ Сбор аналитики Ozon (посетители, позиции в выдаче, выручка)
- ✅ Интеграция с 1С для получения себестоимости и остатков
- ✅ Автоматическое формирование отчётов в Google Sheets
- ✅ Кэширование данных в Redis для ускорения повторных запусков
- ✅ Резервное копирование данных в S3
- ✅ Поддержка нескольких кабинетов Ozon

## Требования

- Python 3.10+
- Redis
- PostgreSQL (опционально, для хранения метаданных)
- Google Cloud Service Account с доступом к Google Sheets API

## Установка

### 1. Клонирование репозитория

```bash
git clone <repository-url>
cd top_200_products
```

### 2. Создание виртуального окружения

```bash
python -m venv .venv
source .venv/bin/activate  # для Linux/Mac
# или
.venv\Scripts\activate  # для Windows
```

### 3. Установка зависимостей

```bash
pip install -r requirements.txt
```

## Конфигурация

### 1. Настройка переменных окружения

Скопируйте файл `.env.example` в `.env` и заполните необходимые параметры:

```bash
cp .env.example .env
```

#### Основные параметры:

**Режим работы:**
```env
MODE=DEV  # DEV или PROD
```

**PostgreSQL (опционально):**
```env
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

**Redis:**
```env
REDIS_HOST=localhost
REDIS_PORT=6379
```

**Google Sheets:**
```env
GOOGLE_SPREADSHEET_ID=your_spreadsheet_id
PATH_TO_CREDENTIALS=/path/to/conf_for_sheets.json
```

**Ozon API:**
```env
OZON_NAME_LK=Account1,Account2,Account3
OZON_CLIENT_IDS=client_id_1,client_id_2,client_id_3
OZON_API_KEYS=api_key_1,api_key_2,api_key_3
```

**Периоды анализа для теста:**
```env
ANALYTICS_MONTHS='октябрь 2024,ноябрь 2024'
DATE_SINCE=2024-10-28T00:00:00Z
DATE_TO=2024-11-03T23:59:59Z
```

**1С:**
```env
ONEC_HOST=http://your-1c-host
ONEC_LOGIN_PASS=login:password
```

**S3 (для бэкапов):**
```env
BUCKET_NAME=your-bucket
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
```

### 2. Настройка Google Sheets API

#### Создание Service Account:

1. Перейдите в [Google Cloud Console](https://console.cloud.google.com/)
2. Создайте новый проект или выберите существующий
3. Включите Google Sheets API и Google Drive API
4. Создайте Service Account:
   - IAM & Admin → Service Accounts → Create Service Account
   - Дайте ему имя, например "ozon-analytics"
   - Создайте ключ (JSON) и сохраните его
5. Скопируйте JSON файл в проект:
   ```bash
   cp example.conf_for_sheets.json conf_for_sheets.json
   # Замените содержимое на ваш JSON ключ
   ```

#### Настройка доступа к таблице:

1. Создайте Google Spreadsheet
2. Поделитесь таблицей с email вашего Service Account (из JSON файла, поле `client_email`)
3. Скопируйте ID таблицы из URL (между `/d/` и `/edit`):
   ```
   https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit
   ```
4. Добавьте ID в `.env`:
   ```env
   GOOGLE_SPREADSHEET_ID=your_spreadsheet_id
   ```

## Использование

### Запуск основного процесса

```bash
python src/main.py
```

Скрипт выполнит:
1. Сбор данных из всех кабинетов Ozon (постинги, остатки, аналитика)
2. Получение данных из 1С
3. Обработку и агрегацию данных
4. Формирование отчётов в Google Sheets

### Структура выходных данных

В Google Sheets будут созданы листы:

1. **Top Products** - сводная таблица по всем товарам из покабинетных остатков:
   - Артикулы, SKU, наименования
   - Остатки по складам и кластерам
   - Обороты и количество заказов по месяцам
   - Аналитика (посетители, позиции)
   - Себестоимость из 1С

2. **{Account Name}** - вспомогательные таблицы по каждому кабинету:
   - Модель доставки (FBO/FBS)
   - SKU, цена, статус
   - Количество товаров в заявке
   - Остатки по кластерам

## Архитектура

```
src/
├── clients/          # Клиенты для внешних API (Ozon, Google Sheets)
├── domain/           # Доменные модели и репозитории
├── dto/              # Data Transfer Objects
├── infrastructure/   # Кэширование (Redis)
├── mappers/          # Преобразование данных
├── pipeline/         # Пайплайн обработки данных
├── schemas/          # Pydantic схемы
├── services/         # Бизнес-логика (Ozon, 1С, Google Sheets)
└── utils/            # Утилиты (HTTP клиент, лимитеры)
```

## Особенности реализации

### Фильтрация отменённых заказов

Система **автоматически исключает** заказы со статусом `cancelled` при подсчёте:
- Количества заказов по периодам
- Оборота товаров
- Расчёта цены товара

Это обеспечивает корректность данных и исключает искажение статистики отменёнными заказами.

### Кэширование

Данные кэшируются в Redis для ускорения повторных запусков:
- Постинги по периодам и кабинетам
- Остатки товаров
- Аналитика по месяцам
- Данные из 1С

Для сброса кэша используйте Redis CLI или очистите базу данных.

### Множественные кабинеты

Система поддерживает одновременную работу с несколькими кабинетами Ozon.
Указывайте данные через запятую в `.env`:

```env
OZON_NAME_LK=Account1,Account2,Account3
OZON_CLIENT_IDS=id1,id2,id3
OZON_API_KEYS=key1,key2,key3
```

Порядок должен совпадать для всех трёх параметров.

## Troubleshooting

### Проблемы с Google Sheets API

**Ошибка 403 Forbidden:**
- Убедитесь что Service Account имеет доступ к таблице
- Проверьте что включены Google Sheets API и Drive API в проекте

**Ошибка при записи:**
- Проверьте что у Service Account есть права на редактирование таблицы

### Проблемы с Ozon API

**Ошибка 401 Unauthorized:**
- Проверьте правильность `client_id` и `api_key`
- Убедитесь что API ключи активны в личном кабинете Ozon

**Rate limit exceeded:**
- Система использует автоматический rate limiting
- Если ошибка повторяется, увеличьте паузы между запросами

### Проблемы с Redis

**Connection refused:**
```bash
# Запустите Redis
redis-server
```

**Очистка кэша:**
```bash
redis-cli
> FLUSHALL
```

## Разработка

### Запуск 

```bash
python src/new_main.py
```

### Структура данных

Основные DTO находятся в `src/dto/dto.py`:
- `Item` - товар в постинге
- `PostingsByPeriod` - постинги за период
- `SkuInfo` - информация о SKU с остатками
- `ProductsByArticle` - агрегированные данные по артикулу

### Добавление новых полей

1. Обновите DTO в `src/dto/dto.py`
2. Обновите маппинг в `src/mappers/transformation_functions.py`
3. Обновите формирование таблицы в функции `collect_sheets_values`

## Контакты

Для вопросов и предложений обращайтесь к roman_sergeev7680@gmail.com
