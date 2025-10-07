# Документация по расширенному функционалу сэмплинга

## Обзор

Модуль `make_sample.py` теперь поддерживает расширенную фильтрацию языков при создании выборок, включая:
- Фильтрация по макроареалам
- Фильтрация по языкам документации (Sources' Languages)
- Фильтрация по WALS фичам
- Фильтрация по Grambank фичам
- Обязательное включение языков
- Исключение языков

## Использование

### Базовый пример

```python
from make_sample import GenusSample

# Создание простой выборки
sampler = GenusSample()
result = sampler.genus_sample()

print(f"Выбрано языков: {len(result.languages)}")
print(f"Охвачено родов: {len(result.included_genera)}")
```

### Фильтрация по макроареалам

```python
sampler = GenusSample(
    macroareas=['Africa', 'Eurasia']
)
result = sampler.primary_sample(sample_size=50)
```

### Фильтрация по языкам документации

```python
# Только языки с английской документацией
sampler = GenusSample(
    doc_languages=['eng']
)
result = sampler.core_sample()

# Несколько языков документации (OR логика - любой из них)
sampler = GenusSample(
    doc_languages=['eng', 'rus', 'fra']
)
result = sampler.genus_sample()
```

### Фильтрация по WALS фичам

```python
sampler = GenusSample(
    wals_features={
        '1A': ['2.0', '3.0'],  # Consonant Inventories: Moderately small or Average
        '81A': ['1.0']         # Word Order: SOV
    }
)
result = sampler.core_sample()
```

### Фильтрация по Grambank фичам

```python
sampler = GenusSample(
    grambank_features={
        'GB020': ['1'],  # Has definite/specific articles
        'GB021': ['1']   # Has indefinite articles
    }
)
result = sampler.genus_sample()
```

### Комбинированные фильтры

```python
sampler = GenusSample(
    macroareas=['Africa', 'Eurasia'],
    doc_languages=['eng'],
    wals_features={
        '81A': ['1.0']  # SOV order
    },
    grambank_features={
        'GB020': ['1']  # Has definite articles
    },
    include_languages=['stan1293', 'russ1263'],  # Обязательно включить
    exclude_languages=['fren1241']  # Исключить
)
result = sampler.primary_sample(sample_size=30)
```

## Методы сэмплинга

### 1. `genus_sample()`
Один язык из каждого рода.

```python
result = sampler.genus_sample()
```

### 2. `core_sample()`
Один язык из каждого рода с пригодными источниками данных.

```python
result = sampler.core_sample()
```

### 3. `restricted_sample()`
Подвыборка с тем же распределением генеалогического разнообразия, что и макроареалы.

```python
result = sampler.restricted_sample()
```

### 4. `primary_sample(sample_size)`
Выборка заданного размера с равным генеалогическим разнообразием в каждом макроареале.

```python
result = sampler.primary_sample(sample_size=100)
```

## SamplingResult

Результат сэмплинга возвращается как объект `SamplingResult`:

```python
result = sampler.primary_sample(sample_size=50)

# Доступ к языкам
for language in result.languages:
    print(f"{language.name} ({language.glottocode})")

# Доступ к родам
for genus in result.included_genera:
    print(genus.name)

# Статистика
summary = result.summary()
print(summary)

# Расширение выборки
result2 = sampler2.primary_sample(sample_size=30)
extended = result.extend_sample(result2)
```

## Вспомогательные функции (database.py)

### Получение списка фичей

```python
from database import get_all_features, get_feature_values

# Все WALS фичи
wals_features = get_all_features(source='WALS')

# Все Grambank фичи
grambank_features = get_all_features(source='Grambank')

# Все фичи
all_features = get_all_features()
```

### Получение значений фичи

```python
from database import get_feature_values

values = get_feature_values('1A')
for value in values:
    print(f"{value.value_code}: {value.value_name}")
```

### Получение фичей языка

```python
from database import get_language_features

features = get_language_features('stan1293')  # glottocode русского
print(features)  # {'1A': '3.0', '2A': '2.0', ...}
```

### Поиск языков по фиче

```python
from database import get_languages_with_feature_value

# Найти все языки с SOV порядком слов
languages = get_languages_with_feature_value('81A', '1.0')
```

### Работа с языками документации

```python
from database import (
    get_document_languages,
    get_languages_with_doc_language,
    get_all_document_language_codes
)

# Получить языки документации для конкретного языка
doc_langs = get_document_languages('stan1293')
print(doc_langs)  # ['eng', 'rus']

# Найти все языки с документацией на английском
languages = get_languages_with_doc_language('eng')

# Получить список всех доступных языков документации
all_doc_langs = get_all_document_language_codes()
print(all_doc_langs)  # ['eng', 'rus', 'fra', 'deu', ...]
```

## Интеграция с Flask

API endpoints для получения списка фичей и языков документации:

```
GET /api/features/wals
GET /api/features/grambank
GET /api/document-languages
```

Пример ответа `/api/document-languages`:
```json
["ara", "deu", "eng", "fra", "rus", "spa"]
```

Пример ответа `/api/features/wals`:
```json
[
  {
    "code": "1A",
    "name": "Consonant Inventories",
    "values": [
      {"code": "1.0", "name": "Small"},
      {"code": "2.0", "name": "Moderately small"},
      {"code": "3.0", "name": "Average"}
    ]
  }
]
```

## Формат данных для веб-формы

### WALS/Grambank фичи
Формат: `feature_code:value_code`

Пример:
```
grambank[]: GB020:1
grambank[]: GB021:1
wals[]: 1A:2.0
wals[]: 1A:3.0
```

### Языки для включения/исключения
Формат: список glottocode

Пример:
```
include[]: stan1293
include[]: russ1263
exclude[]: fren1241
```

### Макроареалы
Формат: список названий

Пример:
```
macroareas[]: Africa
macroareas[]: Eurasia
```

### Языки документации
Формат: список ISO 639-3 кодов

Пример:
```
docLanguages[]: eng
docLanguages[]: rus
```

## Примеры использования

См. файл `demo_make_sample.ipynb` для интерактивных примеров использования всех функций.
