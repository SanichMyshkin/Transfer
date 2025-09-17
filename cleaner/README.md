# Документация по проекту очистки Nexus Repositories

<details>
<summary>Архитектура и устройство</summary>

## Структура проекта

```
project_root/
│── main.py               # Точка входа, запуск программы
│── common.py             # Общие функции: загрузка конфигов, логирование, правила
│── repository.py         # Работа с репозиториями: raw, docker, вызовы API Nexus
│── maven.py              # Специализированная логика очистки для Maven
│── requirements.txt      # Зависимости проекта
│── configs/              # Папка с YAML-конфигами
│── logs/                 # Папка для логов (чистить не нужно)
│── .env                  # Файл с переменными окржуения
```

---

## `main.py`

Точка входа. Основные задачи:

- Сканирует папку `configs/` и подкаталоги на наличие `.yaml` файлов.
- Загружает конфиги с помощью `load_config` из `common.py`.
- Для каждого репозитория вызывает функцию `clear_repository` из `repository.py`.

Ключевая функция:

- **main()** – управляет процессом очистки.

---

## `common.py`

Общий модуль. В нём находятся:

- **Логирование** (ротация логов по дням, хранение до 7 файлов).
- **load_config(path)** – загрузка и парсинг YAML-файлов конфигурации.
- **get_matching_rule(...)** – определение правил хранения артефактов по регулярным выражениям или настройкам "по умолчанию".

---

## `repository.py`

Модуль для работы с репозиториями Nexus (raw, docker, maven).

Функции:

- **get_repository_format(repo_name)** – определяет формат репозитория (`raw`, `docker`, `maven2`).
- **get_repository_items(repo_name, repo_format)** – получает список артефактов или компонентов из Nexus API.
- **convert_raw_assets_to_components(assets)** – преобразует `raw` ассеты в компоненты (name + version).
- **delete_component(id, name, version, dry_run, use_asset)** – удаляет компонент или ассет из Nexus.
- **filter_components_to_delete(components, rules, ...)** – отбирает, что нужно удалить (по retention, reserved, last download).
- **clear_repository(repo_name, cfg)** – управляющая функция очистки репозитория.  
  Работает так:
  1. Определяет формат репозитория.
  2. Получает список элементов через API.
  3. В зависимости от формата применяет соответствующий фильтр (`filter_components_to_delete` или `filter_maven_components_to_delete`).
  4. Вызывает `delete_component` для удаления лишних артефактов.

---

## `maven.py`

Модуль для обработки **Maven-репозиториев**.

Функции:

- **detect_maven_type(component)** – определяет тип артефакта (`snapshot` или `release`).
- **filter_maven_components_to_delete(components, rules)** – фильтрует список компонентов Maven по правилам:
  - retention_days (возраст хранения),
  - reserved (количество последних версий для хранения),
  - min_days_since_last_download (защита от удаления недавно скачанных).

---

## `configs/`

Содержит YAML-файлы с правилами очистки. В каждом файле можно описать:

- `repo_names` – список репозиториев для очистки.
- `regex_rules` – правила очистки по маскам версий.
- `no_match_retention_days` – сколько хранить артефактов, если версия не подходит ни под одно правило.
- `no_match_reserved` – сколько последних версий хранить.
- `no_match_min_days_since_last_download` – сколько дней ждать после последней загрузки, прежде чем удалять.
- `maven_rules` – отдельные правила для `snapshot` и `release`.

---

## `logs/`

Папка с логами.  

- Логи ведутся в файл `logs/cleaner.log` и ротируются по дням.
- В логах фиксируется:
  - старт/завершение обработки репозиториев,
  - количество найденных и удалённых компонентов,
  - ошибки при запросах к API,
  - пропуски при dry-run.

---

## Взаимодействие модулей

```
main.py
  │
  ├── common.py
  │     ├── load_config()
  │     └── get_matching_rule()
  │
  └── repository.py
        ├── get_repository_format()
        ├── get_repository_items()
        ├── convert_raw_assets_to_components()
        ├── filter_components_to_delete()
        ├── delete_component()
        └── clear_repository()
              │
              └── maven.py (для maven2)
                     ├── detect_maven_type()
                     └── filter_maven_components_to_delete()
```

---
</details>

<details>
<summary>Руководство по использованию</summary>

# Пользовательская документация

## Инструкция по настройке конфигурации для очистки Nexus-репозиториев

Файл конфигурации (`.yaml`) описывает правила, по которым скрипт будет определять, какие компоненты можно удалить, а какие — сохранить.

> Поддерживаются репозитории форматов:

- **`docker`**
- **`raw`**
- **`maven`**

---

## Пример файла конфигурации

```yaml
repo_names:
  - test-docker
  - test-raw
  - test-maven

regex_rules:
  "^dev-":
    retention_days: 5
    reserved: 2
  "^release-.*":
    retention_days: 15
    reserved: 3
    min_days_since_last_download: 7

no_match_retention_days: 10
no_match_reserved: 1
no_match_min_days_since_last_download: 21

maven_rules:
  snapshot:
    regex_rules:
      ".*-SNAPSHOT":
        retention_days: 7
        reserved: 2
    no_match_retention_days: 14
    no_match_reserved: 1

  release:
    regex_rules:
      ".*":
        retention_days: 30
        reserved: 5
    no_match_retention_days: 60
    no_match_reserved: 2

dry_run: true
```

---

## Описание параметров

| Поле                                    | Описание                                                                                 |
| --------------------------------------- | ---------------------------------------------------------------------------------------- |
| `repo_names`                            | Список репозиториев (`docker`, `raw` или `maven`), в которых будет производиться очистка |
| `regex_rules`                           | Словарь с шаблонами версий (регулярные выражения)                                        |
| `retention_days`                        | Срок хранения для совпадающих по regex компонентов                                       |
| `reserved`                              | Количество последних компонентов, которые нельзя удалять                                 |
| `min_days_since_last_download`          | Минимальное число дней с последнего скачивания                                           |
| `no_match_retention_days`               | Срок хранения, если не совпадает ни с одним regex                                        |
| `no_match_reserved`                     | Количество последних компонентов без совпадений, которые нужно оставить                  |
| `no_match_min_days_since_last_download` | Минимальные дни с последнего скачивания без совпадений                                   |
| `dry_run`                               | `true` — только логирование, без удаления                                                |
| `maven_rules`                           | Специальный блок правил для Maven (`snapshot` и `release`)                               |

---

## Приоритет применения правил

| Приоритет | Параметр                                                                 | Где применяется            | Условие применения                                          |
| --------- | ------------------------------------------------------------------------ | -------------------------- | ----------------------------------------------------------- |
| 1         | `reserved` / `no_match_reserved`                                         | Внутри правила / глобально | **Сохраняется** N самых новых компонентов                   |
| 2         | `min_days_since_last_download` / `no_match_min_days_since_last_download` | Внутри правила / глобально | **Не удаляется**, если скачан менее X дней назад            |
| 2         | `retention_days` / `no_match_retention_days`                             | Внутри правила / глобально | Удаляется, если старше срока и не защищён другими условиями |

> ❗ Тег `latest` **никогда не удаляется**

---

## Поведение при разных комбинациях параметров

| #   | retention_days | reserved | min_days_since_last_download | no_match_retention_days | no_match_reserved | no_match_min_days_since_last_download | Поведение для MATCH                                                                           | Поведение для NO-MATCH                                                                                                   |
| --- | -------------- | -------- | ---------------------------- | ----------------------- | ----------------- | ------------------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| 1   | ✅              | ✅        | ✅                            | ✅                       | ✅                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит |
| 2   | ✅              | ✅        | ✅                            | ✅                       | ✅                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention; иначе удалит                            |
| 3   | ✅              | ✅        | ✅                            | ✅                       | ❌                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит | Без reserved: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит                            |
| 4   | ✅              | ✅        | ✅                            | ✅                       | ❌                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит | Без reserved: сохранит если age ≤ no_match_retention; иначе удалит                                                       |
| 5   | ✅              | ✅        | ✅                            | ❌                       | ✅                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 6   | ✅              | ✅        | ✅                            | ❌                       | ✅                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 7   | ✅              | ✅        | ✅                            | ❌                       | ❌                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит | Без любых правил no-match: сохранит если dl ≤ no_match_min_days                                                                               |
| 8   | ✅              | ✅        | ✅                            | ❌                       | ❌                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит | Без любых правил no-match: все сохраняются                                                                               |
| 9   | ✅              | ✅        | ❌                            | ✅                       | ✅                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention; иначе удалит                   | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит |
| 10  | ✅              | ✅        | ❌                            | ✅                       | ✅                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention; иначе удалит                   | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention; иначе удалит                            |
| 11  | ✅              | ✅        | ❌                            | ✅                       | ❌                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention; иначе удалит                   | Без reserved: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит                            |
| 12  | ✅              | ✅        | ❌                            | ✅                       | ❌                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention; иначе удалит                   | Без reserved: сохранит если age ≤ no_match_retention; иначе удалит                                                       |
| 13  | ✅              | ✅        | ❌                            | ❌                       | ✅                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention; иначе удалит                   | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 14  | ✅              | ✅        | ❌                            | ❌                       | ✅                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention; иначе удалит                   | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 15  | ✅              | ✅        | ❌                            | ❌                       | ❌                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention; иначе удалит                   | Без любых правил no-match: все сохраняются                                                                               |
| 16  | ✅              | ✅        | ❌                            | ❌                       | ❌                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если age ≤ retention; иначе удалит                   | Без любых правил no-match: все сохраняются                                                                               |
| 17  | ✅              | ❌        | ✅                            | ✅                       | ✅                 | ✅                                     | Без reserved: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит                   | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит |
| 18  | ✅              | ❌        | ✅                            | ✅                       | ✅                 | ❌                                     | Без reserved: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит                   | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention; иначе удалит                            |
| 19  | ✅              | ❌        | ✅                            | ✅                       | ❌                 | ✅                                     | Без reserved: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит                   | Без reserved: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит                            |
| 20  | ✅              | ❌        | ✅                            | ✅                       | ❌                 | ❌                                     | Без reserved: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит                   | Без reserved: сохранит если age ≤ no_match_retention; иначе удалит                                                       |
| 21  | ✅              | ❌        | ✅                            | ❌                       | ✅                 | ✅                                     | Без reserved: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит                   | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 22  | ✅              | ❌        | ✅                            | ❌                       | ✅                 | ❌                                     | Без reserved: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит                   | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 23  | ✅              | ❌        | ✅                            | ❌                       | ❌                 | ✅                                     | Без reserved: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит                   | Без любых правил no-match: все сохраняются                                                                               |
| 24  | ✅              | ❌        | ✅                            | ❌                       | ❌                 | ❌                                     | Без reserved: сохранит если age ≤ retention или dl ≤ min_days; иначе удалит                   | Без любых правил no-match: все сохраняются                                                                               |
| 25  | ✅              | ❌        | ❌                            | ✅                       | ✅                 | ✅                                     | Без reserved: сохранит если age ≤ retention; иначе удалит                                     | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит |
| 26  | ✅              | ❌        | ❌                            | ✅                       | ✅                 | ❌                                     | Без reserved: сохранит если age ≤ retention; иначе удалит                                     | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention; иначе удалит                            |
| 27  | ✅              | ❌        | ❌                            | ✅                       | ❌                 | ✅                                     | Без reserved: сохранит если age ≤ retention; иначе удалит                                     | Без reserved: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит                            |
| 28  | ✅              | ❌        | ❌                            | ✅                       | ❌                 | ❌                                     | Без reserved: сохранит если age ≤ retention; иначе удалит                                     | Без reserved: сохранит если age ≤ no_match_retention; иначе удалит                                                       |
| 29  | ✅              | ❌        | ❌                            | ❌                       | ✅                 | ✅                                     | Без reserved: сохранит если age ≤ retention; иначе удалит                                     | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 30  | ✅              | ❌        | ❌                            | ❌                       | ✅                 | ❌                                     | Без reserved: сохранит если age ≤ retention; иначе удалит                                     | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 31  | ✅              | ❌        | ❌                            | ❌                       | ❌                 | ✅                                     | Без reserved: сохранит если age ≤ retention; иначе удалит                                     | Без любых правил no-match: все сохраняются                                                                               |
| 32  | ✅              | ❌        | ❌                            | ❌                       | ❌                 | ❌                                     | Без reserved: сохранит если age ≤ retention; иначе удалит                                     | Без любых правил no-match: все сохраняются                                                                               |
| 33  | ❌              | ✅        | ✅                            | ✅                       | ✅                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если dl ≤ min_days; иначе удалит                     | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит |
| 34  | ❌              | ✅        | ✅                            | ✅                       | ✅                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если dl ≤ min_days; иначе удалит                     | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention; иначе удалит                            |
| 35  | ❌              | ✅        | ✅                            | ✅                       | ❌                 | ✅                                     | Оставит top-`reserved`; прочие: сохранит если dl ≤ min_days; иначе удалит                     | Без reserved: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит                            |
| 36  | ❌              | ✅        | ✅                            | ✅                       | ❌                 | ❌                                     | Оставит top-`reserved`; прочие: сохранит если dl ≤ min_days; иначе удалит                     | Без reserved: сохранит если age ≤ no_match_retention; иначе удалит                                                       |
| 37  | ❌              | ✅        | ✅                            | ❌                       | ✅                 | ✅                                     | Оставит top-`reserved`; без retention: прочие сохранятся если dl ≤ min_days; иначе удалит     | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 38  | ❌              | ✅        | ✅                            | ❌                       | ✅                 | ❌                                     | Оставит top-`reserved`; без retention: прочие сохранятся если dl ≤ min_days; иначе удалит     | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 39  | ❌              | ✅        | ✅                            | ❌                       | ❌                 | ✅                                     | Оставит top-`reserved`; без retention: прочие сохранятся если dl ≤ min_days; иначе удалит     | Без любых правил no-match: все сохраняются                                                                               |
| 40  | ❌              | ✅        | ✅                            | ❌                       | ❌                 | ❌                                     | Оставит top-`reserved`; без retention: прочие сохранятся если dl ≤ min_days; иначе удалит     | Без любых правил no-match: все сохраняются                                                                               |
| 41  | ❌              | ✅        | ❌                            | ✅                       | ✅                 | ✅                                     | Оставит top-`reserved`; прочие: удалит (нет retention/min_days)                               | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит |
| 42  | ❌              | ✅        | ❌                            | ✅                       | ✅                 | ❌                                     | Оставит top-`reserved`; прочие: удалит                                                        | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention; иначе удалит                            |
| 43  | ❌              | ✅        | ❌                            | ✅                       | ❌                 | ✅                                     | Оставит top-`reserved`; прочие: удалит                                                        | Без reserved: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит                            |
| 44  | ❌              | ✅        | ❌                            | ✅                       | ❌                 | ❌                                     | Оставит top-`reserved`; прочие: удалит                                                        | Без reserved: сохранит если age ≤ no_match_retention; иначе удалит                                                       |
| 45  | ❌              | ✅        | ❌                            | ❌                       | ✅                 | ✅                                     | Оставит top-`reserved`; прочие: удалит                                                        | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 46  | ❌              | ✅        | ❌                            | ❌                       | ✅                 | ❌                                     | Оставит top-`reserved`; прочие: удалит                                                        | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 47  | ❌              | ✅        | ❌                            | ❌                       | ❌                 | ✅                                     | Оставит top-`reserved`; прочие: удалит                                                        | Без любых правил no-match: все сохраняются                                                                               |
| 48  | ❌              | ✅        | ❌                            | ❌                       | ❌                 | ❌                                     | Оставит top-`reserved`; прочие: удалит                                                        | Без любых правил no-match: все сохраняются                                                                               |
| 49  | ❌              | ❌        | ✅                            | ✅                       | ✅                 | ✅                                     | Без reserved: сохранит если dl ≤ min_days; иначе удалит                                       | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит |
| 50  | ❌              | ❌        | ✅                            | ✅                       | ✅                 | ❌                                     | Без reserved: сохранит если dl ≤ min_days; иначе удалит                                       | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention; иначе удалит                            |
| 51  | ❌              | ❌        | ✅                            | ✅                       | ❌                 | ✅                                     | Без reserved: сохранит если dl ≤ min_days; иначе удалит                                       | Без reserved: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит                            |
| 52  | ❌              | ❌        | ✅                            | ✅                       | ❌                 | ❌                                     | Без reserved: сохранит если dl ≤ min_days; иначе удалит                                       | Без reserved: сохранит если age ≤ no_match_retention; иначе удалит                                                       |
| 53  | ❌              | ❌        | ✅                            | ❌                       | ✅                 | ✅                                     | Без reserved: сохранит если dl ≤ min_days; иначе удалит                                       | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 54  | ❌              | ❌        | ✅                            | ❌                       | ✅                 | ❌                                     | Без reserved: сохранит если dl ≤ min_days; иначе удалит                                       | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 55  | ❌              | ❌        | ✅                            | ❌                       | ❌                 | ✅                                     | Без reserved: сохранит если dl ≤ min_days; иначе удалит                                       | Без любых правил no-match: все сохраняются                                                                               |
| 56  | ❌              | ❌        | ✅                            | ❌                       | ❌                 | ❌                                     | Без reserved: сохранит если dl ≤ min_days; иначе удалит                                       | Без любых правил no-match: все сохраняются                                                                               |
| 57  | ❌              | ❌        | ❌                            | ✅                       | ✅                 | ✅                                     | Нет правил match → нет фильтрации                                                             | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит |
| 58  | ❌              | ❌        | ❌                            | ✅                       | ✅                 | ❌                                     | Нет правил match → нет фильтрации                                                             | Оставит top-`no_match_reserved`; прочие: сохранит если age ≤ no_match_retention; иначе удалит                            |
| 59  | ❌              | ❌        | ❌                            | ✅                       | ❌                 | ✅                                     | Нет правил match → нет фильтрации                                                             | Без reserved: сохранит если age ≤ no_match_retention или dl ≤ no_match_min_days; иначе удалит                            |
| 60  | ❌              | ❌        | ❌                            | ✅                       | ❌                 | ❌                                     | Нет правил match → нет фильтрации                                                             | Без reserved: сохранит если age ≤ no_match_retention; иначе удалит                                                       |
| 61  | ❌              | ❌        | ❌                            | ❌                       | ✅                 | ✅                                     | Нет правил match → нет фильтрации                                                             | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 62  | ❌              | ❌        | ❌                            | ❌                       | ✅                 | ❌                                     | Нет правил match → нет фильтрации                                                             | Оставит top-`no_match_reserved`; без retention/min_days: остальные удалит                                                |
| 63  | ❌              | ❌        | ❌                            | ❌                       | ❌                 | ✅                                     | Нет правил match → нет фильтрации                                                             | Без любых правил no-match: все сохраняются                                                                               |
| 64  | ❌              | ❌        | ❌                            | ❌                       | ❌                 | ❌                                     | Нет правил match → нет фильтрации                                                             | Без любых правил no-match: все сохраняются                                                                               |  |

> ✅ - параметр задан  
> ❌ - параметр не задан

---

## Логика выбора правила по регулярке

Для определения правил хранения компонентов используется список регулярных выражений (`regex_rules`), сопоставляемых с версией компонента.

Если версия соответствует нескольким выражениям, применяется **одно** — с наибольшей длиной шаблона. Это считается более специфичным совпадением.

# Особенности фильтрации компонентов

Скрипт поддерживает **разные схемы фильтрации** в зависимости от типа репозитория:

| Тип репозитория | Что используется как имя компонента | Что считается версией компонента                |
| --------------- | ----------------------------------- | ----------------------------------------------- |
| Docker          | `image:tag`                         | `tag` (тег Docker-образа)                       |
| RAW             | `путь`                              | `имя файла` (последний сегмент)                 |
| Maven           | `groupId:artifactId`                | `version` (например `1.0.0` или `1.0-SNAPSHOT`) |

---

## Docker

- **Имя компонента**: строка в формате `название_образа:тег`  
  Пример: `my-backend:dev-2025.08.01`

- **Для фильтрации** используется только часть **`тег`**, так как именно он отражает версионность компонента.  
  Название образа (например, `my-backend`) не участвует в проверке регулярных выражений.

- Регулярные выражения из `regex_rules` применяются к тегам Docker-образов.

---

## RAW

- **Имя компонента**: путь до файла внутри репозитория  
  Пример: `my-app/releases/1.2.3/build.zip`

- **Для фильтрации** используется только **имя файла** — последняя часть пути (в примере: `build.zip` или `1.2.3.zip` в зависимости от структуры).

- Регулярные выражения из `regex_rules` применяются к имени файла, а не к полному пути.

---

## ☕ Maven

- Поддерживаются два типа компонентов:  
  - **Snapshot** (например, `1.0-SNAPSHOT` или `1.0-20250829.123456-1`)  
  - **Release** (например, `1.0.0`, `2.3.4`)  

- Для каждого типа можно задать отдельные правила внутри блока `maven_rules`:
  - `snapshot:` → настройки для snapshot-компонентов  
  - `release:` → настройки для релизов

- **Имя компонента**: `groupId:artifactId`  
- **Версия**: `version` (например, `1.0-SNAPSHOT`, `2.3.0`)  
- Регулярные выражения из `regex_rules` внутри `maven_rules` применяются именно к версии.

---

# Примеры

### Docker

```yaml
regex_rules:
  "^dev-":
    retention_days: 5
```

Теги: `dev-1`, `dev-2`, `prod-1`  
→ Подойдут под правило только `dev-1`, `dev-2`.

---

### RAW

Файл: `projects/my-lib/versions/dev-1.0.0.zip`  
→ Для фильтрации используется `dev-1.0.0.zip`  
→ Если регулярка такая: `"^dev-"` — файл попадёт под правило.

---

### Maven

Версия: `my.group:my-artifact:1.0-SNAPSHOT`  
→ Определяется как **snapshot**  
→ Сравнивается с регулярками внутри `maven_rules.snapshot.regex_rules`

---

## ⚠️ Важно

**Паттерны не должны пересекаться.**  
Использование похожих или пересекающихся регулярных выражений — **крайне нежелательно**.  
Это приводит к непредсказуемым результатам, особенно если шаблоны одинаковой длины. Подобная конфигурация должна использоваться только в исключительных случаях.

---

# Пример работы очистки репозитория

## Компоненты

```text
release-1.0 (45 дней назад)
release-1.1 (30 дней назад)
release-1.2 (20 дней назад)
release-1.3 (10 дней назад)
release-1.4 (5 дней назад)
release-1.5 (2 дня назад)
```

## 🔧 Конфигурация

```yaml
regex_rules:
  "^release-":
    retention_days: 15
    reserved: 3
    min_days_since_last_download: 7
```

## Последние скачивания

| Компонент   | Последнее скачивание (дн. назад) |
| ----------- | -------------------------------- |
| release-1.0 | 50                               |
| release-1.1 | 40                               |
| release-1.2 | 15                               |
| release-1.3 | 5                                |
| release-1.4 | 3                                |
| release-1.5 | 1                                |

---

## Результат

| Компонент   | Возраст (дн.) | В reserved? | Старше retention? | Скачивали недавно? | Итог                                |
| ----------- | ------------- | ----------- | ----------------- | ------------------ | ----------------------------------- |
| release-1.0 | 45            | ❌           | ✅                 | ❌                  | ❌ Удаляется (старый и не скачивали) |
| release-1.1 | 30            | ❌           | ✅                 | ❌                  | ❌ Удаляется (старый и не скачивали) |
| release-1.2 | 20            | ❌           | ✅                 | ❌                  | ❌ Удаляется (старый и не скачивали) |
| release-1.3 | 10            | ✅           | ❌                 | ✅                  | ✅ Сохраняется (в reserved)          |
| release-1.4 | 5             | ✅           | ❌                 | ✅                  | ✅ Сохраняется (в reserved)          |
| release-1.5 | 2             | ✅           | ❌                 | ✅                  | ✅ Сохраняется (в reserved)          |

---

## Выводы

- `reserved: 3` → защищает `release-1.3`, `1.4`, `1.5`, **независимо от возраста и активности**.
- Остальные проверяются по:
  - `retention_days: 15`
  - `min_days_since_last_download: 7`
- Всё, что **старше 15 дней и не скачивалось более 7 дней**, — **удаляется**.

</details>
