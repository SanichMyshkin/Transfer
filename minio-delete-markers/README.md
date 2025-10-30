создание бакета с версионированием

```
mc alias set single http://sanich.tech:8200

mc mb single/test1

mc version enable single/test1

mc ilm ls single/test1

mc ls --versions single/markers/content/vol-41/chap-32    

mc ilm add minio1/nx-delete-markers-denis --noncurrent-expire-days 1 --expire-delete-marker
mc ilm add minio1/test1 --noncurrent-expire-days 1 
mc ilm add minio1/test2 --expire-delete-marker

mc ilm rm single/test1 --all --force

```


# Тест только с expired days

1. Создал блоб и репо с названиями `onlyexpdays`

2. Включил версионирование

```bash
mc version enable single/onlyexpdays

❯ mc version info single/onlyexpdays
single/onlyexpdays versioning is enabled

```

3. Залил файл на `65м`
```bash
sanich@5418105-sanich ~/Transfer/volume/minio-single-data main*
❯ du -sh onlyexpdays
65M     onlyexpdays

sanich@5418105-sanich ~/Transfer/volume/minio-single-data main*
❯ mc ls onlyexpdays 
[2025-10-13 10:38:10 MSK] 4.0KiB content/
[2025-10-13 10:35:15 MSK] 4.0KiB metadata.properties/
```

4. Включаем политику 

```bash
❯ mc ilm add single/onlyexpdays --expire-delete-marker
Lifecycle configuration rule added with ID `d3maqnlchg19nltvmdr0` to single/onlyexpdays.
```

Проверяем что оно правдо появилось 

```bash
❯ mc ilm ls single/onlyexpdays

┌───────────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for latest version (Expiration)                                            │
├──────────────────────┬─────────┬────────┬──────┬────────────────┬─────────────────────┤
│ ID                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ EXPIRE DELETEMARKER │
├──────────────────────┼─────────┼────────┼──────┼────────────────┼─────────────────────┤
│ d3maqnlchg19nltvmdr0 │ Enabled │ -      │ -    │              0 │ true                │
└──────────────────────┴─────────┴────────┴──────┴────────────────┴─────────────────────┘
```

5. Удаляем на стороне нексуса файл запускаем задачу `Admin - Blob store compact`

6. Ожидаем удаление со стороны нексуса, как только мы ключаем версионирование, удаление происходит немного дольше, и это занимает 1-2 часа

7. После того как блоб будет показывать `0`, то время посмтореть наличие делит маркеров на стороне минио.
    Состояние маркеров до удаления
```bash
❯ mc ls --versions single/onlyexpdays/content/vol-03/chap-34
[2025-10-13 10:38:10 MSK]  64MiB STANDARD 5230e694-0b33-4f47-b9d4-c58db15bb9ce v1 PUT 35d93d36-5533-45f0-b165-bfe2ec1d894c.bytes
[2025-10-13 10:38:10 MSK]   400B STANDARD 6d05e0e4-662a-43a3-8edb-3397451b849b v2 PUT 35d93d36-5533-45f0-b165-bfe2ec1d894c.properties
[2025-10-13 10:38:10 MSK]   316B STANDARD 250b1e2b-8b8e-49b0-b12e-cef5c915b830 v1 PUT 35d93d36-5533-45f0-b165-bfe2ec1d894c.properties
```

Состояние, после того как блоб в нексусе был очищен и передал запрос на удаление в `minio`
```bash

```

8. Сколько хрнатяся делит маркеры и как часто происходит удаление их - не известно, предположительно задача запускается раз в сутки  



# Роботоспособность 

До запуска есть 3 бакета

```bash
.venv ❯ mc ilm ls replica/test1
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for older versions (NoncurrentVersionExpiration)                     │
├──────────────────────┬─────────┬────────┬──────┬────────────────┬───────────────┤
│ ID                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ KEEP VERSIONS │
├──────────────────────┼─────────┼────────┼──────┼────────────────┼───────────────┤
│ d3meoatchg1fm8lf3850 │ Enabled │ -      │ -    │              1 │             0 │
└──────────────────────┴─────────┴────────┴──────┴────────────────┴───────────────┘

sanich@5418105-sanich ~/Transfer/volume/minio-replica-data main*
.venv ❯ mc ilm ls replica/test2
┌───────────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for latest version (Expiration)                                            │
├──────────────────────┬─────────┬────────┬──────┬────────────────┬─────────────────────┤
│ ID                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ EXPIRE DELETEMARKER │
├──────────────────────┼─────────┼────────┼──────┼────────────────┼─────────────────────┤
│ d3meob5chg1fmaimd7l0 │ Enabled │ -      │ -    │              0 │ true                │
└──────────────────────┴─────────┴────────┴──────┴────────────────┴─────────────────────┘

sanich@5418105-sanich ~/Transfer/volume/minio-replica-data main*
.venv ❯ mc ilm ls replica/test3
mc: <ERROR> Unable to get lifecycle. The lifecycle configuration does not exist.

sanich@5418105-sanich ~/Transfer/volume/minio-replica-data main*
.venv ❯                        
```

после запуска те же бакеты
```bash 
.venv ❯ /home/sanich/Transfer/minio-delete-markers/.venv/bin/python3.10 /home/sanich/Transfer/minio-delete-markers/main.py
2025-10-13 15:12:32,082 [INFO] Найдено бакетов: 4
2025-10-13 15:12:32,083 [INFO] Режим: LIVE (вносятся изменения)
2025-10-13 15:12:32,083 [INFO] ⏭ Пропускаем бакет nexus-artifacts — не соответствует префиксу test
2025-10-13 15:12:32,083 [INFO] 🔍 Обрабатываем бакет test1
2025-10-13 15:12:32,087 [WARNING] ⚠️ test1: lifecycle правило отсутствует или неполное
2025-10-13 15:12:32,087 [INFO] 🔧 Добавляю missing правила для test1
2025-10-13 15:12:32,102 [INFO] ✅ Lifecycle правило обновлено в test1
2025-10-13 15:12:32,102 [INFO] 🔍 Обрабатываем бакет test2
2025-10-13 15:12:32,105 [WARNING] ⚠️ test2: lifecycle правило отсутствует или неполное
2025-10-13 15:12:32,106 [INFO] 🔧 Добавляю missing правила для test2
2025-10-13 15:12:32,119 [INFO] ✅ Lifecycle правило обновлено в test2
2025-10-13 15:12:32,119 [INFO] 🔍 Обрабатываем бакет test3
2025-10-13 15:12:32,125 [WARNING] ⚠️ test3: lifecycle правило отсутствует или неполное
2025-10-13 15:12:32,125 [INFO] 🔧 Добавляю missing правила для test3
2025-10-13 15:12:32,140 [INFO] ✅ Lifecycle правило обновлено в test3
2025-10-13 15:12:32,141 [INFO] 🧾 Обработка завершена 
====================================================================================================
.venv ❯ 
```
Статусы по бакетов

```
.venv ❯ mc ilm ls replica/test3                                                                                           
┌───────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for latest version (Expiration)                                                            │
├──────────────────────────────────────┬─────────┬────────┬──────┬────────────────┬─────────────────────┤
│ ID                                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ EXPIRE DELETEMARKER │
├──────────────────────────────────────┼─────────┼────────┼──────┼────────────────┼─────────────────────┤
│ 304dfd71-323b-4a23-b045-6a5d3a73e17d │ Enabled │ -      │ -    │              0 │ true                │
└──────────────────────────────────────┴─────────┴────────┴──────┴────────────────┴─────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for older versions (NoncurrentVersionExpiration)                                     │
├──────────────────────────────────────┬─────────┬────────┬──────┬────────────────┬───────────────┤
│ ID                                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ KEEP VERSIONS │
├──────────────────────────────────────┼─────────┼────────┼──────┼────────────────┼───────────────┤
│ 304dfd71-323b-4a23-b045-6a5d3a73e17d │ Enabled │ -      │ -    │              1 │             0 │
└──────────────────────────────────────┴─────────┴────────┴──────┴────────────────┴───────────────┘

sanich@5418105-sanich ~/Transfer/volume/minio-replica-data main*
.venv ❯ mc ilm ls replica/test2                                                                                           
┌───────────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for latest version (Expiration)                                            │
├──────────────────────┬─────────┬────────┬──────┬────────────────┬─────────────────────┤
│ ID                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ EXPIRE DELETEMARKER │
├──────────────────────┼─────────┼────────┼──────┼────────────────┼─────────────────────┤
│ d3meob5chg1fmaimd7l0 │ Enabled │ -      │ -    │              0 │ true                │
└──────────────────────┴─────────┴────────┴──────┴────────────────┴─────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for older versions (NoncurrentVersionExpiration)                                     │
├──────────────────────────────────────┬─────────┬────────┬──────┬────────────────┬───────────────┤
│ ID                                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ KEEP VERSIONS │
├──────────────────────────────────────┼─────────┼────────┼──────┼────────────────┼───────────────┤
│ dbf3aabe-bc2d-4523-bfe2-ea3ec8aba131 │ Enabled │ -      │ -    │              1 │             0 │
└──────────────────────────────────────┴─────────┴────────┴──────┴────────────────┴───────────────┘

sanich@5418105-sanich ~/Transfer/volume/minio-replica-data main*
.venv ❯ mc ilm ls replica/test1                                                                                           
┌───────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for latest version (Expiration)                                                            │
├──────────────────────────────────────┬─────────┬────────┬──────┬────────────────┬─────────────────────┤
│ ID                                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ EXPIRE DELETEMARKER │
├──────────────────────────────────────┼─────────┼────────┼──────┼────────────────┼─────────────────────┤
│ 6bde0cb0-15c2-48ba-b4ea-bfaa29b197f6 │ Enabled │ -      │ -    │              0 │ true                │
└──────────────────────────────────────┴─────────┴────────┴──────┴────────────────┴─────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for older versions (NoncurrentVersionExpiration)                     │
├──────────────────────┬─────────┬────────┬──────┬────────────────┬───────────────┤
│ ID                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ KEEP VERSIONS │
├──────────────────────┼─────────┼────────┼──────┼────────────────┼───────────────┤
│ d3meoatchg1fm8lf3850 │ Enabled │ -      │ -    │              1 │             0 │
└──────────────────────┴─────────┴────────┴──────┴────────────────┴───────────────┘
```



# 🧾 MinIO Lifecycle Fixer

Скрипт автоматически проверяет и при необходимости добавляет **правила жизненного цикла (Lifecycle Rules)** в бакеты MinIO.  
Работает аналогично команде:

```bash
mc ilm add minio1/example --noncurrent-expire-days 1 --expire-delete-marker
```

---

## 📄 Назначение

Скрипт выполняет проверку всех бакетов и:
- добавляет правило для **удаления delete markers** (`expired_object_delete_marker=True`);
- добавляет правило для **удаления старых версий** (`noncurrent_version_expiration(noncurrent_days=1)`).

Если оба правила уже существуют — ничего не делает.  
По умолчанию работает в режиме **dry-run** (только проверка без внесения изменений).

---

## ⚙️ Переменные окружения

```bash
MINIO_ENDPOINT=minio:9000
ACCESS_KEY=minioadmin
SECRET_KEY=minioadmin
PREFIX=example
DRY_RUN=true
```

---

## 🧩 Логирование

Логи записываются в файл:

```
logs/minio-delete-markers.log
```

и одновременно выводятся в консоль.

Используется ротация логов:
- создаётся новый файл каждый день в полночь;
- хранится 7 последних логов.

---

## 🔍 Основная логика работы

1. Загружает переменные окружения из `.env`.
2. Подключается к MinIO с указанными параметрами.
3. Получает список всех бакетов.
4. Проверяет lifecycle-конфигурацию:
   - наличие `expired_object_delete_marker=True`;
   - наличие `noncurrent_version_expiration(noncurrent_days=1)`.
5. Если чего-то не хватает:
   - сообщает об этом в dry-run режиме;
   - добавляет правила при `DRY_RUN=false`.
6. Сохраняет обновлённую конфигурацию обратно в MinIO.

---

## 🧠 Пример вывода

```text
2025-10-30 12:01:23 [INFO] Найдено бакетов: 3
2025-10-30 12:01:23 [INFO] Режим: 🧪 DRY-RUN (только проверка)
2025-10-30 12:01:23 [INFO] 🔍 Обрабатываем бакет example-bucket
2025-10-30 12:01:23 [WARNING] ⚠️ example-bucket: lifecycle правило отсутствует или неполное
2025-10-30 12:01:23 [INFO] 🧪 [DRY RUN] Добавил бы lifecycle правило для example-bucket
2025-10-30 12:01:23 [INFO] 🧾 Обработка завершена ==================================================
```

---

## 🧩 Зависимости

```bash
pip install minio python-dotenv
```

---

## 📦 Структура проекта

```
minio-delete-markers.py
.env
logs/
 └── minio-delete-markers.log
```

---

## 🔚 Итог

Скрипт предназначен для автоматического поддержания корректных lifecycle-политик в MinIO.  
Он безопасно проверяет конфигурации, при необходимости добавляет недостающие правила и ведёт полное логирование действий.
