создание бакета с версионированием

```
mc alias set single http://sanich.tech:8200

mc mb single/test1

mc version enable single/test1

mc ilm ls single/test1

mc ls --versions single/markers/content/vol-41/chap-32    

mc ilm add minio1/nx-delete-markers-denis --noncurrent-expire-days 1 --expire-delete-marker

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
