# Сбор метрик через audit.log

Суть в следующем по адресу `volume/nexus-data/log/audit` - хранятся все обращения/запуски задач причем достаточно подробно. Если брать этот метод то необходимо обращаться нарпямую в волум(НЕ безопасно) или же делать выгрузку этого лога куда-либо и уже забирать логи от туда и формировать на их основе метрки.

# Сбор метрик из бд

Сейчас патчится база, то есть у нас фактические метрики просто напросто не попадают в бд, они делятся на 1000, что бы сохранялась работоспособность Nexus. Следовательно даже если брать значение метрик которые дает сам нексус, мы получим кашу, так как если нас за день посетили 899 человек, метрика отдаст 0, что вообще является не точным
Можно перед патчингом вставлять фактические значения в таблицы дублеры, которые никак не будет использовать нексус, таким образм можно сохранить все необходимые метрики и выгружать их с помощью скриптов

сейчас ситуация следующая:

Создается функция которая делит фактический результат на 1000

```sql
CREATE OR REPLACE FUNCTION change_metrics_log()
RETURNS TRIGGER AS $$
BEGIN
    NEW.metric_value = NEW.metric_value / 1000;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Затем применяеются тригеры на две таблицы `metrics_log`
```sql
CREATE TRIGGER trg_divide_metrics_log
BEFORE INSERT ON METRICS_LOG
FOR EACH ROW
EXECUTE FUNCTION change_metrics_log();
```
А так же на таблицу `aggregated_metrics`

```sql
CREATE TRIGGER trg_divide_aggmetrics_log
BEFORE INSERT ON aggregated_metrics
FOR EACH ROW
EXECUTE FUNCTION change_metrics_log();
```

Следовательно необходимо создать таблицы для наших метрик и обновить функцию которая будет вставлять фактические метрики

Тригеры менять смысла нет так как они настроены на ту же самую функцию, то есть нам нужно изменить только ее

### Пошаговое изменение


```bash
docker exec -it postgres /bin/bash
```

Заходим под нужным пользователем

```bash
psql -U nexus -d nexus
```

Cоздаем таблицу `metrics_log_raw`

```sql
CREATE TABLE metrics_log_raw (
    metric_id INT,                   -- копия metric_id из оригинальной таблицы
    metric_name VARCHAR,             -- копия metric_name
    metric_value BIGINT,             -- оригинальное значение до деления
    metric_date TIMESTAMPTZ,         -- дата/время метрики
    node_id VARCHAR(256)             -- node_id
);
```
создаем таблицу `aggregated_metrics_raw`

```sql
CREATE TABLE aggregated_metrics_raw (
    aggregated_metric_id SERIAL PRIMARY KEY,       -- int4 + автоувеличение
    metric_name          VARCHAR NOT NULL,         -- имя метрики
    metric_value         BIGINT NOT NULL,          -- значение метрики
    metric_date          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- дата метрики
    period_type          VARCHAR NOT NULL,         -- тип периода (день, месяц и т.д.)
    node_id              VARCHAR                   -- идентификатор узла (может быть NULL)
);


```


Обновляем функцию, которая сначала запишет фактические значения в табицу `metrics_log_raw` и `aggregated_metrics_raw`

```sql
DROP TRIGGER IF EXISTS trg_divide_metrics_log ON metrics_log;
DROP TRIGGER IF EXISTS trg_divide_aggmetrics_log ON aggregated_metrics;

DROP FUNCTION IF EXISTS change_metrics_log();
```

```sql
CREATE OR REPLACE FUNCTION change_metrics_log()
RETURNS TRIGGER AS $$
BEGIN
    -- Определяем, откуда пришла вставка
    IF TG_TABLE_NAME = 'metrics_log' THEN
        INSERT INTO metrics_log_raw (
            metric_id,
            metric_name,
            metric_value,
            metric_date,
            node_id
        )
        VALUES (
            NEW.metric_id,
            NEW.metric_name,
            NEW.metric_value,
            NEW.metric_date,
            NEW.node_id
        );

    ELSIF TG_TABLE_NAME = 'aggregated_metrics' THEN
        INSERT INTO aggregated_metrics_raw (
            aggregated_metric_id,
            metric_name,
            metric_value,
            metric_date,
            period_type,
            node_id
        )
        VALUES (
            NEW.aggregated_metric_id,
            NEW.metric_name,
            NEW.metric_value,
            NEW.metric_date,
            NEW.period_type,
            NEW.node_id
        );

        -- изменяем значение для Nexus
        NEW.metric_value := NEW.metric_value / 1000;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Создаем тригеры заного 

```sql
CREATE TRIGGER trg_divide_metrics_log
BEFORE INSERT ON metrics_log
FOR EACH ROW
EXECUTE FUNCTION change_metrics_log();

CREATE TRIGGER trg_divide_aggmetrics_log
BEFORE INSERT ON aggregated_metrics
FOR EACH ROW
EXECUTE FUNCTION change_metrics_log();

```

Теперь у нас создалась копия таблицы `metrics_log` b `aggregated_metrics`


`metrics_log` - пишет статистику вроде как, но как разобрать что есть что не понятно, так же нужно выяснить как автоочистка таблицы происходит, так как по ощущениям это просто таблица с логами прием старыми, и в которой не совсем понятно что за метрики где используются

`aggregated_metrics` - пишет усредненую статистику, нас постянке ли не уверен, то есть какой обхем она тоже хрнаит не понятно и так же не могу понять что за метриики там лежат, нужно пробовать по id их найти