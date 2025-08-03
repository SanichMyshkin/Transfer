Скачивание с n3rd
```bash
docker run --rm \
  -v ./:/backup \
  -e HOME=/tmp \
  --user $(id -u):$(id -g) \
  utrecht/n3dr:7.6.0 repositoriesV2 \
  --backup \
  --n3drRepo source-maven2 \
  --directory-prefix /backup \
  -u usr \
  -p pswrd \
  -n nexus.sanich.space \
  --https=false
```
```bash
root@sanich ~/transfer/migration-blobs/source-maven2
.venv ❯ tree
.
└── com
    └── example
        └── demo-lib
            ├── 1.0
            │   ├── demo-lib-1.0.jar
            │   └── demo-lib-1.0.pom
            └── 2.0
                ├── demo-lib-2.0.jar
                └── demo-lib-2.0.pom

5 directories, 4 files
```
Выполняется успешно

По инструкции нужно удалить `pom` файлы, но прежде сделать резервную копию скаченных артефактов
```bash
cp -r ./source-maven2 ./source-maven2-backup  # резеврная копия
find ./source-maven2 -name "*.pom" -type f -delete
```
```bash
root@sanich ~/transfer/migration-blobs/source-maven2 main*
.venv ❯ tree
.
└── com
    └── example
        └── demo-lib
            ├── 1.0
            │   └── demo-lib-1.0.jar
            └── 2.0
                └── demo-lib-2.0.jar

5 directories, 2 files
```

Так же прежде чем пушить нужно засунуть все что находится в указанной нами директорией `source-maven2` в директории с названием нашего будующего репозитория
```
root@sanich ~/transfer/migration-blobs/source-maven2 main*
.venv ❯ tree
.
└── target-maven2
    └── com
        └── example
            └── demo-lib
                ├── 1.0
                │   └── demo-lib-1.0.jar
                └── 2.0
                    └── demo-lib-2.0.jar

6 directories, 2 files
```

Этой командой нужно запушить артефакты, но выдается 415 ошибка
```bash
docker run --rm \
  -v $(pwd)/source-maven2:/backup \
  -e HOME=/tmp \
  --user $(id -u):$(id -g) \
  utrecht/n3dr:7.6.0 repositoriesV2 \
  --upload \
  --n3drRepo target-maven2 \
  --directory-prefix /backup \
  -u usr \
  -p pswrd \
  -n nexus.sanich.space \
  --https=false
```

Тадам вы прочитали иснтрукцию как делается миграция, но в итоге неделается, СПАСИБО!