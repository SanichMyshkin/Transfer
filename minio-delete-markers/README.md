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