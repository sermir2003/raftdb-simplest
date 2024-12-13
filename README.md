# RaftDB simplest

Простое распределённое key-value хранилище, использующее алгоритма консенсуса Raft для репликации записей. Поддерживает CRUD+ API, по каждому ключу вы можете создать, прочитать, обновить, удалить запись или сделать атомарный compare-exchange записи. Примеры запросов смотрите ниже.

## Установка и запуск

```bash
git clone git@github.com:sermir2003/raftdb-simplest.git  # Склонируйте
cd raftdb-simplest
python3 -m venv .venv  # Установите зависимости, например так
source .venv/bin/activate
pip install -r requirements.txt
docker build -t node_image .  # Соберите образ
docker network create --driver bridge raft_network  # Создайте отдельную сеть для симуляции проблем
./run_node.py node-*
```

`run_node.py` — это простой скрипт для запуска docker-образов узлов, передающий необходимые параметры. Не стесняйтесь переписывать его под свои предпочтения. По умолчанию скрип принимает имя узла, и запускает docker-контейнер узла с данным именем и конфигом `config.json`. Вы можете менять конфиг для настройки узла.

## Отправка запросов

```bash
# Чтобы оказаться в одной с контейнерами сети
docker run -it --name observer --network raft_network --entrypoint /bin/sh alpine:latest
# В контейнере чтобы установить curl
apk update && apk add curl
```

## Примеры запросов к сервису

Используйте hostname узла вместо `node-*`.

### Create

```bash
curl -L --header "Content-Type: application/json" --request POST --data '{"value":123}' http://node-*:5000/items/abc
```

### Read

```bash
curl --header "Content-Type: application/json" --request GET http://node-*:5000/items/abc
```

### Update

```bash
curl -L --header "Content-Type: application/json" --request PUT --data '{"value":456}' http://node-*:5000/items/abc
```

### Delete

```bash
curl -L --header "Content-Type: application/json" --request DELETE http://node-*:5000/items/abc
```

### CAS

```bash
curl -L --header "Content-Type: application/json" --request PUT --data '{"expected":null,"desired":789}' http://node-*:5000/items/abc/cas
```

`expected` и `desired` могут быть `null`.

### Get state

```bash
curl http://node-*:5000/debug/state
```

Mostly for debug

## Симуляция падений и перезапусков узлов

В любой момент вы можете остановить любой контейнер, после чего при желании снова поднять его той же командой (`./run_node.py node-*`). Каждый узел сохраняет персистентное состояние на диск в директорию `{node_id}`. В соответствии с настройками в `run_node.py` данная директория прокидывается в хостовую файловую систему. В той же директории можно найти логи узла.

## Симуляция сетевых проблем

```bash
# Получите id виртуального сетевого интерфейса docker сети raft_network
docker network ls | grep raft_network
# Положите его в переменную окружения
export raft_network_id=...

# Создайте сетевые проблемы
# loss in 33% cases
sudo tc qdisc add dev br-$raft_network_id root netem loss 33%
# duplicate in 50% cases
sudo tc qdisc add dev br-$raft_network_id root netem duplicate 50%
# add 30ms as base delay and random jitter (variation in latency) of up to 25ms
sudo tc qdisc add dev br-$raft_network_id root netem delay 30ms 25ms
# reorder 25% packages by adding to their delay 10ms, consecutive packets are reordered in 50% cases
sudo tc qdisc add dev br-$raft_network_id root netem delay 10ms reorder 25% 50%
# or any combination
sudo tc qdisc add dev br-$raft_network_id root netem delay 30ms 25ms loss 33% duplicate 50% reorder 25% 50%
# This applies:
#   30ms base latency with 25ms jitter
#    33% packet loss
#    50% packet duplication
#    25% packet reordering with a 50% correlation

# Отмените сетевые проблемы
sudo tc qdisc del dev br-$raft_network_id root
```
