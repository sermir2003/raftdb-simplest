# Примеры запросов к хранилищу

## Create

```bash
curl -L --header "Content-Type: application/json" --request POST --data '{"value":123}' http://localhost:8000/items/abc
```

## Read

```bash
curl --header "Content-Type: application/json" --request GET http://localhost:8000/items/abc
```

## Update

```bash
curl -L --header "Content-Type: application/json" --request PUT --data '{"value":456}' http://localhost:8000/items/abc
```

## Delete

```bash
curl -L --header "Content-Type: application/json" --request DELETE http://localhost:8000/items/abc
```

## CAS

```bash
curl -L --header "Content-Type: application/json" --request PUT --data '{"expected":null,"desired":789}' http://localhost:8000/items/abc/cas
```

## Get state

```bash
curl http://localhost:8000/debug/state
```
