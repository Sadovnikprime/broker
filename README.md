# Message Broker

| Сервис | Порт | Назначение |
|--------|------|------------|
| Manager | 8080 | offset'ы групп, `/status` |
| Broker | 8081 | запись и чтение сообщений |

---

## Терминал — запуск и работа сервисов

```bash
cd C:\Users\kocet\Desktop\Practic

# запуск
docker compose up --build -d
docker compose ps

# отправить сообщения (CLI)
docker compose run --rm broker /app/publisher -broker http://broker:8081 -topic demo -count 5

# прочитать сообщения (CLI)
docker compose run --rm broker /app/subscriber -manager http://manager:8080 -topic demo -group g1 -id w1 -once

# остановка
docker compose down
```

---

## Консоль 

```bash
# статус системы
curl http://localhost:8080/status

# healthcheck
curl http://localhost:8080/health
curl http://localhost:8081/health

# отправить сообщение
curl -X POST http://localhost:8081/api/v1/topics/demo/messages \
  -H "Content-Type: application/json" \
  -d "{\"payload\":\"Hello!\"}"

# прочитать сообщения напрямую из брокера
curl "http://localhost:8081/api/v1/topics/demo/messages?from=0&limit=10"

# получить сообщение через менеджер (с учётом группы)
curl -X POST http://localhost:8080/api/v1/consume \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"demo\",\"group\":\"g1\",\"consumer_id\":\"w1\",\"limit\":10}"

# подтвердить обработку (ACK)
curl -X POST http://localhost:8080/api/v1/ack \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"demo\",\"group\":\"g1\",\"consumer_id\":\"w1\",\"offsets\":[0]}"
```
