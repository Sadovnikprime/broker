# Распределённый брокер сообщений (Message Broker)

**Технологии:** Go, HTTP REST, файловое хранение (без БД), Docker.

---

##  Быстрый запуск (Docker)

```bash
# 1. Запустить все сервисы
docker compose up --build -d

# 2. Проверить статус
curl http://localhost:8080/status

# 3. Отправить сообщение
curl -X POST http://localhost:8081/api/v1/topics/demo/messages \
  -H "Content-Type: application/json" \
  -d '{"payload":"Hello!"}'

# 4. Прочитать сообщения
curl http://localhost:8081/api/v1/topics/demo/messages?from=0&limit=10

# 5. Остановить
docker compose down   