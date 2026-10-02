# practiceRabbitMQ

Учебный Go-микросервис обработки событий заказов с PostgreSQL и RabbitMQ.

Сервис принимает события заказа по HTTP, сохраняет их в PostgreSQL со статусом `pending`, периодически публикует ожидающие события в RabbitMQ и обрабатывает их consumer'ом. При успешной обработке событие становится sent. Ошибки увеличивают `retry_count`, неуспешные сообщения попадают в DLQ, а события со статусом `failed` можно повторно поставить в обработку через admin endpoint.

Все команды ниже выполняются из корня проекта, где находится go.mod.

## Основной поток события

```text
POST /api/events
      |
      v
   Handler
      |
      v
   Service
      |
      v
 PostgreSQL
 status=pending
      |
      | publisher
      v
orders.exchange
      |
      v
notifications.queue
      |
      v
   Consumer
    /     \
 success   error
   |        |
   v        v
 status    retry_count + 1
 =sent     Nack(requeue=false)
            |
            v
      notifications.dlx
            |
            v
      notifications.dlq
```

Publisher периодически выбирает до 10 событий со статусом `pending` и публикует их в RabbitMQ. Consumer имитирует отправку уведомления; часть обработок завершается ошибкой для проверки retry/DLQ-сценария.

## Архитектура

```text
practiceRabbitMQ/
├── cmd/
│   └── api/
│       └── main.go
├── internal/
│   ├── config/
│   ├── domain/
│   ├── handler/
│   ├── service/
│   ├── repository/
│   └── broker/
├── migrations/
├── docs/
├── docker-compose/
│   ├── docker-compose.yml
│   └── docker-compose.test.yml
├── go.mod
├── go.sum
└── README.md
```

Роли пакетов:

- `cmd/api` — сборка приложения и запуск HTTP-сервера, PostgreSQL и RabbitMQ.
- `internal/config` — чтение конфигурации из переменных окружения.
- `internal/domain` — модели событий и ответы API.
- `internal/handler` — HTTP-обработчики.
- `internal/service` — бизнес-логика.
- `internal/repository` — SQL и работа с PostgreSQL.
- `internal/broker` — publisher, consumer, RabbitMQ, reconnect/backoff.
- `migrations` — миграции PostgreSQL.
- `docs` — сгенерированная Swagger-документация.

## Требования

- Go версии из go.mod.
- Docker с Docker Compose.
- PostgreSQL и RabbitMQ для запуска приложения.
- Для unit-тестов достаточно Go.
- Для команд с `-race` нужны включённый CGO и доступный C-компилятор.

## Быстрый запуск приложения

Поднять PostgreSQL и RabbitMQ:

```sh
docker compose -f docker-compose/docker-compose.yml up -d
```

Проверить контейнеры:

```sh
docker ps
```

В обычном окружении используются:

```text
PostgreSQL:          localhost:5434
RabbitMQ AMQP:       localhost:5672
RabbitMQ Management: http://localhost:15672
```

Перед первым запуском применить миграцию к пустой базе:

```sh
docker exec -i notifications-postgres \
  psql -U app -d notifications < migrations/001_init.sql
```

Запустить приложение:

```sh
go run ./cmd/api
```

При успешном старте в логах должны появиться сообщения о подключении к PostgreSQL и RabbitMQ и запуске HTTP-сервера на :8080.

Остановить приложение можно через Ctrl+C. Сервер и RabbitMQ manager завершаются через graceful shutdown.

## HTTP API

### POST /api/events

Регистрирует новое событие заказа.

```sh
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "123",
    "order_id": "order-100",
    "event_type": "paid",
    "payload": {
      "amount": 2500
    }
  }'
```

Допустимые `event_type`:

- `created`
- `paid`
- `shipped`

Успешный ответ:

```json
{
  "event_id": "uuid",
  "status": "queued"
}
```

Событие при этом сохраняется в PostgreSQL со статусом pending.

### GET /api/events

Поддерживаются фильтры и пагинация:

- `status`
- `user_id`
- `limit`
- `offset`

```sh
curl "http://localhost:8080/api/events?user_id=123&status=sent&limit=10&offset=0"
```

### GET /api/events/{event_id}

```sh
curl http://localhost:8080/api/events/708b8a2f-222a-40da-850d-4ae5eff1324a
```

Если событие отсутствует, API возвращает 404.

### POST /api/admin/retry-pending

Повторно ставит события со статусом `failed` в обработку.

```sh
curl -X POST http://localhost:8080/api/admin/retry-pending
```

Пример ответа:

```json
{
  "retried": 1
}
```

При повторной постановке событие возвращается в `pending`, `retry_count` сбрасывается, а `error_message` очищается.

## Статусы события

```text
pending
  |
  | успешная обработка
  v
 sent
```

При ошибке consumer увеличивает retry_count. Неуспешная доставка сообщения отправляется через DLX в notifications.dlq.

После достижения лимита повторных ошибок событие переводится в failed. Его можно вернуть в обработку через POST /api/admin/retry-pending.

## RabbitMQ

При старте приложения создаются:

- `orders.exchange`
- `notifications.queue`
- `notifications.dlx`
- `notifications.dlq`

Основной маршрут:

```text
orders.exchange
  -> notifications.queue
```

Ошибка consumer:

```text
notifications.queue
  -> Nack(requeue=false)
  -> notifications.dlx
  -> notifications.dlq
```

Проверить очереди:

```sh
docker exec notifications-rabbitmq \
  rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

## Проверка данных PostgreSQL

```sh
docker exec -it notifications-postgres \
  psql -U app -d notifications
```

```sql
SELECT event_id, user_id, order_id, event_type, status, retry_count
FROM events
ORDER BY created_at DESC
LIMIT 10;
```

Выйти:

```text
\q
```

## Swagger

После запуска приложения Swagger UI доступен по адресу:

<http://localhost:8080/swagger/>

После изменения API документацию можно пересоздать:

```sh
swag init \
  -g main.go \
  -d cmd/api,internal/handler,internal/domain \
  --parseInternal \
  -o docs
```

## Тестирование

### Какие тесты есть

Unit-тесты проверяют функции без настоящих PostgreSQL и RabbitMQ. Вместо внешних зависимостей используются fake-объекты.

Интеграционные тесты работают с настоящими PostgreSQL или RabbitMQ и проверяют SQL, миграции, маршрутизацию сообщений и DLQ.

Табличный тест описывает несколько сценариев и выполняет их через t.Run.

| Файл | Что проверяет | Нужны сервисы |
| --- | --- | --- |
| `internal/service/service_test.go` | Валидацию, UUID, передачу аргументов и ошибки репозитория | Нет |
| `internal/handler/handlers_test.go` | HTTP-статусы, JSON, валидацию, фильтры и пагинацию | Нет |
| `internal/broker/consumer_test.go` | Обработку сообщений, retry, Ack/Nack | Нет |
| `internal/broker/publisher_test.go` | Публикацию, ошибки, timer и cancellation | Нет |
| `internal/broker/rabbitmq_test.go` | Reconnect, backoff и закрытие ресурсов | Нет |
| `internal/repository/repository_test.go` | Проверки repository без настоящей БД | Нет |
| `internal/repository/repository_integration_test.go` | SQL, фильтры, пагинацию, статусы и `retry_count` | PostgreSQL |
| `internal/broker/rabbitmq_integration_test.go` | Реальную маршрутизацию и DLQ | RabbitMQ + management API |

Общие fake-объекты broker-тестов находятся в internal/broker/test_helpers_test.go.

### Обычное и тестовое окружения

| Параметр | Окружение приложения | Тестовое окружение |
| --- | --- | --- |
| Compose-файл | `docker-compose/docker-compose.yml` | `docker-compose/docker-compose.test.yml` |
| База PostgreSQL | `notifications` | `notifications_test` |
| PostgreSQL | `localhost:5434` | `localhost:15434` |
| PostgreSQL user/password | `app / app` | `app / app` |
| RabbitMQ AMQP | `localhost:5672` | `localhost:15673` |
| RabbitMQ management | `http://localhost:15672` | `http://localhost:15674` |
| RabbitMQ user/password | `guest / guest` | `guest / guest` |
| PostgreSQL storage | `postgres_data` | `tmpfs` |

Тестовый Compose-проект создаёт отдельные контейнеры. Интеграционные тесты используют `TEST_DATABASE_URL`, `TEST_RABBITMQ_URL` и TEST_RABBITMQ_MANAGEMENT_URL.

#### Изоляция PostgreSQL integration tests

Каждый integration test:

1. создаёт схему вида `test_repository_<uuid>`;
2. настраивает `search_path`;
3. применяет миграции из migrations;
4. создаёт необходимые данные;
5. через `t.Cleanup` удаляет временную схему.

Поэтому тесты не используют `public.events` и не мешают друг другу.

#### Изоляция RabbitMQ integration tests

Для каждого RabbitMQ integration test создаётся отдельный vhost:

```text
practice-rabbitmq-test-<uuid>
```

Маршрут:

```text
orders.exchange
  -> notifications.queue
  -> Nack(requeue=false)
  -> notifications.dlx
  -> notifications.dlq
```

После теста временный vhost удаляется.

### Запуск unit-тестов

```sh
go test ./...
```

Подробно:

```sh
go test -v ./...
```

С race detector:

```sh
go test -race ./...
```

Один пакет:

```sh
go test -v ./internal/service
```

Один сценарий:

```sh
go test -v ./internal/service -run '^TestValidateEvent/missing_user$'
```

Integration-файлы начинаются с:

```go
//go:build integration
```

Без `-tags=integration` они не запускаются.

### Запуск всех тестов с интеграцией

```sh
docker compose -f docker-compose/docker-compose.test.yml \
  up --wait --wait-timeout 60

TEST_DATABASE_URL='postgres://app:app@localhost:15434/notifications_test?sslmode=disable' \
TEST_RABBITMQ_URL='amqp://guest:guest@localhost:15673/' \
TEST_RABBITMQ_MANAGEMENT_URL='http://localhost:15674' \
go test -race -tags=integration ./... -count=1 -timeout=60s
```

Само приложение через `go run` для integration tests запускать не требуется.

### Только repository + PostgreSQL

```sh
docker compose -f docker-compose/docker-compose.test.yml \
  up --wait --wait-timeout 60 postgres

TEST_DATABASE_URL='postgres://app:app@localhost:15434/notifications_test?sslmode=disable' \
go test -v -tags=integration ./internal/repository -count=1 -timeout=60s
```

### Только broker + RabbitMQ

```sh
docker compose -f docker-compose/docker-compose.test.yml \
  up --wait --wait-timeout 60 rabbitmq

TEST_RABBITMQ_URL='amqp://guest:guest@localhost:15673/' \
TEST_RABBITMQ_MANAGEMENT_URL='http://localhost:15674' \
go test -v -tags=integration ./internal/broker -count=1 -timeout=60s
```

## Покрытие кода

Coverage показывает, какие statements выполнились во время конкретного запуска.

### Только unit-тесты

```sh
go test ./... -count=1 -coverprofile=coverage-unit.out
go tool cover -func=coverage-unit.out
go tool cover -html=coverage-unit.out -o coverage-unit.html
```

### Почему SQL-методы могут показывать 0%

service-тесты используют fake repository и не выполняют настоящий SQL из internal/repository.

Без integration tests методы вроде:

- `GetEvents`
- `GetEventByID`
- `RetryFailedEvents`
- `GetPendingEvents`
- `MarkEventSent`
- `IncrementRetryCount`
- `MarkEventFailed`

могут показывать 0.0%.

Это означает, что настоящий SQL-код не выполнялся в данном запуске, а не то, что тесты упали.

### Coverage repository с PostgreSQL

```sh
TEST_DATABASE_URL='postgres://app:app@localhost:15434/notifications_test?sslmode=disable' \
go test -tags=integration ./internal/repository \
  -count=1 \
  -timeout=60s \
  -coverprofile=coverage-repository.out

go tool cover -func=coverage-repository.out
go tool cover -html=coverage-repository.out -o coverage-repository.html
```

### Общее coverage с integration tests

```sh
docker compose -f docker-compose/docker-compose.test.yml \
  up --wait --wait-timeout 60

TEST_DATABASE_URL='postgres://app:app@localhost:15434/notifications_test?sslmode=disable' \
TEST_RABBITMQ_URL='amqp://guest:guest@localhost:15673/' \
TEST_RABBITMQ_MANAGEMENT_URL='http://localhost:15674' \
go test -race -tags=integration ./... \
  -count=1 \
  -timeout=60s \
  -covermode=atomic \
  -coverprofile=coverage-all.out

go tool cover -func=coverage-all.out
go tool cover -html=coverage-all.out -o coverage-all.html
```

После изменения кода или тестов coverage profile нужно создавать заново.

## Переменные окружения тестового окружения

| Переменная | Кто читает | Значение |
| --- | --- | --- |
| `TEST_DATABASE_URL` | repository integration tests | `postgres://app:app@localhost:15434/notifications_test?sslmode=disable` |
| `TEST_RABBITMQ_URL` | RabbitMQ integration tests | `amqp://guest:guest@localhost:15673/` |
| `TEST_RABBITMQ_MANAGEMENT_URL` | RabbitMQ integration tests | `http://localhost:15674` |
| `TEST_POSTGRES_PORT` | Docker Compose | `15434` |
| `TEST_RABBITMQ_PORT` | Docker Compose | `15673` |
| `TEST_RABBITMQ_MANAGEMENT_PORT` | Docker Compose | `15674` |

## Статус, логи и очистка test environment

```sh
docker compose -f docker-compose/docker-compose.test.yml ps

docker compose -f docker-compose/docker-compose.test.yml \
  logs --tail=100 postgres rabbitmq

docker compose -f docker-compose/docker-compose.test.yml down
```

Эта команда не удаляет основные контейнеры приложения и основной PostgreSQL volume.

## Troubleshooting

| Симптом | Что проверить |
| --- | --- |
| `0.0%` у repository SQL-методов | Запустить integration tests с `-tags=integration` |
| set TEST_DATABASE_URL... | Передать `TEST_DATABASE_URL` |
| set `TEST_RABBITMQ_URL` and TEST_RABBITMQ_MANAGEMENT_URL... | Передать обе RabbitMQ test-переменные |
| connection refused | Проверить контейнеры, порт и URL |
| database "notifications_test" does not exist | Проверить test Compose и `TEST_DATABASE_URL` |
| Порт занят | Изменить Compose port variable и соответствующий test URL |
| undefined: FakeRepository | Проверить актуальность test-файлов и package |
| VS Code показывает старый файл | Закрыть старую вкладку без сохранения либо выполнить `Developer: Reload Window`; при конфликте сначала использовать `Compare` |

## Ручная end-to-end проверка

Создать событие:

```sh
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "123",
    "order_id": "e2e-test",
    "event_type": "paid",
    "payload": {"amount": 2500}
  }'
```

В логах приложения ожидается:

```text
published event: <event_id>
consumer received: <event_id>
event sent: <event_id>
```

Проверить результат:

```sh
docker exec -it notifications-postgres \
  psql -U app -d notifications \
  -c "SELECT event_id, order_id, status, retry_count
      FROM events
      WHERE order_id='e2e-test';"
```

После успешной обработки:

```text
status = sent
retry_count = 0
```

Проверить DLQ:

```sh
docker exec notifications-rabbitmq \
  rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

## Что уже проверено

В текущей реализации вручную и тестами проверены:

- создание события через HTTP;
- сохранение `pending` в PostgreSQL;
- публикация в RabbitMQ;
- получение сообщения consumer'ом;
- переход `pending` -> `sent`;
- увеличение `retry_count` после simulated error;
- повторная публикация `pending`;
- попадание неуспешной доставки в `notifications.dlq`;
- admin retry для `failed`;
- сброс retry-данных при повторной постановке;
- повторный переход в `sent`;
- graceful shutdown;
- RabbitMQ reconnect/backoff;
- unit- и integration-тесты.
