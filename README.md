# practiceRabbitMQ

Учебный Go-сервис событий заказов с PostgreSQL и RabbitMQ. Этот README описывает устройство тестов, отдельное тестовое окружение, запуск проверок и получение покрытия.

Все команды ниже выполняются **из корня проекта**, где находится `go.mod`. Примеры переменных окружения рассчитаны на Bash/Zsh.

## Требования

- Go версии из [go.mod](go.mod): сейчас `1.26.2`.
- Docker с Docker Compose для тестов с настоящими PostgreSQL и RabbitMQ.
- Для юнит-тестов достаточно Go: запускать контейнеры или HTTP-приложение не требуется.
- Для команд с `-race` дополнительно нужны включённый CGO и доступный C-компилятор.

## Какие тесты есть

**Юнит-тесты** проверяют функции без настоящих PostgreSQL и RabbitMQ. Вместо внешних зависимостей используются fake-объекты: они возвращают заданные ответы и записывают вызовы, аргументы и сообщения.

**Интеграционные тесты** работают с настоящими PostgreSQL или RabbitMQ. Они проверяют SQL-запросы, миграции, маршрутизацию сообщений и доставку в очередь ошибок.

**Табличный тест** — способ описать несколько сценариев и выполнить их через `t.Run`. Табличными могут быть и юнит-тесты, и интеграционные тесты. Например, валидация события проверяется без БД, а разные фильтры и страницы списка событий — с PostgreSQL.

| Файл | Что проверяет | Нужны сервисы |
| --- | --- | --- |
| [service_test.go](internal/service/service_test.go) | Валидацию, генерацию UUID, передачу аргументов и контекста, возврат данных и ошибок репозитория | Нет |
| [handlers_test.go](internal/handler/handlers_test.go) | HTTP-статусы, JSON, заголовки, валидацию, фильтры и пагинацию; отсутствие вызовов репозитория при отклонении запроса | Нет |
| [consumer_test.go](internal/broker/consumer_test.go) | Обработку сообщения, ошибки, границы числа попыток, параметры Ack/Nack и завершение обработчика | Нет |
| [publisher_test.go](internal/broker/publisher_test.go) | Полное содержимое сообщений, несколько публикаций, ошибку посередине, срабатывания таймера и отмену | Нет |
| [rabbitmq_test.go](internal/broker/rabbitmq_test.go) | Повторные подключения, задержки между попытками, закрытие ресурсов и отмену через fake-объекты | Нет |
| [repository_test.go](internal/repository/repository_test.go) | Конструктор репозитория и ошибку сериализации payload до обращения к БД | Нет |
| [repository_integration_test.go](internal/repository/repository_integration_test.go) | Запись и чтение событий, фильтры, порядок, пагинацию, смену статусов и счётчики повторов | PostgreSQL |
| [rabbitmq_integration_test.go](internal/broker/rabbitmq_integration_test.go) | Реальную доставку через exchange в очередь и перенаправление отклонённого сообщения в DLQ | RabbitMQ и management API |

Общие fake-объекты тестов брокера находятся в [test_helpers_test.go](internal/broker/test_helpers_test.go). Тестовые типы другого пакета, например из `internal/service/service_test.go`, не становятся доступны пакету `broker`.

Случайный выбор ошибки consumer в тестах управляется явно. Тесты publisher подают сигналы времени и ожидают завершения горутины, поэтому им не нужно ждать рабочего интервала публикации в 30 секунд.

## Обычное и тестовое окружения

Для них используются разные Compose-файлы и порты:

| Параметр | Окружение приложения | Тестовое окружение |
| --- | --- | --- |
| Compose-файл | [docker-compose.yml](docker-compose/docker-compose.yml) | [docker-compose.test.yml](docker-compose/docker-compose.test.yml) |
| База PostgreSQL | `notifications` | `notifications_test` |
| PostgreSQL на хосте | `localhost:5434` | `localhost:15434` |
| Пользователь / пароль PostgreSQL | `app / app` | `app / app` |
| RabbitMQ AMQP на хосте | `localhost:5672` | `localhost:15673` |
| RabbitMQ management | `http://localhost:15672` | `http://localhost:15674` |
| Пользователь / пароль RabbitMQ | `guest / guest` | `guest / guest` |
| Хранение PostgreSQL | Именованный volume `postgres_data` | Временное хранилище `tmpfs` |

Тестовый Compose-проект называется `notifications-tests`. Он создаёт отдельные контейнеры; его порты доступны через `127.0.0.1`. Данные тестовых PostgreSQL и RabbitMQ хранятся в `tmpfs` и теряются при остановке контейнеров.

Приложение читает `DATABASE_URL` и `RABBITMQ_URL`. Интеграционные тесты читают отдельные переменные `TEST_DATABASE_URL`, `TEST_RABBITMQ_URL` и `TEST_RABBITMQ_MANAGEMENT_URL`. Указанные ниже тестовые URL ведут в тестовые контейнеры и не используют основную базу `notifications`.

### Где находятся тестовые данные PostgreSQL

Базу `notifications_test` создаёт контейнер PostgreSQL через настройку `POSTGRES_DB`. Сам тестовый helper подключается к **уже существующей базе** и выполняет следующие действия:

1. Создаёт схему с уникальным именем `test_repository_<uuid>`.
2. Настраивает `search_path` соединений на эту схему.
3. Применяет SQL-файлы из [migrations](migrations) по порядку имён.
4. Создаёт события, необходимые конкретному тесту.
5. Через `t.Cleanup` закрывает пул соединений и удаляет свою схему вместе с таблицами и данными.

Поэтому таблица теста находится, например, в `test_repository_<uuid>.events`, а не в `public.events`. После завершения тестов эти временные схемы обычно уже удалены. Независимые тесты и запуски не очищают таблицы друг друга: общий `TRUNCATE events` не используется.

Для своей тестовой инфраструктуры нужно заранее создать базу и дать тестовому пользователю право создавать схемы. Обычные ошибки и `t.Fatal` запускают cleanup; принудительное завершение процесса может оставить временные схемы. В поставляемом тестовом окружении остановка контейнера также удаляет временные данные.

### Где находятся тестовые очереди RabbitMQ

Для каждого RabbitMQ-интеграционного теста helper создаёт vhost `practice-rabbitmq-test-<uuid>` через management API. Vhost — отдельное пространство очередей и exchange внутри RabbitMQ.

В нём проверяется маршрут:

```text
orders.exchange
  → notifications.queue
  → Nack с requeue=false
  → notifications.dlx
  → notifications.dlq
```

После проверки соединение закрывается, а тестовый vhost удаляется. Имена очередей совпадают с именами в приложении, но очереди находятся в отдельном vhost.

Management API использует логин и пароль из `TEST_RABBITMQ_URL`. Пользователю нужны права доступа к management API, создания vhost и выдачи разрешений. Для конфигурации из репозитория используется `guest / guest`.

## Запуск юнит-тестов

Все юнит-тесты с проверкой гонок данных:

```sh
go test -race ./...
```

Без проверки гонок можно выполнить `go test ./...`.

Один пакет или один именованный сценарий:

```sh
go test -v ./internal/service
go test -v ./internal/service -run '^TestValidateEvent/missing_user$'
```

Файлы интеграционных тестов начинаются с:

```go
//go:build integration
```

Без тега `integration` Go не включает их в сборку тестов. Поэтому обычный запуск не требует БД и RabbitMQ.

## Запуск всех тестов с интеграцией

Поднять тестовые сервисы и дождаться готовности:

```sh
docker compose -f docker-compose/docker-compose.test.yml up --wait --wait-timeout 60
```

Запустить юнит-тесты и интеграционные тесты вместе:

```sh
TEST_DATABASE_URL='postgres://app:app@localhost:15434/notifications_test?sslmode=disable' \
TEST_RABBITMQ_URL='amqp://guest:guest@localhost:15673/' \
TEST_RABBITMQ_MANAGEMENT_URL='http://localhost:15674' \
go test -race -tags=integration ./... -count=1 -timeout=60s
```

`-tags=integration` добавляет интеграционные тесты к обычным; `-count=1` запускает проверки заново без использования закэшированного результата; `-timeout=60s` ограничивает время выполнения тестового пакета.

Запускать само приложение через `go run` для этих проверок не нужно. Без обязательных `TEST_...` переменных соответствующие интеграционные тесты завершаются ошибкой настройки.

## Проверить только репозиторий с PostgreSQL

RabbitMQ для этой команды не требуется:

```sh
docker compose -f docker-compose/docker-compose.test.yml up --wait --wait-timeout 60 postgres

TEST_DATABASE_URL='postgres://app:app@localhost:15434/notifications_test?sslmode=disable' \
go test -v -tags=integration ./internal/repository -count=1 -timeout=60s
```

Для одного сценария добавь, например, `-run '^TestRepositoryGetEvents$'` к команде `go test`.

## Проверить брокер с настоящим RabbitMQ

PostgreSQL для этой команды не требуется: данные событий предоставляет fake-репозиторий.

```sh
docker compose -f docker-compose/docker-compose.test.yml up --wait --wait-timeout 60 rabbitmq

TEST_RABBITMQ_URL='amqp://guest:guest@localhost:15673/' \
TEST_RABBITMQ_MANAGEMENT_URL='http://localhost:15674' \
go test -v -tags=integration ./internal/broker -count=1 -timeout=60s
```

## Покрытие кода

Покрытие показывает, какие операторы кода выполнились во время конкретного запуска тестов. Процент не измеряет количество тестов или полноту проверок всех граничных случаев.

### Почему у SQL-методов бывает 0%

В обычных юнит-тестах репозитория выполняются только конструктор и ветка ошибки сериализации payload. Методы `GetEvents`, `GetEventByID`, `RetryFailedEvents`, `GetPendingEvents`, `MarkEventSent`, `IncrementRetryCount` и `MarkEventFailed` проверяются с настоящим PostgreSQL в интеграционном файле.

Если собрать покрытие без `-tags=integration`, эти методы не исполнятся и получат `0.0%`. Юнит-тесты сервиса используют fake и не добавляют покрытие SQL-коду настоящего репозитория.

### Покрытие только юнит-тестов

```sh
go test ./... -count=1 -coverprofile=coverage-unit.out
go tool cover -func=coverage-unit.out
go tool cover -html=coverage-unit.out -o coverage-unit.html
```

Файл `coverage-unit.html` можно открыть в браузере: там видны выполненные и невыполненные участки.

### Покрытие репозитория с PostgreSQL

Этот вариант позволяет проверить именно методы, у которых в обычном отчёте были нули:

```sh
docker compose -f docker-compose/docker-compose.test.yml up --wait --wait-timeout 60 postgres

TEST_DATABASE_URL='postgres://app:app@localhost:15434/notifications_test?sslmode=disable' \
go test -tags=integration ./internal/repository -count=1 -timeout=60s -coverprofile=coverage-repository.out

go tool cover -func=coverage-repository.out
go tool cover -html=coverage-repository.out -o coverage-repository.html
```

### Общее покрытие с интеграционными тестами

```sh
docker compose -f docker-compose/docker-compose.test.yml up --wait --wait-timeout 60

TEST_DATABASE_URL='postgres://app:app@localhost:15434/notifications_test?sslmode=disable' \
TEST_RABBITMQ_URL='amqp://guest:guest@localhost:15673/' \
TEST_RABBITMQ_MANAGEMENT_URL='http://localhost:15674' \
go test -race -tags=integration ./... -count=1 -timeout=60s -covermode=atomic -coverprofile=coverage-all.out

go tool cover -func=coverage-all.out
go tool cover -html=coverage-all.out -o coverage-all.html
```

`coverage-all.out` содержит результаты одного общего запуска юнит-тестов и интеграционных тестов. Все файлы отчётов в этих примерах создаются в корне проекта.

`go tool cover` читает готовый профиль и не запускает тесты заново. После изменения кода или набора тестов профиль нужно пересоздать. Старый `docker-compose/coverage.out` содержит пути до разнесения кода по `internal/` и не отражает текущее состояние проекта.

## Переменные окружения и другие порты

| Переменная | Кто читает | Значение для тестового Compose по умолчанию |
| --- | --- | --- |
| `TEST_DATABASE_URL` | Тесты репозитория | `postgres://app:app@localhost:15434/notifications_test?sslmode=disable` |
| `TEST_RABBITMQ_URL` | Тесты RabbitMQ | `amqp://guest:guest@localhost:15673/` |
| `TEST_RABBITMQ_MANAGEMENT_URL` | Тесты RabbitMQ | `http://localhost:15674` |
| `TEST_POSTGRES_PORT` | Docker Compose | `15434` |
| `TEST_RABBITMQ_PORT` | Docker Compose | `15673` |
| `TEST_RABBITMQ_MANAGEMENT_PORT` | Docker Compose | `15674` |

Для трёх URL в тестовом коде нет значения по умолчанию: их нужно передать явно. Переменные портов влияют на Docker Compose; они не формируют URL для `go test` автоматически.

Например, если порт `15434` занят, можно запустить тестовый PostgreSQL на `15435`:

```sh
TEST_POSTGRES_PORT=15435 docker compose -f docker-compose/docker-compose.test.yml up --wait postgres

TEST_DATABASE_URL='postgres://app:app@localhost:15435/notifications_test?sslmode=disable' \
go test -tags=integration ./internal/repository -count=1 -timeout=60s
```

## Статус, логи и очистка окружения

Проверить контейнеры и последние сообщения сервисов:

```sh
docker compose -f docker-compose/docker-compose.test.yml ps
docker compose -f docker-compose/docker-compose.test.yml logs --tail=100 postgres rabbitmq
```

После проверок остановить и удалить тестовые контейнеры и сеть:

```sh
docker compose -f docker-compose/docker-compose.test.yml down
```

Эта команда относится к проекту `notifications-tests`. Основные контейнеры из `docker-compose.yml` и их PostgreSQL volume она не удаляет. При следующем запуске тестового Compose создаётся чистое временное окружение.

## Если проверка не проходит

| Симптом | Что проверить |
| --- | --- |
| `0.0%` у методов репозитория | Собрать новый профиль с `-tags=integration` и запущенным тестовым PostgreSQL |
| `set TEST_DATABASE_URL...` | Передать URL в той же команде `go test` или экспортировать его в текущем терминале |
| `set TEST_RABBITMQ_URL and TEST_RABBITMQ_MANAGEMENT_URL...` | Передать обе переменные для тестов брокера |
| `connection refused` | Проверить готовность контейнеров через `ps`, логи и совпадение порта в URL с Compose |
| `database "notifications_test" does not exist` | Убедиться, что URL указывает на тестовый контейнер; для своего сервера создать БД заранее |
| Ошибка создания схемы или HTTP 401/403 от management API | Проверить тестовые учётные данные и права создания схемы или vhost |
| Порт уже занят | Использовать переменные портов Compose и обновить соответствующий тестовый URL |
| `undefined: FakeRepository` после обновления файлов | Проверить, что загружены актуальные тесты: в них используется `fakeRepository` из своего пакета |
| Редактор показывает старые тесты | Сравнить полный путь файла с открытым проектом; при конфликте загрузить версию с диска, предварительно сохранив свои несохранённые правки отдельно |

## Swagger

После изменения API пакет [docs](docs) можно пересоздать из аннотаций обработчиков. Команда для `swag` версии `v1.16.4`, указанной в `go.mod`:

```sh
swag init -g main.go -d cmd/api,internal/handler,internal/domain --parseInternal --outputTypes go
```
