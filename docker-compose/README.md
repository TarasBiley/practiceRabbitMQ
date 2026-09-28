# Notification Service

Микросервис уведомлений для интернет-магазина.

Сервис принимает события заказов, сохраняет их в PostgreSQL и асинхронно отправляет в RabbitMQ для дальнейшей обработки.

## Возможности

- создание событий заказа;
- получение списка событий;
- получение события по ID;
- фильтрация и пагинация;
- PostgreSQL;
- RabbitMQ;
- Publisher;
- Consumer;
- ACK / NACK;
- Dead Letter Queue;
- retry failed-событий;
- повторное подключение к RabbitMQ;
- Swagger / OpenAPI;
- graceful shutdown;
- unit и integration tests.

## Стек

- Go
- PostgreSQL
- RabbitMQ
- Docker Compose
- pgx
- amqp091-go
- Swagger / swaggo

## Архитектура

Основная цепочка создания события:

```text
Client
  |
  v
HTTP Handler
  |
  v
Service
  |
  v
Repository
  |
  v
PostgreSQL
  |
  | pending
  v
Publisher
  |
  v
RabbitMQ
  |
  v
Consumer
  |
  +------ success ------> sent
  |
  +------ error --------> retry / failed / DLQ