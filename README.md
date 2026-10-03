# Messenger Backend

REST API + WebSocket backend для веб-мессенджера. Весь код и конфигурация находятся в одном файле — `main.go`. Backend построен по принципу API-first: фронтенд развёрнут отдельно и подключается по HTTP и WebSocket.

## Содержание

- [Описание](#описание)
- [Stack](#stack)
- [Запуск локально](#запуск-локально)
- [Запуск через Docker](#запуск-через-docker)
- [Environment variables](#environment-variables)
- [Migrations](#migrations)
- [API documentation](#api-documentation)
- [Примеры API requests](#примеры-api-requests)
- [WebSocket protocol](#websocket-protocol)
- [Запуск tests](#запуск-tests)

---

## Описание

Возможности:

- регистрация, вход, выход, обновление access token;
- поиск пользователей по логину;
- создание диалогов (1-на-1);
- отправка и получение сообщений в реальном времени;
- история сообщений с пагинацией;
- удаление своих сообщений;
- статус пользователя online/offline.

Правила:

- **Логин** — только латинские буквы `a–z` в нижнем регистре, длина 3–32 символа.
- **Пароль** — от 6 до 72 символов.
- При попытке зарегистрировать занятый логин возвращается ошибка `409 Conflict` с сообщением `login is already taken`.
- После успешной авторизации фронтенд показывает главную страницу из трёх элементов: поиск пользователя по логину, список последних чатов, область чата. Для этого backend отдаёт `GET /api/users/search`, `GET /api/chats` и `GET /api/chats/{id}/messages` плюс WebSocket для живых событий.

> **Важно:** фактические пути и поля запросов описаны в разделе [API documentation](#api-documentation). Они начинаются с `/api/` (без `/v1`), а регистрация принимает `login` и `password`.

## Stack

| Компонент | Технология |
|---|---|
| Язык | Go 1.22+ (стандартный `net/http` с маршрутизацией по методам) |
| База данных | PostgreSQL (`pgx/v5`, пул соединений) |
| Кэш, presence, pub/sub | Redis (`go-redis/v9`) |
| WebSocket | `gorilla/websocket` |
| Аутентификация | JWT HS256 (`golang-jwt/jwt/v5`), refresh-токены в Redis |
| Хэширование паролей | bcrypt |

Как используется Redis:

- `rt:<sha256(refresh)>` — refresh-токены (одноразовые, с TTL);
- `bl:<jti>` — чёрный список access-токенов после logout;
- `presence:<user_id>` — ZSET активных WebSocket-соединений (статус online);
- канал `msgr:events` — pub/sub доставки событий; благодаря ему можно запускать несколько экземпляров backend.

## Запуск локально

Требования: Go 1.22+, доступные PostgreSQL и Redis.

1. Создайте базу данных (один раз). Для контейнера `baza-postgres`:

   ```bash
   docker exec -it baza-postgres psql -U postgres -c "CREATE DATABASE messenger;"
   ```

2. Если запускаете backend на хосте, а не в docker-сети, имена `baza-postgres` и `baza-redis` не резолвятся. Либо опубликуйте порты контейнеров (`5432`, `6379`) и укажите `localhost`, либо задайте адреса явно.

3. Инициализируйте модуль и запустите:

   ```bash
   go mod init messenger     # только первый раз
   go mod tidy

   export JWT_SECRET="$(openssl rand -hex 32)"
   export POSTGRES_DSN="postgres://postgres:postgres@localhost:5432/messenger?sslmode=disable"
   export REDIS_ADDR="localhost:6379"

   go run .
   ```

4. Проверка:

   ```bash
   curl http://localhost:8080/healthz
   # {"status":"ok"}
   ```

При старте backend ждёт доступности Postgres и Redis (до 30 попыток с интервалом 2 секунды), затем применяет схему БД.

## Запуск через Docker

PostgreSQL и Redis уже развёрнуты в контейнерах `baza-postgres` и `baza-redis`. Backend нужно подключить к той же docker-сети.

1. Узнайте сеть, в которой работают контейнеры:

   ```bash
   docker inspect baza-postgres --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}'
   ```

2. Создайте `Dockerfile` рядом с `main.go` (нужны `go.mod` и `go.sum` — их создаёт `go mod tidy`):

   ```dockerfile
   FROM golang:1.22-alpine AS build
   WORKDIR /src
   COPY go.mod go.sum ./
   RUN go mod download
   COPY main.go .
   RUN CGO_ENABLED=0 go build -o /messenger .

   FROM alpine:3.20
   RUN adduser -D app
   USER app
   COPY --from=build /messenger /messenger
   EXPOSE 8080
   CMD ["/messenger"]
   ```

3. Соберите и запустите:

   ```bash
   docker build -t messenger-backend .

   docker run -d --name messenger-backend \
     --network <имя_сети_из_шага_1> \
     -p 8080:8080 \
     -e JWT_SECRET="$(openssl rand -hex 32)" \
     -e POSTGRES_DSN="postgres://postgres:<пароль>@baza-postgres:5432/messenger?sslmode=disable" \
     -e REDIS_ADDR="baza-redis:6379" \
     -e CORS_ORIGIN="https://<адрес-фронтенда>" \
     messenger-backend
   ```

### Вариант с docker compose

```yaml
services:
  backend:
    build: .
    ports:
      - "8080:8080"
    environment:
      JWT_SECRET: ${JWT_SECRET}
      POSTGRES_DSN: postgres://postgres:${POSTGRES_PASSWORD}@baza-postgres:5432/messenger?sslmode=disable
      REDIS_ADDR: baza-redis:6379
      CORS_ORIGIN: ${CORS_ORIGIN:-*}
    networks:
      - infra
    restart: unless-stopped

networks:
  infra:
    external: true
    name: <имя_сети_из_шага_1>
```

## Environment variables

| Переменная | По умолчанию | Описание |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Адрес и порт HTTP-сервера |
| `POSTGRES_DSN` | `postgres://postgres:postgres@baza-postgres:5432/messenger?sslmode=disable` | Строка подключения к PostgreSQL |
| `REDIS_ADDR` | `baza-redis:6379` | Адрес Redis |
| `REDIS_PASSWORD` | пусто | Пароль Redis (если включён) |
| `JWT_SECRET` | случайный при старте | Секрет подписи JWT. **Задайте обязательно**: иначе после перезапуска все токены станут недействительны, а при нескольких экземплярах backend токены не будут совпадать |
| `ACCESS_TTL` | `15m` | Время жизни access-токена (формат Go: `15m`, `1h`) |
| `REFRESH_TTL` | `720h` (30 дней) | Время жизни refresh-токена |
| `CORS_ORIGIN` | `*` | Разрешённый origin фронтенда. Используется и для CORS, и для проверки Origin при WebSocket. В production укажите конкретный адрес |

## Migrations

Отдельного инструмента миграций нет. Схема объявлена в константе `schema` в `main.go` и применяется автоматически при каждом старте. Все выражения идемпотентны (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`), поэтому повторный запуск безопасен.

Схема:

| Таблица | Назначение |
|---|---|
| `users` | `id`, `login` (уникальный, CHECK `^[a-z]{3,32}$`), `password_hash`, `created_at` |
| `chats` | Диалог двух пользователей. `user_a < user_b` (CHECK) и `UNIQUE(user_a, user_b)` — на пару пользователей существует ровно один диалог |
| `messages` | `id`, `chat_id`, `sender_id`, `body`, `created_at`, `deleted_at` (мягкое удаление). Индекс `(chat_id, id DESC)` |

Чтобы применить схему вручную, скопируйте SQL из константы `schema` и выполните его через `psql`.

Ограничение: автоматически создаются только новые таблицы и индексы. Изменения существующих таблиц (например, новые колонки) нужно выполнять вручную или подключить инструмент миграций (goose, golang-migrate), когда схема начнёт развиваться.

## API documentation

Базовый URL: `http://localhost:8080`. Формат — JSON. Защищённые эндпоинты требуют заголовок:

```
Authorization: Bearer <access_token>
```

Формат ошибок: `{"error": "описание"}`.

### Эндпоинты

| Метод | Путь | Auth | Описание |
|---|---|---|---|
| GET | `/healthz` | нет | Проверка работоспособности |
| POST | `/api/auth/register` | нет | Регистрация |
| POST | `/api/auth/login` | нет | Вход |
| POST | `/api/auth/refresh` | нет | Обновление токенов |
| POST | `/api/auth/logout` | да | Выход |
| GET | `/api/me` | да | Текущий пользователь |
| GET | `/api/users/search?q=` | да | Поиск по префиксу логина |
| GET | `/api/chats` | да | Список последних чатов |
| POST | `/api/chats` | да | Создать или получить диалог |
| GET | `/api/chats/{id}/messages` | да | История сообщений |
| POST | `/api/chats/{id}/messages` | да | Отправить сообщение |
| DELETE | `/api/messages/{id}` | да | Удалить своё сообщение |
| GET | `/api/ws?token=` | да (через query) | WebSocket |

### Auth

**POST `/api/auth/register`**

```json
{ "login": "alex", "password": "StrongPassword123" }
```

- `201` — `TokenPair` (пользователь сразу авторизован);
- `400` — логин не соответствует `a–z` (3–32) или пароль короче 6 / длиннее 72 символов;
- `409` — `login is already taken`.

**POST `/api/auth/login`** — тело как у регистрации.

- `200` — `TokenPair`;
- `401` — `invalid login or password`.

**POST `/api/auth/refresh`** — `{ "refresh_token": "..." }`

- `200` — новый `TokenPair`. Старый refresh-токен сразу становится недействительным (ротация);
- `401` — `invalid refresh token`.

**POST `/api/auth/logout`** — `{ "refresh_token": "..." }` (поле необязательное, но рекомендуется передавать)

- `204` — refresh удалён, текущий access-токен занесён в чёрный список.

**TokenPair**

```json
{
  "access_token": "eyJ...",
  "refresh_token": "9f2c...",
  "expires_in": 900,
  "user": { "id": 1, "login": "alex", "online": false }
}
```

### Users

**GET `/api/me`** → `{ "id": 1, "login": "alex", "online": true }`

**GET `/api/users/search?q=al`** — поиск по префиксу логина (только `a–z`, до 32 символов), себя в результатах нет, максимум 20 записей. Некорректный запрос возвращает пустой массив.

```json
[ { "id": 2, "login": "alice", "online": true } ]
```

### Chats

**GET `/api/chats`** — до 100 чатов, отсортированы по времени последнего сообщения.

```json
[
  {
    "id": 5,
    "peer": { "id": 2, "login": "alice", "online": true },
    "last_message": {
      "id": 41, "chat_id": 5, "sender_id": 2,
      "body": "Привет!", "created_at": "2026-10-03T12:00:00Z"
    }
  }
]
```

`last_message` отсутствует, если сообщений ещё нет.

**POST `/api/chats`** — `{ "login": "alice" }`

- `200` — `{ "id": 5, "peer": {...} }`. Повторный вызов возвращает существующий диалог;
- `400` — попытка создать диалог с самим собой;
- `404` — пользователь не найден.

### Messages

**GET `/api/chats/{id}/messages?before=<message_id>&limit=50`**

- `limit` — 1–100, по умолчанию 50;
- `before` — вернуть сообщения с `id` меньше указанного (для подгрузки более старой истории);
- ответ — массив `Message` в хронологическом порядке (старые первыми);
- `404` — чат не найден или пользователь не его участник.

**POST `/api/chats/{id}/messages`** — `{ "body": "Привет!" }`

- `201` — `Message`;
- `400` — тело пустое или длиннее 4000 символов;
- `404` — чат не найден.

**DELETE `/api/messages/{id}`**

- `204` — сообщение удалено, обоим участникам уходит событие `message.deleted`;
- `404` — сообщение не найдено или принадлежит другому пользователю.

**Message**

```json
{ "id": 41, "chat_id": 5, "sender_id": 2, "body": "Привет!", "created_at": "2026-10-03T12:00:00Z" }
```

## Примеры API requests

Ниже `jq` используется только для удобства (можно заменить копированием токена вручную).

### Регистрация

```bash
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "login": "alex",
    "password": "StrongPassword123"
  }'
```

> **Про пример из ТЗ.** Запрос `POST /api/v1/auth/register` с полями `username`, `email`, `display_name` в текущей реализации **не поддерживается**: backend не использует префикс `/v1`, а регистрация идёт только по логину `a–z` и паролю (как в исходном описании мессенджера). Такой запрос вернёт `404`. Если нужен именно `/api/v1` и расширенный профиль, это требует изменений в `main.go` (маршруты, таблица `users`, валидация).

Повторная регистрация того же логина:

```bash
curl -i -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"login":"alex","password":"StrongPassword123"}'
# HTTP/1.1 409 Conflict
# {"error":"login is already taken"}
```

### Вход и сохранение токенов

```bash
RESP=$(curl -s -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"login":"alex","password":"StrongPassword123"}')

ACCESS=$(echo "$RESP"  | jq -r .access_token)
REFRESH=$(echo "$RESP" | jq -r .refresh_token)
```

### Текущий пользователь

```bash
curl http://localhost:8080/api/me -H "Authorization: Bearer $ACCESS"
```

### Обновление токена

```bash
curl -X POST http://localhost:8080/api/auth/refresh \
  -H "Content-Type: application/json" \
  -d "{\"refresh_token\":\"$REFRESH\"}"
```

### Поиск пользователя

```bash
curl "http://localhost:8080/api/users/search?q=al" \
  -H "Authorization: Bearer $ACCESS"
```

### Создание диалога

```bash
curl -X POST http://localhost:8080/api/chats \
  -H "Authorization: Bearer $ACCESS" \
  -H "Content-Type: application/json" \
  -d '{"login":"alice"}'
```

### Список чатов

```bash
curl http://localhost:8080/api/chats -H "Authorization: Bearer $ACCESS"
```

### Отправка сообщения

```bash
curl -X POST http://localhost:8080/api/chats/5/messages \
  -H "Authorization: Bearer $ACCESS" \
  -H "Content-Type: application/json" \
  -d '{"body":"Привет!"}'
```

### История сообщений

```bash
# последние 50
curl "http://localhost:8080/api/chats/5/messages" -H "Authorization: Bearer $ACCESS"

# предыдущие 20 перед сообщением 41
curl "http://localhost:8080/api/chats/5/messages?before=41&limit=20" \
  -H "Authorization: Bearer $ACCESS"
```

### Удаление сообщения

```bash
curl -X DELETE http://localhost:8080/api/messages/41 -H "Authorization: Bearer $ACCESS"
```

### Выход

```bash
curl -X POST http://localhost:8080/api/auth/logout \
  -H "Authorization: Bearer $ACCESS" \
  -H "Content-Type: application/json" \
  -d "{\"refresh_token\":\"$REFRESH\"}"
```

## WebSocket protocol

### Подключение

```
ws://localhost:8080/api/ws?token=<access_token>
```

Браузерный `WebSocket` не умеет передавать заголовок `Authorization`, поэтому access-токен передаётся в query-параметре `token`. Невалидный или просроченный токен приводит к `401` ещё до апгрейда соединения. Origin проверяется по `CORS_ORIGIN`.

```js
const ws = new WebSocket(`wss://api.example.com/api/ws?token=${accessToken}`);
ws.onmessage = (e) => {
  const { type, data } = JSON.parse(e.data);
  // ...
};
ws.send(JSON.stringify({ type: "message.send", data: { chat_id: 5, body: "Привет!" } }));
```

Тест из консоли (`websocat`):

```bash
websocat "ws://localhost:8080/api/ws?token=$ACCESS"
```

### Формат сообщений

Все сообщения — JSON вида `{"type": "...", "data": {...}}`.

### Клиент → сервер

| type | data | Описание |
|---|---|---|
| `message.send` | `{ "chat_id": 5, "body": "текст" }` | Отправить сообщение. Результат придёт событием `message.new` |
| `ping` | — | Прикладной ping, обновляет presence. Ответ — `pong` |

### Сервер → клиент

| type | data | Когда |
|---|---|---|
| `message.new` | `Message` | Новое сообщение в чате (получают оба участника, включая отправителя) |
| `message.deleted` | `{ "id": 41, "chat_id": 5 }` | Сообщение удалено |
| `presence` | `{ "user_id": 2, "online": true }` | Собеседник вышел в онлайн или офлайн |
| `pong` | `null` | Ответ на `ping` |
| `error` | `{ "error": "описание" }` | Ошибка обработки команды (например, неверный `chat_id` или пустое сообщение). Соединение при этом не закрывается |

### Heartbeat и presence

- Сервер отправляет WebSocket ping каждые **25 секунд**. Браузер отвечает pong автоматически.
- Если от клиента нет данных (pong или сообщений) более **60 секунд**, соединение закрывается.
- Пользователь считается online, пока у него есть хотя бы одно живое соединение (несколько вкладок поддерживаются).
- События `presence` приходят только тем, с кем у пользователя есть диалог. Начальное состояние берите из поля `online` в `GET /api/chats` и `GET /api/users/search`.

### Рекомендации для фронтенда

- Access-токен живёт 15 минут. Перед истечением вызовите `/api/auth/refresh` и переподключите WebSocket с новым токеном (токен проверяется только при подключении).
- При разрыве соединения переподключайтесь с экспоненциальной задержкой и после этого подгрузите пропущенные сообщения через `GET /api/chats/{id}/messages`.
- Если медленный клиент не успевает читать, сервер может пропустить события, поэтому история через REST — источник истины.

## Запуск tests

В проекте пока **нет автоматических тестов**. Доступные проверки:

```bash
go vet ./...        # статический анализ
go build ./...      # проверка сборки
go test ./...       # запуск тестов (после их добавления)
go test -race ./... # с детектором гонок
```

Быстрая ручная проверка работающего сервера (smoke test):

```bash
BASE=http://localhost:8080
curl -sf $BASE/healthz

curl -s -X POST $BASE/api/auth/register -H "Content-Type: application/json" \
  -d '{"login":"smokea","password":"password1"}' | jq .user
curl -s -o /dev/null -w "дубликат логина: %{http_code} (ожидается 409)\n" \
  -X POST $BASE/api/auth/register -H "Content-Type: application/json" \
  -d '{"login":"smokea","password":"password1"}'
curl -s -o /dev/null -w "неверный логин: %{http_code} (ожидается 400)\n" \
  -X POST $BASE/api/auth/register -H "Content-Type: application/json" \
  -d '{"login":"Smoke1","password":"password1"}'
```

Для интеграционных тестов (`main_test.go`) рекомендуется поднимать PostgreSQL и Redis через [testcontainers-go](https://golang.testcontainers.org/) и вызывать обработчики через `httptest.NewServer`. Тестировать стоит как минимум: регистрацию и дубликат логина, цикл login → refresh → logout, доступ к чужому чату (`404`), удаление чужого сообщения (`404`) и доставку `message.new` по WebSocket обоим участникам.
