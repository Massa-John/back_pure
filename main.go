// Messenger backend: REST + WebSocket, PostgreSQL, Redis. Весь код и конфигурация в одном файле.
//
// Запуск:
//   go mod init messenger && go mod tidy && go run .
//
// Конфигурация через переменные окружения (значения по умолчанию рассчитаны на docker-сеть,
// где контейнеры доступны по именам baza-postgres и baza-redis).
//
// REST:
//   POST   /api/auth/register   {login, password}        -> 201 + токены
//   POST   /api/auth/login      {login, password}        -> токены
//   POST   /api/auth/refresh    {refresh_token}          -> новые токены (ротация)
//   POST   /api/auth/logout     {refresh_token?}         -> 204
//   GET    /api/me
//   GET    /api/users/search?q=abc                       -> поиск по логину (префикс)
//   GET    /api/chats                                    -> последние чаты
//   POST   /api/chats           {login}                  -> создать/получить диалог
//   GET    /api/chats/{id}/messages?before=<id>&limit=50 -> история
//   POST   /api/chats/{id}/messages {body}
//   DELETE /api/messages/{id}
//   GET    /healthz
//
// WebSocket: GET /api/ws?token=<access_token>
//   клиент -> сервер: {"type":"message.send","data":{"chat_id":1,"body":"hi"}}
//   сервер -> клиент: {"type":"message.new"|"message.deleted"|"presence"|"error","data":{...}}
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

// ---------------------------------------------------------------- config

type Config struct {
	HTTPAddr   string
	PostgresDSN string
	RedisAddr  string
	RedisPass  string
	JWTSecret  string
	AccessTTL  time.Duration
	RefreshTTL time.Duration
	CORSOrigin string // "*" или конкретный origin фронтенда
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envDur(k string, d time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func loadConfig() Config {
	c := Config{
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		PostgresDSN: env("POSTGRES_DSN", "postgres://postgres:postgres@baza-postgres:5432/messenger?sslmode=disable"),
		RedisAddr:   env("REDIS_ADDR", "baza-redis:6379"),
		RedisPass:   env("REDIS_PASSWORD", ""),
		JWTSecret:   os.Getenv("JWT_SECRET"),
		AccessTTL:   envDur("ACCESS_TTL", 15*time.Minute),
		RefreshTTL:  envDur("REFRESH_TTL", 30*24*time.Hour),
		CORSOrigin:  env("CORS_ORIGIN", "*"),
	}
	if c.JWTSecret == "" {
		c.JWTSecret = randHex(32)
		log.Println("WARNING: JWT_SECRET не задан, сгенерирован временный (токены сбросятся после перезапуска)")
	}
	return c
}

// ---------------------------------------------------------------- schema

const schema = `
CREATE TABLE IF NOT EXISTS users (
	id BIGSERIAL PRIMARY KEY,
	login TEXT NOT NULL UNIQUE CHECK (login ~ '^[a-z]{3,32}$'),
	password_hash TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS chats (
	id BIGSERIAL PRIMARY KEY,
	user_a BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	user_b BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CHECK (user_a < user_b),
	UNIQUE (user_a, user_b)
);
CREATE INDEX IF NOT EXISTS chats_user_b_idx ON chats(user_b);
CREATE TABLE IF NOT EXISTS messages (
	id BIGSERIAL PRIMARY KEY,
	chat_id BIGINT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
	sender_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	body TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	deleted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS messages_chat_idx ON messages(chat_id, id DESC);
`

// ---------------------------------------------------------------- types

type User struct {
	ID     int64  `json:"id"`
	Login  string `json:"login"`
	Online bool   `json:"online"`
}

type Message struct {
	ID        int64     `json:"id"`
	ChatID    int64     `json:"chat_id"`
	SenderID  int64     `json:"sender_id"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

type ChatItem struct {
	ID          int64    `json:"id"`
	Peer        User     `json:"peer"`
	LastMessage *Message `json:"last_message,omitempty"`
}

type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	User         User   `json:"user"`
}

type authInfo struct {
	UID int64
	JTI string
	Exp time.Time
}
type ctxKey struct{}

var loginRe = regexp.MustCompile(`^[a-z]{3,32}$`)
var prefixRe = regexp.MustCompile(`^[a-z]{1,32}$`)

// ---------------------------------------------------------------- server

type Server struct {
	cfg Config
	db  *pgxpool.Pool
	rdb *redis.Client
	hub *Hub
	up  websocket.Upgrader
}

const eventsChannel = "msgr:events"

func main() {
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Postgres (с ретраями: контейнер может ещё стартовать)
	var db *pgxpool.Pool
	var err error
	for i := 0; i < 30; i++ {
		if db, err = pgxpool.New(ctx, cfg.PostgresDSN); err == nil {
			if err = db.Ping(ctx); err == nil {
				break
			}
			db.Close()
		}
		log.Printf("postgres недоступен (%v), повтор...", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, schema); err != nil {
		log.Fatalf("миграция: %v", err)
	}

	// Redis
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPass})
	for i := 0; i < 30; i++ {
		if err = rdb.Ping(ctx).Err(); err == nil {
			break
		}
		log.Printf("redis недоступен (%v), повтор...", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	s := &Server{cfg: cfg, db: db, rdb: rdb, hub: newHub()}
	s.up = websocket.Upgrader{
		ReadBufferSize: 1024, WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			o := r.Header.Get("Origin")
			return o == "" || cfg.CORSOrigin == "*" || o == cfg.CORSOrigin
		},
	}
	go s.subscribeEvents(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /api/auth/register", s.register)
	mux.HandleFunc("POST /api/auth/login", s.login)
	mux.HandleFunc("POST /api/auth/refresh", s.refresh)
	mux.HandleFunc("POST /api/auth/logout", s.auth(s.logout))
	mux.HandleFunc("GET /api/me", s.auth(s.getMe))
	mux.HandleFunc("GET /api/users/search", s.auth(s.searchUsers))
	mux.HandleFunc("GET /api/chats", s.auth(s.listChats))
	mux.HandleFunc("POST /api/chats", s.auth(s.createChat))
	mux.HandleFunc("GET /api/chats/{id}/messages", s.auth(s.getMessages))
	mux.HandleFunc("POST /api/chats/{id}/messages", s.auth(s.postMessage))
	mux.HandleFunc("DELETE /api/messages/{id}", s.auth(s.deleteMessage))
	mux.HandleFunc("GET /api/ws", s.auth(s.serveWS))

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: s.cors(mux), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sc, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		srv.Shutdown(sc)
	}()
	log.Printf("listening on %s", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// ---------------------------------------------------------------- helpers

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func hashToken(t string) string {
	h := sha256.Sum256([]byte(t))
	return hex.EncodeToString(h[:])
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(v); err != nil {
		writeErr(w, 400, "invalid json")
		return false
	}
	return true
}

func me(r *http.Request) authInfo { return r.Context().Value(ctxKey{}).(authInfo) }

func pathID(r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	return id, err == nil && id > 0
}

func isUnique(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", s.cfg.CORSOrigin)
		h.Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
		h.Set("Vary", "Origin")
		if r.Method == http.MethodOptions {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------- auth

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := ""
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			tok = h[7:]
		} else if r.URL.Path == "/api/ws" { // браузерный WebSocket не умеет заголовки
			tok = r.URL.Query().Get("token")
		}
		if tok == "" {
			writeErr(w, 401, "unauthorized")
			return
		}
		claims := &jwt.RegisteredClaims{}
		_, err := jwt.ParseWithClaims(tok, claims, func(*jwt.Token) (any, error) { return []byte(s.cfg.JWTSecret), nil },
			jwt.WithValidMethods([]string{"HS256"}))
		uid, perr := strconv.ParseInt(claims.Subject, 10, 64)
		if err != nil || perr != nil || claims.ExpiresAt == nil {
			writeErr(w, 401, "invalid or expired token")
			return
		}
		if n, _ := s.rdb.Exists(r.Context(), "bl:"+claims.ID).Result(); n > 0 {
			writeErr(w, 401, "token revoked")
			return
		}
		ctx := context.WithValue(r.Context(), ctxKey{}, authInfo{UID: uid, JTI: claims.ID, Exp: claims.ExpiresAt.Time})
		next(w, r.WithContext(ctx))
	}
}

func (s *Server) issueTokens(ctx context.Context, u User) (TokenPair, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Subject: strconv.FormatInt(u.ID, 10), ID: randHex(8),
		IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(s.cfg.AccessTTL)),
	}
	access, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(s.cfg.JWTSecret))
	if err != nil {
		return TokenPair{}, err
	}
	refresh := randHex(32)
	if err := s.rdb.Set(ctx, "rt:"+hashToken(refresh), u.ID, s.cfg.RefreshTTL).Err(); err != nil {
		return TokenPair{}, err
	}
	return TokenPair{AccessToken: access, RefreshToken: refresh, ExpiresIn: int(s.cfg.AccessTTL.Seconds()), User: u}, nil
}

type credentials struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var in credentials
	if !decode(w, r, &in) {
		return
	}
	if !loginRe.MatchString(in.Login) {
		writeErr(w, 400, "login must contain only latin letters a-z (3-32 chars, lowercase)")
		return
	}
	if len(in.Password) < 6 || len(in.Password) > 72 {
		writeErr(w, 400, "password must be 6-72 characters")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	u := User{Login: in.Login}
	err = s.db.QueryRow(r.Context(), `INSERT INTO users(login,password_hash) VALUES($1,$2) RETURNING id`, in.Login, string(hash)).Scan(&u.ID)
	if isUnique(err) {
		writeErr(w, 409, "login is already taken")
		return
	}
	if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	tp, err := s.issueTokens(r.Context(), u)
	if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	writeJSON(w, 201, tp)
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var in credentials
	if !decode(w, r, &in) {
		return
	}
	var u User
	var hash string
	err := s.db.QueryRow(r.Context(), `SELECT id,login,password_hash FROM users WHERE login=$1`, in.Login).Scan(&u.ID, &u.Login, &hash)
	if err != nil || bcrypt.CompareHashAndPassword([]byte(hash), []byte(in.Password)) != nil {
		writeErr(w, 401, "invalid login or password")
		return
	}
	tp, err := s.issueTokens(r.Context(), u)
	if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	writeJSON(w, 200, tp)
}

func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	var in struct {
		RefreshToken string `json:"refresh_token"`
	}
	if !decode(w, r, &in) || in.RefreshToken == "" {
		return
	}
	// GETDEL: refresh-токен одноразовый (ротация)
	val, err := s.rdb.GetDel(r.Context(), "rt:"+hashToken(in.RefreshToken)).Result()
	uid, perr := strconv.ParseInt(val, 10, 64)
	if err != nil || perr != nil {
		writeErr(w, 401, "invalid refresh token")
		return
	}
	u := User{ID: uid}
	if err := s.db.QueryRow(r.Context(), `SELECT login FROM users WHERE id=$1`, uid).Scan(&u.Login); err != nil {
		writeErr(w, 401, "invalid refresh token")
		return
	}
	tp, err := s.issueTokens(r.Context(), u)
	if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	writeJSON(w, 200, tp)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	a := me(r)
	var in struct {
		RefreshToken string `json:"refresh_token"`
	}
	json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in)
	if in.RefreshToken != "" {
		s.rdb.Del(r.Context(), "rt:"+hashToken(in.RefreshToken))
	}
	if ttl := time.Until(a.Exp); ttl > 0 { // access-токен в чёрный список до истечения
		s.rdb.Set(r.Context(), "bl:"+a.JTI, 1, ttl)
	}
	w.WriteHeader(204)
}

func (s *Server) getMe(w http.ResponseWriter, r *http.Request) {
	u := User{ID: me(r).UID, Online: true}
	if err := s.db.QueryRow(r.Context(), `SELECT login FROM users WHERE id=$1`, u.ID).Scan(&u.Login); err != nil {
		writeErr(w, 404, "user not found")
		return
	}
	writeJSON(w, 200, u)
}

// ---------------------------------------------------------------- users & chats

func (s *Server) searchUsers(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	out := []User{}
	if !prefixRe.MatchString(q) {
		writeJSON(w, 200, out)
		return
	}
	rows, err := s.db.Query(r.Context(), `SELECT id,login FROM users WHERE login LIKE $1 AND id<>$2 ORDER BY login LIMIT 20`, q+"%", me(r).UID)
	if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	defer rows.Close()
	for rows.Next() {
		var u User
		if rows.Scan(&u.ID, &u.Login) == nil {
			u.Online = s.isOnline(r.Context(), u.ID)
			out = append(out, u)
		}
	}
	writeJSON(w, 200, out)
}

func (s *Server) listChats(w http.ResponseWriter, r *http.Request) {
	uid := me(r).UID
	rows, err := s.db.Query(r.Context(), `
		SELECT c.id, u.id, u.login, m.id, m.sender_id, m.body, m.created_at
		FROM chats c
		JOIN users u ON u.id = CASE WHEN c.user_a=$1 THEN c.user_b ELSE c.user_a END
		LEFT JOIN LATERAL (
			SELECT id,sender_id,body,created_at FROM messages
			WHERE chat_id=c.id AND deleted_at IS NULL ORDER BY id DESC LIMIT 1) m ON true
		WHERE c.user_a=$1 OR c.user_b=$1
		ORDER BY COALESCE(m.created_at, c.created_at) DESC LIMIT 100`, uid)
	if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	defer rows.Close()
	out := []ChatItem{}
	for rows.Next() {
		var c ChatItem
		var mid, msender *int64
		var mbody *string
		var mat *time.Time
		if rows.Scan(&c.ID, &c.Peer.ID, &c.Peer.Login, &mid, &msender, &mbody, &mat) != nil {
			continue
		}
		if mid != nil {
			c.LastMessage = &Message{ID: *mid, ChatID: c.ID, SenderID: *msender, Body: *mbody, CreatedAt: *mat}
		}
		c.Peer.Online = s.isOnline(r.Context(), c.Peer.ID)
		out = append(out, c)
	}
	writeJSON(w, 200, out)
}

func (s *Server) createChat(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Login string `json:"login"`
	}
	if !decode(w, r, &in) {
		return
	}
	uid := me(r).UID
	peer := User{}
	err := s.db.QueryRow(r.Context(), `SELECT id,login FROM users WHERE login=$1`, in.Login).Scan(&peer.ID, &peer.Login)
	if errors.Is(err, pgx.ErrNoRows) {
		writeErr(w, 404, "user not found")
		return
	} else if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	if peer.ID == uid {
		writeErr(w, 400, "cannot create chat with yourself")
		return
	}
	a, b := uid, peer.ID
	if a > b {
		a, b = b, a
	}
	var chatID int64
	err = s.db.QueryRow(r.Context(), `
		INSERT INTO chats(user_a,user_b) VALUES($1,$2)
		ON CONFLICT (user_a,user_b) DO UPDATE SET user_a=EXCLUDED.user_a RETURNING id`, a, b).Scan(&chatID)
	if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	peer.Online = s.isOnline(r.Context(), peer.ID)
	writeJSON(w, 200, ChatItem{ID: chatID, Peer: peer})
}

// chatPeer проверяет, что uid — участник чата, и возвращает собеседника.
func (s *Server) chatPeer(ctx context.Context, chatID, uid int64) (int64, error) {
	var a, b int64
	if err := s.db.QueryRow(ctx, `SELECT user_a,user_b FROM chats WHERE id=$1`, chatID).Scan(&a, &b); err != nil {
		return 0, err
	}
	switch uid {
	case a:
		return b, nil
	case b:
		return a, nil
	}
	return 0, pgx.ErrNoRows
}

// ---------------------------------------------------------------- messages

func (s *Server) getMessages(w http.ResponseWriter, r *http.Request) {
	chatID, ok := pathID(r)
	if !ok {
		writeErr(w, 400, "bad chat id")
		return
	}
	if _, err := s.chatPeer(r.Context(), chatID, me(r).UID); err != nil {
		writeErr(w, 404, "chat not found")
		return
	}
	before, _ := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.db.Query(r.Context(), `
		SELECT id,chat_id,sender_id,body,created_at FROM messages
		WHERE chat_id=$1 AND deleted_at IS NULL AND ($2=0 OR id<$2)
		ORDER BY id DESC LIMIT $3`, chatID, before, limit)
	if err != nil {
		writeErr(w, 500, "internal error")
		return
	}
	defer rows.Close()
	msgs := []Message{}
	for rows.Next() {
		var m Message
		if rows.Scan(&m.ID, &m.ChatID, &m.SenderID, &m.Body, &m.CreatedAt) == nil {
			msgs = append(msgs, m)
		}
	}
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 { // хронологический порядок
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
	writeJSON(w, 200, msgs)
}

var errBadBody = errors.New("message must be 1-4000 characters")

func (s *Server) createMessage(ctx context.Context, uid, chatID int64, body string) (Message, error) {
	body = strings.TrimSpace(body)
	if n := utf8.RuneCountInString(body); n == 0 || n > 4000 {
		return Message{}, errBadBody
	}
	peer, err := s.chatPeer(ctx, chatID, uid)
	if err != nil {
		return Message{}, err
	}
	m := Message{ChatID: chatID, SenderID: uid, Body: body}
	err = s.db.QueryRow(ctx, `INSERT INTO messages(chat_id,sender_id,body) VALUES($1,$2,$3) RETURNING id,created_at`,
		chatID, uid, body).Scan(&m.ID, &m.CreatedAt)
	if err != nil {
		return Message{}, err
	}
	s.publish(ctx, []int64{uid, peer}, "message.new", m)
	return m, nil
}

func (s *Server) postMessage(w http.ResponseWriter, r *http.Request) {
	chatID, ok := pathID(r)
	if !ok {
		writeErr(w, 400, "bad chat id")
		return
	}
	var in struct {
		Body string `json:"body"`
	}
	if !decode(w, r, &in) {
		return
	}
	m, err := s.createMessage(r.Context(), me(r).UID, chatID, in.Body)
	switch {
	case errors.Is(err, errBadBody):
		writeErr(w, 400, err.Error())
	case errors.Is(err, pgx.ErrNoRows):
		writeErr(w, 404, "chat not found")
	case err != nil:
		writeErr(w, 500, "internal error")
	default:
		writeJSON(w, 201, m)
	}
}

func (s *Server) deleteMessage(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		writeErr(w, 400, "bad message id")
		return
	}
	uid := me(r).UID
	var chatID int64
	// удалять можно только свои сообщения (soft delete)
	err := s.db.QueryRow(r.Context(), `UPDATE messages SET deleted_at=now() WHERE id=$1 AND sender_id=$2 AND deleted_at IS NULL RETURNING chat_id`,
		id, uid).Scan(&chatID)
	if err != nil {
		writeErr(w, 404, "message not found")
		return
	}
	if peer, err := s.chatPeer(r.Context(), chatID, uid); err == nil {
		s.publish(r.Context(), []int64{uid, peer}, "message.deleted", map[string]int64{"id": id, "chat_id": chatID})
	}
	w.WriteHeader(204)
}

// ---------------------------------------------------------------- events (Redis pub/sub)

type envelope struct {
	To   []int64         `json:"to"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func (s *Server) publish(ctx context.Context, to []int64, typ string, data any) {
	d, _ := json.Marshal(data)
	b, _ := json.Marshal(envelope{To: to, Type: typ, Data: d})
	if err := s.rdb.Publish(ctx, eventsChannel, b).Err(); err != nil {
		log.Printf("publish: %v", err)
	}
}

func (s *Server) subscribeEvents(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, eventsChannel)
	defer sub.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-sub.Channel():
			if !ok {
				return
			}
			var e envelope
			if json.Unmarshal([]byte(m.Payload), &e) != nil {
				continue
			}
			out, _ := json.Marshal(map[string]any{"type": e.Type, "data": e.Data})
			s.hub.deliver(e.To, out)
		}
	}
}

// ---------------------------------------------------------------- presence

func presKey(uid int64) string { return "presence:" + strconv.FormatInt(uid, 10) }

func (s *Server) isOnline(ctx context.Context, uid int64) bool {
	key := presKey(uid)
	pipe := s.rdb.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(time.Now().Unix(), 10))
	card := pipe.ZCard(ctx, key)
	pipe.Exec(ctx)
	return card.Val() > 0
}

func (s *Server) presenceTouch(ctx context.Context, uid int64, cid string) {
	s.rdb.ZAdd(ctx, presKey(uid), redis.Z{Score: float64(time.Now().Add(60 * time.Second).Unix()), Member: cid})
	s.rdb.Expire(ctx, presKey(uid), 2*time.Minute)
}

func (s *Server) notifyPresence(ctx context.Context, uid int64, online bool) {
	rows, err := s.db.Query(ctx, `SELECT CASE WHEN user_a=$1 THEN user_b ELSE user_a END FROM chats WHERE user_a=$1 OR user_b=$1`, uid)
	if err != nil {
		return
	}
	defer rows.Close()
	var peers []int64
	for rows.Next() {
		var p int64
		if rows.Scan(&p) == nil {
			peers = append(peers, p)
		}
	}
	if len(peers) > 0 {
		s.publish(ctx, peers, "presence", map[string]any{"user_id": uid, "online": online})
	}
}

// ---------------------------------------------------------------- websocket

type Client struct {
	id   string
	uid  int64
	conn *websocket.Conn
	send chan []byte
	done chan struct{}
}

type Hub struct {
	mu    sync.RWMutex
	conns map[int64]map[*Client]struct{}
}

func newHub() *Hub { return &Hub{conns: map[int64]map[*Client]struct{}{}} }

func (h *Hub) add(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns[c.uid] == nil {
		h.conns[c.uid] = map[*Client]struct{}{}
	}
	h.conns[c.uid][c] = struct{}{}
}

func (h *Hub) remove(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.conns[c.uid], c)
	if len(h.conns[c.uid]) == 0 {
		delete(h.conns, c.uid)
	}
}

func (h *Hub) deliver(to []int64, msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := map[int64]bool{}
	for _, uid := range to {
		if seen[uid] {
			continue
		}
		seen[uid] = true
		for c := range h.conns[uid] {
			select {
			case c.send <- msg:
			default: // медленный клиент: пропускаем, история доступна через REST
			}
		}
	}
}

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &Client{id: randHex(8), uid: me(r).UID, conn: conn, send: make(chan []byte, 64), done: make(chan struct{})}
	s.hub.add(c)

	bg := context.Background()
	wasOffline := !s.isOnline(bg, c.uid)
	s.presenceTouch(bg, c.uid, c.id)
	if wasOffline {
		go s.notifyPresence(bg, c.uid, true)
	}

	go c.writePump()
	s.readPump(c)

	// disconnect
	s.hub.remove(c)
	close(c.done)
	conn.Close()
	s.rdb.ZRem(bg, presKey(c.uid), c.id)
	if !s.isOnline(bg, c.uid) {
		s.notifyPresence(bg, c.uid, false)
	}
}

func (c *Client) writePump() {
	t := time.NewTicker(25 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if c.conn.WriteMessage(websocket.TextMessage, msg) != nil {
				c.conn.Close()
				return
			}
		case <-t.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if c.conn.WriteMessage(websocket.PingMessage, nil) != nil {
				c.conn.Close()
				return
			}
		}
	}
}

func (c *Client) reply(typ string, data any) {
	d, _ := json.Marshal(data)
	b, _ := json.Marshal(map[string]any{"type": typ, "data": json.RawMessage(d)})
	select {
	case c.send <- b:
	default:
	}
}

func (s *Server) readPump(c *Client) {
	c.conn.SetReadLimit(16 << 10)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		s.presenceTouch(context.Background(), c.uid, c.id)
		return nil
	})
	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		var in struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if json.Unmarshal(raw, &in) != nil {
			c.reply("error", map[string]string{"error": "invalid json"})
			continue
		}
		switch in.Type {
		case "ping":
			s.presenceTouch(context.Background(), c.uid, c.id)
			c.reply("pong", nil)
		case "message.send":
			var m struct {
				ChatID int64  `json:"chat_id"`
				Body   string `json:"body"`
			}
			if json.Unmarshal(in.Data, &m) != nil {
				c.reply("error", map[string]string{"error": "invalid data"})
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := s.createMessage(ctx, c.uid, m.ChatID, m.Body)
			cancel()
			if err != nil {
				msg := "chat not found"
				if errors.Is(err, errBadBody) {
					msg = err.Error()
				} else if !errors.Is(err, pgx.ErrNoRows) {
					msg = "internal error"
				}
				c.reply("error", map[string]string{"error": msg})
			}
		default:
			c.reply("error", map[string]string{"error": "unknown type"})
		}
	}
}
