package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	_ "practiceRabbitMQ/docs"

	"practiceRabbitMQ/internal/broker"
	"practiceRabbitMQ/internal/config"
	"practiceRabbitMQ/internal/handler"
	"practiceRabbitMQ/internal/repository"

	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

// @title Notifications API
// @version 1.0
// @description API микросервиса уведомлений интернет-магазина.
// @host localhost:8080
// @BasePath /
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	config := config.LoadConfig()

	pool, err := pgxpool.New(
		context.Background(),
		config.DatabaseURL,
	)

	if err != nil {
		return fmt.Errorf("database connection error: %w", err)
	}

	defer pool.Close()

	if err := pool.Ping(context.Background()); err != nil {
		return fmt.Errorf("database ping error: %w", err)
	}

	fmt.Println("connected to PostgreSQL")

	repo := repository.NewRepository(pool)
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		broker.StartRabbitMQ(
			ctx,
			repo,
			config.RabbitURL,
		)
	}()

	http.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {

		switch r.Method {

		case http.MethodPost:
			handler.CreateEventHandler(repo, w, r)

		case http.MethodGet:
			handler.GetEventsHandler(repo, w, r)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}

	})

	http.HandleFunc("/api/events/", func(w http.ResponseWriter, r *http.Request) {
		handler.GetEventByIDHandler(repo, w, r)
	})

	http.HandleFunc("/api/admin/retry-pending", func(w http.ResponseWriter, r *http.Request) {
		handler.RetryPendingHandler(repo, w, r)
	})
	http.Handle(
		"/swagger/",
		httpSwagger.Handler(
			httpSwagger.URL(
				"/swagger/doc.json",
			),
		),
	)
	server := &http.Server{
		Addr: config.HTTPAddr,
	}

	serverErrors := make(chan error, 1)
	go func() {
		fmt.Println("server started on", config.HTTPAddr)

		err := server.ListenAndServe()

		if err == http.ErrServerClosed {
			err = nil
		}
		serverErrors <- err
	}()

	var serverErr error
	select {
	case serverErr = <-serverErrors:
	case <-ctx.Done():
	}
	stop()

	fmt.Println("shutting down application...")
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	err = server.Shutdown(shutdownCtx)
	if err != nil {
		fmt.Println("HTTP shutdown error:", err)
	}

	fmt.Println("HTTP server stopped")
	wg.Wait()

	fmt.Println("RabbitMQ stopped")
	if serverErr != nil {
		return fmt.Errorf("server error: %w", serverErr)
	}
	return nil
}
