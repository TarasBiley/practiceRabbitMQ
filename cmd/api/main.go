package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "practiceRabbitMQ/docs"

	"practiceRabbitMQ/internal/broker"
	"practiceRabbitMQ/internal/config"
	"practiceRabbitMQ/internal/handler"
	"practiceRabbitMQ/internal/middleware"
	"practiceRabbitMQ/internal/repository"

	"github.com/jackc/pgx/v5/pgxpool"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

// @title Notifications API
// @version 1.0
// @description API микросервиса уведомлений интернет-магазина.
// @host localhost:8080
// @BasePath /
func main() {
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			nil,
		),
	)

	slog.SetDefault(logger)

	if err := run(); err != nil {
		slog.Error(
			"application stopped with error",
			"error", err,
		)

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

	cfg := config.LoadConfig()

	pool, err := pgxpool.New(
		context.Background(),
		cfg.DatabaseURL,
	)
	if err != nil {
		return fmt.Errorf(
			"database connection error: %w",
			err,
		)
	}

	defer pool.Close()

	if err := pool.Ping(context.Background()); err != nil {
		return fmt.Errorf(
			"database ping error: %w",
			err,
		)
	}

	slog.Info("connected to PostgreSQL")

	repo := repository.NewRepository(pool)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		broker.StartRabbitMQ(
			ctx,
			repo,
			cfg.RabbitURL,
		)
	}()

	http.HandleFunc(
		"/api/events",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			switch r.Method {

			case http.MethodPost:
				handler.CreateEventHandler(
					repo,
					w,
					r,
				)

			case http.MethodGet:
				handler.GetEventsHandler(
					repo,
					w,
					r,
				)

			default:
				http.Error(
					w,
					"Method not allowed",
					http.StatusMethodNotAllowed,
				)
			}
		},
	)

	http.HandleFunc(
		"/api/events/",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			handler.GetEventByIDHandler(
				repo,
				w,
				r,
			)
		},
	)

	http.HandleFunc(
		"/api/admin/retry-pending",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			handler.RetryPendingHandler(
				repo,
				w,
				r,
			)
		},
	)

	http.Handle(
		"/swagger/",
		httpSwagger.Handler(
			httpSwagger.URL(
				"/swagger/doc.json",
			),
		),
	)

	server := &http.Server{
		Addr: cfg.HTTPAddr,

		// Каждый HTTP-запрос сначала проходит
		// через correlation ID middleware.
		Handler: middleware.CorrelationID(
			http.DefaultServeMux,
		),
	}

	serverErrors := make(
		chan error,
		1,
	)

	go func() {
		slog.Info(
			"HTTP server started",
			"addr", cfg.HTTPAddr,
		)

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

	slog.Info("shutting down application")

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	err = server.Shutdown(shutdownCtx)

	if err != nil {
		slog.Error(
			"HTTP shutdown error",
			"error", err,
		)
	}

	slog.Info("HTTP server stopped")

	wg.Wait()

	slog.Info("RabbitMQ stopped")

	if serverErr != nil {
		return fmt.Errorf(
			"server error: %w",
			serverErr,
		)
	}

	return nil
}
