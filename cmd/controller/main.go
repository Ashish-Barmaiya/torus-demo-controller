package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/config"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/policy"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/ratelimit"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/request"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/server"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/torus"
)

const shutdownTimeout = 5 * time.Second

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	idSource := request.NewRandomIDSource(0)

	generator := request.NewGenerator(
		cfg.TorusBaseURL,
		idSource,
	)

	torusClient := torus.NewClient(cfg.TorusTimeout)

	executor, err := execution.NewExecutor(
		generator,
		torusClient,
		cfg.ExecutorMaxConcurrentRequests,
	)
	if err != nil {
		log.Fatalf("create executor: %v", err)
	}

	executionPolicy := policy.Default()

	limiter := ratelimit.New(
		cfg.RateLimitMaxConcurrentExecutions,
		cfg.RateLimitMaxRequestsPerWindow,
		cfg.RateLimitWindow,
	)

	app, err := server.New(
		executor,
		executionPolicy,
		limiter,
	)
	if err != nil {
		log.Fatalf("create server: %v", err)
	}

	httpServer := &http.Server{
		Addr:    cfg.Address(),
		Handler: app.Handler(),
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf(
			"controller listening on %s torus=%s",
			cfg.Address(),
			cfg.TorusBaseURL,
		)

		serverErrors <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received")

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			shutdownTimeout,
		)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)

			if closeErr := httpServer.Close(); closeErr != nil {
				log.Printf("force shutdown failed: %v", closeErr)
			}
		}

	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server stopped: %v", err)
		}
	}
}
