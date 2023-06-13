package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

/*
Используем кастомный логгер, чтобы можно было легко менять вывод,
используем интерфейс, чтобы менять реализацию позже не думая о том, что сломается
*/
type LoggerImpl struct {
}

func (logger *LoggerImpl) PrintRequest(r *http.Request) {
	log.Printf("%s %s\r\n", r.Method, r.URL)
}

func (logger *LoggerImpl) Errorf(format string, v ...any) {
	log.Fatalf(format, v...)
}

type Logger interface {
	PrintRequest(r *http.Request)
	Errorf()
}

func NewLogger() *LoggerImpl {
	return &LoggerImpl{}
}

const (
	DefaultPort    = 3000
	DefaultTimeout = 1
)

func (broker *Broker) putMessage(w http.ResponseWriter, r *http.Request) {
	broker.logger.PrintRequest(r)

	queueName := r.URL.Path[1:]
	message := r.URL.Query().Get("v")

	if message == "" {
		http.Error(w, "Message is requied", http.StatusBadRequest)
		return
	}

	/*
		Здесь мы блокируем нашу мапу только на запись, так как в этот момент может и должно быть доступно чтение
	*/
	broker.queuesMux.Lock()
	defer broker.queuesMux.Unlock()

	queue, exists := broker.queues[queueName]
	if !exists {
		queue = &Queue{}
		broker.queues[queueName] = queue
	}

	queue.enqueue(message)
	w.WriteHeader(http.StatusOK)

}

func (broker *Broker) getMessage(w http.ResponseWriter, r *http.Request) {
	broker.logger.PrintRequest(r)

	queueName := r.URL.Path[1:]
	timeoutStr := r.URL.Query().Get("timeout")
	timeout := DefaultTimeout

	if timeoutStr != "" {
		timeoutParsed, err := strconv.Atoi(timeoutStr)
		if err != nil {
			http.Error(w, "Invalid timeout value", http.StatusBadRequest)
			return
		}

		timeout = timeoutParsed
	}

	queue, exists := broker.queues[queueName]

	if !exists {
		http.Error(w, "Queue not found", http.StatusNotFound)
		return
	}

	startTime := time.Now()
	for {
		message := queue.dequeue()
		if message != "" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(message))
			return
		}

		elapsedTime := time.Since(startTime)
		if timeout > 0 && elapsedTime >= time.Duration(timeout)*time.Second {
			http.Error(w, "No message available", http.StatusNotFound)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

/*
Представляет собой классическую конкурентную очередь с блокировкой на чтение или запись
*/
type Queue struct {
	messages []string
	lock     sync.RWMutex
}

func (q *Queue) enqueue(message string) {
	q.lock.Lock()
	q.messages = append(q.messages, message)
	defer q.lock.Unlock()
}

func (q *Queue) dequeue() string {
	q.lock.RLock()
	defer q.lock.RUnlock()

	for len(q.messages) == 0 {
		return ""
	}

	message := q.messages[0]
	q.messages = q.messages[1:]

	return message
}

type Broker struct {
	server *http.Server
	logger *LoggerImpl
	/*
		Здесь мы используем указатель на Queue, так как внутри него содержится мьютекс, а мы не можем копировать его.
	*/
	queues    map[string]*Queue
	queuesMux sync.RWMutex
}

func NewBroker(addr string) *Broker {
	return &Broker{
		server: &http.Server{Addr: addr},
		logger: NewLogger(),
		queues: make(map[string]*Queue),
	}
}

func (broker *Broker) Start() error {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			broker.putMessage(w, r)
		case http.MethodGet:
			broker.getMessage(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	err := broker.server.ListenAndServe()
	return err
}

func main() {
	portPtr := flag.Int("port", DefaultPort, "a port of message broker")
	flag.Parse()

	broker := NewBroker(fmt.Sprintf(":%d", *portPtr))

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		err := broker.Start()
		if err != nil && err != http.ErrServerClosed {
			broker.logger.Errorf("Listen: %s\n", err)
		}
	}()
	log.Printf("Server started on port: %d.", *portPtr)

	<-done
	log.Println("Server stopped.")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := broker.server.Shutdown(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Server shut down.")
}
