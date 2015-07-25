package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"runtime"
)

// Payload -----------------------------------------------------------------------------------------

// A sequence of payload objects to process
type PayloadSeq struct {
	Id       string    `json:"id"`
	Payloads []Payload `json:"data"`
}

// A single payload item to process
type Payload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Ts    int64  `json:"ts"`
}

// Payload processing function
func (p *Payload) Process() error {
	// TODO
	log.Printf("Processing payload %v", p)
	return nil
}

// Task --------------------------------------------------------------------------------------------

// Represents a task to be run
type Task struct {
	Payload Payload
}

// Task processing function
func (t *Task) Process() error {
	return t.Payload.Process()
}

// A buffered channel that we can send tasks through.
var TaskQueue chan Task = make(chan Task)

// Worker ------------------------------------------------------------------------------------------

// Represents a worker that executes tasks
type Worker struct {
	Id          int            // Unique ID
	TaskChannel chan Task      // Receives tasks
	WorkerPool  chan chan Task // Register the task channel
	quit        chan bool      // Receives quit signal
}

// Create a new worker
func NewWorker(id int, workerPool chan chan Task) Worker {
	return Worker{Id: id, WorkerPool: workerPool, TaskChannel: make(chan Task), quit: make(chan bool)}
}

// Starts the run loop for the worker
func (w *Worker) Start() {
	go func() {
		for {
			// Register this worker with the pool.
			w.WorkerPool <- w.TaskChannel
			select {
			// Do work son!
			case task := <-w.TaskChannel:
				log.Printf("Worker %d about to process task", w.Id)
				if err := task.Process(); err != nil {
					log.Printf("Error processing task: %v", err)
				}
			// Done
			case <-w.quit:
				return
			}
		}
	}()
}

// Stop listening for work requests.
func (w *Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

// Dispatcher --------------------------------------------------------------------------------------

// Dispatches tasks to workers
type Dispatcher struct {
	WorkerPool chan chan Task
	MaxWorkers int
	Workers    []Worker
}

// Create a new dispatcher
func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Task, maxWorkers)
	workers := make([]Worker, 0, maxWorkers)
	return &Dispatcher{WorkerPool: pool, MaxWorkers: maxWorkers, Workers: workers}
}

// Stop processing and shut down all workers
func (d *Dispatcher) Stop() {
	for i := 0; i < d.MaxWorkers; i++ {
		d.Workers[i].Stop()
	}
}

// Create workers and start listening for work
func (d *Dispatcher) Run() {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.WorkerPool)
		worker.Start()
		d.Workers = append(d.Workers, worker)
	}
	go d.dispatch()
}

// Obtain a worker from the pool when a task has been received
func (d *Dispatcher) dispatch() {
	for {
		select {
		case task := <-TaskQueue:
			go func(t Task) {
				workerChannel := <-d.WorkerPool // This will block until a worker is idle
				workerChannel <- t
			}(task)
		}
	}
}

// HTTP --------------------------------------------------------------------------------------------

// Max request size
var maxBodyLength = int64(10 * 1024 * 1024) // 10MB

// Request handler function.
func Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// Decode json payload sequence
	var content = &PayloadSeq{}
	err := json.NewDecoder(io.LimitReader(r.Body, maxBodyLength)).Decode(&content)
	if err != nil {
		log.Println(err)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	// Wrap individual payload entries in tasks and queue
	for _, payload := range content.Payloads {
		TaskQueue <- Task{Payload: payload}
	}
	// Tell the client we've successfully queued the task
	w.WriteHeader(http.StatusOK)
}

// Main --------------------------------------------------------------------------------------------

func main() {
	d := NewDispatcher(runtime.NumCPU() - 1)
	d.Run()
	http.HandleFunc("/", Handler)
	err := http.ListenAndServe(":8888", nil)
	if err != nil {
		d.Stop()
		log.Fatal(err)
	}
}
