package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cdr/amboy"
	"github.com/cdr/amboy/pool"
	"github.com/cdr/grip"
	"github.com/cdr/grip/message"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// LocalPriorityQueue is an amboy.Queue implementation that dispatches
// jobs in priority order, using the Priority method of the Job
// interface to determine priority. These queues do not have shared
// storage.
type priorityLocalQueue struct {
	storage    *priorityStorage
	fixed      *fixedStorage
	channel    chan amboy.Job
	scopes     ScopeManager
	dispatcher Dispatcher
	runner     amboy.Runner
	id         string
	counters   struct {
		started   int
		completed int
		sync.RWMutex
	}
}

// NewLocalPriorityQueue constructs a new priority queue instance and
// initializes a local worker queue with the specified number of
// worker processes.
func NewLocalPriorityQueue(workers, capacity int) amboy.Queue {
	q := &priorityLocalQueue{
		scopes:  NewLocalScopeManager(),
		storage: makePriorityStorage(),
		fixed:   newFixedStorage(capacity),
		id:      fmt.Sprintf("queue.local.unordered.priority.%s", uuid.New().String()),
	}
	q.dispatcher = NewDispatcher(q)
	q.runner = pool.NewLocalWorkers(workers, q)
	return q
}

func (q *priorityLocalQueue) ID() string { return q.id }

// Put adds a job to the priority queue. If the Job already exists,
// this operation updates it in the queue, potentially reordering the
// queue accordingly.
func (q *priorityLocalQueue) Put(ctx context.Context, j amboy.Job) error {
	j.UpdateTimeInfo(amboy.JobTimeInfo{
		Created: time.Now(),
	})

	if err := j.TimeInfo().Validate(); err != nil {
		return errors.Wrap(err, "invalid job timeinfo")
	}

	return q.storage.Insert(j)
}

func (q *priorityLocalQueue) Save(ctx context.Context, j amboy.Job) error {
	q.storage.Save(j)
	return nil
}

// Get takes the name of a job and returns the job from the queue that
// matches that ID. Use the second return value to check if a job
// object with that ID exists in the queue.e
func (q *priorityLocalQueue) Get(ctx context.Context, name string) (amboy.Job, bool) {
	return q.storage.Get(name)
}

// Next returns a job for processing the queue. This may be a nil job
// if the context is canceled. Otherwise, this operation blocks until
// a job is available for dispatching.
func (q *priorityLocalQueue) Next(ctx context.Context) amboy.Job {
	for {
		select {
		case <-ctx.Done():
			return nil
		case job := <-q.channel:
			ti := job.TimeInfo()
			if ti.IsStale() {
				q.storage.Remove(job.ID())
				grip.Notice(message.Fields{
					"state":    "stale",
					"job":      job.ID(),
					"job_type": job.Type().Name,
				})
				continue
			}

			if !ti.IsDispatchable() {
				_ = q.storage.Insert(job)
				continue
			}
			if err := q.dispatcher.Dispatch(ctx, job); err != nil {
				_ = q.storage.Insert(job)
				continue
			}

			if err := q.scopes.Acquire(job.ID(), job.Scopes()); err != nil {
				_ = q.storage.Insert(job)
				continue
			}

			q.counters.Lock()
			q.counters.started++
			q.counters.Unlock()

			return job
		}
	}
}

func (q *priorityLocalQueue) Info() amboy.QueueInfo {
	return amboy.QueueInfo{
		Started:     q.channel != nil,
		LockTimeout: amboy.LockTimeout,
	}
}

// Jobs is a generator of all jobs that report as "Completed" in
// the queue.
func (q *priorityLocalQueue) Jobs(ctx context.Context) <-chan amboy.Job {
	output := make(chan amboy.Job)

	go func() {
		defer close(output)
		for job := range q.storage.Contents() {
			if ctx.Err() != nil {
				return
			}
			if job.Status().Completed {
				output <- job
			}
		}
	}()

	return output
}

// Runner returns the embedded runner instance, which provides and
// manages the worker processes.
func (q *priorityLocalQueue) Runner() amboy.Runner {
	return q.runner
}

// SetRunner allows users to override the default embedded runner. This
// is *only* possible if the queue has not started processing jobs. If
// you attempt to set the runner after the queue has started the
// operation returns an error and has no effect.
func (q *priorityLocalQueue) SetRunner(r amboy.Runner) error {
	if q.Info().Started {
		return errors.New("cannot set runner after queue is started")
	}

	q.runner = r

	return nil
}

// Stats returns an amboy.QueueStats object that reflects the queue's
// current state.
func (q *priorityLocalQueue) Stats(ctx context.Context) amboy.QueueStats {
	stats := amboy.QueueStats{
		Total:   q.storage.Size(),
		Pending: q.storage.Pending(),
	}

	q.counters.RLock()
	defer q.counters.RUnlock()

	stats.Completed = q.counters.completed
	stats.Running = q.counters.started - q.counters.completed

	return stats
}

// Complete marks a job complete. The operation is asynchronous in
// this implementation.
func (q *priorityLocalQueue) Complete(ctx context.Context, j amboy.Job) {
	if ctx.Err() != nil {
		return
	}
	id := j.ID()
	grip.Debugf("marking job (%s) as complete", id)
	q.counters.Lock()
	defer q.counters.Unlock()
	q.fixed.Push(id)

	grip.Warning(message.WrapError(
		q.scopes.Release(j.ID(), j.Scopes()),
		message.Fields{
			"id":     j.ID(),
			"scopes": j.Scopes(),
			"queue":  q.ID(),
			"op":     "releasing scope lock during completion",
		}))
	q.dispatcher.Complete(ctx, j)

	if num := q.fixed.Oversize(); num > 0 {
		for i := 0; i < num; i++ {
			q.storage.Remove(q.fixed.Pop())
		}
	}

	q.counters.completed++
}

// Start begins the work of the queue. It is a noop, without error, to
// start a queue that's been started, but the operation can error if
// there were problems starting the underlying runner instance. All
// resources are released when the context is canceled.
func (q *priorityLocalQueue) Start(ctx context.Context) error {
	if q.channel != nil {
		return nil
	}

	q.channel = make(chan amboy.Job)

	go q.storage.JobServer(ctx, q.channel)

	err := q.runner.Start(ctx)
	if err != nil {
		return err
	}

	grip.Info("job server running")

	return nil
}

func (q *priorityLocalQueue) Delete(ctx context.Context, id string) error {
	q.storage.Remove(id)

	if num := q.fixed.Delete(id); num == 0 {
		return errors.Errorf("job %s does not exist and cannot be removed", id)
	}
	return nil
}
