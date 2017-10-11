package driver

import (
	"container/heap"
	"context"
	"sync"

	"github.com/mongodb/amboy"
)

// PriorityStorage is a local storage system for Jobs in priority
// order. Used by the LocalPriorityQueue, and wrapped by the
// LocalPriorityDriver for use in remote queues.
type PriorityStorage struct {
	pq    priorityQueue
	table map[string]*queueItem
	mutex sync.RWMutex
}

// NewPriorityStorage returns an initialized PriorityStorage object.
func NewPriorityStorage() *PriorityStorage {
	return &PriorityStorage{
		table: make(map[string]*queueItem),
	}
}

// Push inserts a job into the priority queue. If the Job exists (by
// ID), then this operation updates the existing job.
func (s *PriorityStorage) Push(j amboy.Job) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	name := j.ID()
	priority := j.Priority()
	item, ok := s.table[name]
	if ok && !item.job.Status().Completed {
		s.pq.update(item, priority)
		return
	}

	item = &queueItem{
		job:      j,
		priority: priority,
	}

	s.table[name] = item
	heap.Push(&s.pq, item)
}

// Pop returns the next highest priority job from the queue. If there
// are no Jobs in the queue, Pop returns nil.
func (s *PriorityStorage) Pop() amboy.Job {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.pq.Len() == 0 {
		return nil
	}

	item := heap.Pop(&s.pq).(*queueItem)
	return item.job
}

// Contents returns a generator of all jobs. tracked by this
// instance. This includes completed jobs.
func (s *PriorityStorage) Contents() <-chan amboy.Job {
	output := make(chan amboy.Job)

	go func() {
		s.mutex.RLock()
		defer s.mutex.RUnlock()

		for _, job := range s.table {
			output <- job.job
		}
		close(output)
	}()

	return output
}

// JobServer takes a channel constructed outside of this instance, and
// pushes jobs from the priority queue through that channel. The
// JobServer does not push nil jobs through the channel.
func (s *PriorityStorage) JobServer(ctx context.Context, jobs chan amboy.Job) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			job := s.Pop()
			if job == nil {
				continue
			}

			jobs <- job
		}
	}
}

// Get returns a job from the queue's storage by name, with the
// boolean value used to validate the job's existence.
func (s *PriorityStorage) Get(name string) (amboy.Job, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	job, ok := s.table[name]

	if !ok {
		return nil, false
	}

	return job.job, true
}

// Size returns the total number of jobs stored in the instance.
func (s *PriorityStorage) Size() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.table)
}

// Pending returns the total number of pending jobs waiting for
// dispatch.
func (s *PriorityStorage) Pending() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.pq.Len()
}

////////////////////////////////////////////////////////////////////////
//
// Internal implementation of a priority queue using container/heap
//
////////////////////////////////////////////////////////////////////////

type queueItem struct {
	job      amboy.Job
	priority int
	position int
}

type priorityQueue []*queueItem

func (pq priorityQueue) Len() int {
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	// Pop should return highest priority, so use greater than.
	return pq[i].priority > pq[j].priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].position = i
	pq[j].position = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*queueItem)
	item.position = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.position = -1
	*pq = old[0 : n-1]

	return item
}

func (pq *priorityQueue) update(item *queueItem, priority int) {
	item.priority = priority
	heap.Fix(pq, item.position)
}
