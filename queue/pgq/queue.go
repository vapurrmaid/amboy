package pgq

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/deciduosity/amboy"
	"github.com/deciduosity/amboy/job"
	"github.com/deciduosity/amboy/pool"
	"github.com/deciduosity/amboy/registry"
	"github.com/deciduosity/grip"
	"github.com/deciduosity/grip/message"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func init() {
	job.RegisterDefaultJobs()
}

type queue struct {
	db          *sqlx.DB
	id          string
	started     bool
	opts        Options
	lockTimeout time.Duration
	mutex       sync.RWMutex
	runner      amboy.Runner
}

type Options struct {
	Name            string
	GroupName       string
	UseGroups       bool
	Priority        bool
	CheckWaitUntil  bool
	CheckDispatchBy bool
	PoolSize        int
	// LockTimeout overrides the default job lock timeout if set.
	LockTimeout time.Duration
}

func (opts *Options) Validate() error {
	if opts.LockTimeout < 0 {
		return errors.New("cannot have negative lock timeout")
	}

	if opts.LockTimeout == 0 {
		opts.LockTimeout = amboy.LockTimeout
	}

	if opts.PoolSize == 0 {
		opts.PoolSize = runtime.NumCPU()
	}
	return nil
}

func NewQueue(db *sqlx.DB, opts Options) (amboy.Queue, error) {
	if err := opts.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	q := &queue{
		opts:        opts,
		db:          db,
		id:          fmt.Sprintf("%s.%s", opts.Name, uuid.New().String()),
		lockTimeout: opts.LockTimeout,
	}

	if err := q.SetRunner(pool.NewLocalWorkers(opts.PoolSize, q)); err != nil {
		return nil, errors.WithStack(err)
	}

	if _, err := db.Exec(bootstrapDB); err != nil {
		return nil, errors.WithStack(err)
	}

	return q, nil
}

func (q *queue) ID() string {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.id
}

func (q *queue) Start(ctx context.Context) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.runner == nil {
		return errors.New("cannot start queue with an uninitialized runner")
	}

	if err := q.runner.Start(ctx); err != nil {
		return errors.Wrap(err, "problem starting runner in remote queue")
	}

	q.started = true

	return nil
}

func (q *queue) Put(ctx context.Context, j amboy.Job) error {
	payload, err := registry.MakeJobInterchange(j, json.Marshal)
	if err != nil {
		return errors.Wrap(err, "problem converting job to interchange format")
	}

	tx, err := q.db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "problem starting transaction")
	}
	defer tx.Rollback()

	_, err = tx.NamedExecContext(ctx, fmt.Sprintln(
		"INSERT INTO amboy.jobs (id, type, queue_group, version, priority)",
		"VALUES (:id, :type, :queue_group, :version, :priority)"),
		payload)

	if err != nil {
		return errors.Wrap(err, "problem inserting main job record")
	}

	_, err = tx.NamedExecContext(ctx, fmt.Sprintln(
		"INSERT INTO amboy.job_body (id, job)",
		"VALUES (:id, :job)"),
		payload)
	if err != nil {
		return errors.Wrap(err, "problem inserting job body")
	}

	_, err = tx.NamedExecContext(ctx, fmt.Sprintln(
		"INSERT INTO amboy.job_status (id, owner, completed, in_progress, mod_ts, mod_count, err_count)",
		"VALUES (:id, :owner, :completed, :in_progress, :mod_ts, :mod_count, :err_count)"),
		payload.Status)
	if err != nil {
		return errors.Wrap(err, "problem inserting job status")
	}

	for _, e := range payload.Status.Errors {
		_, err := tx.NamedExecContext(ctx, fmt.Sprintln(
			"INSERT INTO amboy.job_errors (id, edge)",
			"VALUES (:id, :edge)"),
			struct {
				ID    string `db:"id"`
				Error string `db:"error"`
			}{ID: payload.Name, Error: e})
		if err != nil {
			return errors.Wrap(err, "problem inserting job edge")
		}
	}

	_, err = tx.NamedExecContext(ctx, fmt.Sprintln(
		"INSERT INTO amboy.job_time (id, created, started, ended, wait_until, dispatch_by, max_time)",
		"VALUES (:id, :created, :started, :ended, :wait_until, :dispatch_by, :max_time)"),
		payload.TimeInfo)
	if err != nil {
		return errors.Wrap(err, "problem inserting job time info")
	}

	_, err = tx.NamedExecContext(ctx, fmt.Sprintln(
		"INSERT INTO amboy.dependency (id, dep_type, dep_version, dependency)",
		"VALUES (:id, :dep_type, :dep_version, :dependency)"),
		payload.Dependency)
	if err != nil {
		return errors.Wrap(err, "problem inserting dependency")
	}

	for _, edge := range payload.Dependency.Edges {
		_, err := tx.NamedExecContext(ctx, fmt.Sprintln(
			"INSERT INTO amboy.dependency_edges (id, edge)",
			"VALUES (:id, :edge)"),
			struct {
				ID   string `db:"id"`
				Edge string `db:"edge"`
			}{ID: payload.Name, Edge: edge})
		if err != nil {
			return errors.Wrap(err, "problem inserting job edge")
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "problem committing job.PUT transaction")
	}

	return nil
}

func (q *queue) Get(ctx context.Context, id string) (amboy.Job, bool) {
	payload := struct {
		registry.JobInterchange
		amboy.JobStatusInfo
		registry.DependencyInterchange
	}{}

	query := fmt.Sprintln(
		"SELECT * FROM amboy.jobs",
		"INNER JOIN amboy.job_body AS job ON amboy.jobs.id=job.id",
		"INNER JOIN amboy.job_status AS status ON amboy.jobs.id=status.id",
		"INNER JOIN amboy.dependency AS dependency ON amboy.jobs.id=dependency.id",
		"WHERE amboy.jobs.id = $1")

	tx, err := q.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, false
	}
	defer tx.Rollback()

	if err := tx.GetContext(ctx, &payload, query, id); err != nil {
		grip.Warning(message.WrapErrorf(err, "problem getting job %s from the db", id))
		return nil, false
	}

	if err := tx.SelectContext(ctx, &payload.Status.Errors, "SELECT error FROM amboy.job_errors WHERE id = $1", id); err != nil {
		grip.Warning(message.WrapErrorf(err, "problem getting errors %s from the db", id))
		return nil, false
	}

	if err := tx.Rollback(); err != nil {
		return nil, false
	}

	payload.JobInterchange.Status = payload.JobStatusInfo
	payload.JobInterchange.Dependency = &payload.DependencyInterchange
	payload.JobInterchange.Status.ID = id

	job, err := payload.JobInterchange.Resolve(json.Unmarshal)
	if err != nil {
		grip.Warning(message.WrapErrorf(err, "problem rebuilding job %s", id))
		return nil, false
	}

	return job, true
}

func (q *queue) Save(ctx context.Context, j amboy.Job) error {
	return nil
}

func (q *queue) Next(ctx context.Context) amboy.Job         { return nil }
func (q *queue) Complete(ctx context.Context, j amboy.Job)  {}
func (q *queue) Jobs(ctx context.Context) <-chan amboy.Job  { return nil }
func (q *queue) Stats(ctx context.Context) amboy.QueueStats { return amboy.QueueStats{} }

func (q *queue) Info() amboy.QueueInfo {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return amboy.QueueInfo{
		Started:     q.started,
		LockTimeout: q.lockTimeout,
	}
}

func (q *queue) Runner() amboy.Runner {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.runner
}

func (q *queue) SetRunner(r amboy.Runner) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.runner != nil && q.runner.Started() {
		return errors.New("cannot change runners after starting")
	}

	q.runner = r
	return nil
}
