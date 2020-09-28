package pgq

const bootstrapDB = `
CREATE SCHEMA IF NOT EXISTS amboy;

CREATE TABLE IF NOT EXISTS amboy.jobs (
id text NOT NULL PRIMARY KEY,
type text NOT NULL,
queue_group text DEFAULT ''::text NOT NULL,
version integer NOT NULL,
priority integer  NOT NULL
);

CREATE TABLE IF NOT EXISTS amboy.job_body (
id text NOT NULL PRIMARY KEY,
job jsonb NOT NULL,
FOREIGN KEY (id) REFERENCES amboy.jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS amboy.job_scopes (
id text NOT NULL,
scope text UNIQUE NOT NULL,
FOREIGN KEY (id) REFERENCES amboy.jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS amboy.job_status (
id text NOT NULL PRIMARY KEY,
owner text NOT NULL,
completed boolean NOT NULL,
in_progress boolean NOT NULL,
mod_ts timestamptz NOT NULL,
mod_count integer NOT NULL,
err_count integer NOT NULL,
FOREIGN KEY (id) REFERENCES amboy.jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS amboy.job_errors (
id text NOT NULL,
error text NOT NULL,
FOREIGN KEY (id) REFERENCES amboy.jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS amboy.job_time (
id text NOT NULL PRIMARY KEY,
created timestamptz NOT NULL,
started timestamptz NOT NULL,
ended timestamptz NOT NULL,
wait_until timestamptz NOT NULL,
dispatch_by timestamptz NOT NULL,
max_time integer NOT NULL,
FOREIGN KEY (id) REFERENCES amboy.jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS amboy.dependency (
id text NOT NULL PRIMARY KEY,
dep_type text NOT NULL,
dep_version integer NOT NULL,
dependency jsonb NOT NULL,
FOREIGN KEY (id) REFERENCES amboy.jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS amboy.dependency_edges (
id text NOT NULL NOT NULL,
edge text NOT NULL,
FOREIGN KEY (id) REFERENCES amboy.jobs(id) ON DELETE CASCADE
);
`

const getJobByID = `
SELECT
   *
FROM
   amboy.jobs
   INNER JOIN amboy.job_body AS job ON amboy.jobs.id=job.id
   INNER JOIN amboy.job_status AS status ON amboy.jobs.id=status.id
   INNER JOIN amboy.job_time AS time_info ON amboy.jobs.id=time_info.id
   INNER JOIN amboy.dependency AS dependency ON amboy.jobs.id=dependency.id
WHERE
   amboy.jobs.id = $1
`

const getErrorsForJob = `
SELECT
  error
FROM
  amboy.job_errors
WHERE
  id = $1
`

const updateJob = `
UPDATE
  amboy.jobs
SET
  type = :type,
  queue_group = :queue_group,
  version = :version,
  priority = :priority
WHERE
  id = :id
`

const updateJobBody = `
UPDATE
  amboy.job_body
SET
  job = :job
WHERE
  id = :id
`

const updateJobStatus = `
UPDATE
  amboy.job_status
SET
  owner = :owner,
  completed = :completed,
  in_progress = :in_progress,
  mod_ts = :mod_ts,
  mod_count = :mod_count,
  err_count = :err_count
WHERE
  id = :id
`

const updateJobTimeInfo = `
UPDATE
  amboy.job_time
SET
  created = :created,
  started = :started,
  ended = :ended,
  wait_until = :wait_until,
  dispatch_by = :dispatch_by,
  max_time = :max_time
WHERE
  id = :id
`

const checkCanUpdate = `
SELECT
   COUNT(*)
FROM
   amboy.jobs
   INNER JOIN amboy.job_status AS status ON amboy.jobs.id=status.id
WHERE
   amboy.jobs.id = :id
   AND (
    (status.owner = :owner
     AND status.mod_count = :mod_count - 1
     AND status.mod_ts > :lock_timeout)
    OR status.mod_ts <= :lock_timeout)`

const countTotalJobs = `
SELECT
   COUNT(*)
FROM
   amboy.jobs
WHERE
   amboy.jobs.queue_group = $1
`

const countPendingJobs = `
SELECT
   COUNT(*)
FROM
   amboy.jobs
   INNER JOIN amboy.job_status AS status ON amboy.jobs.id=status.id
WHERE
   queue_group = $1
   AND status.completed = false
`

const countInProgJobs = `
SELECT
   COUNT(*)
FROM
   amboy.jobs
   INNER JOIN amboy.job_status AS status ON amboy.jobs.id=status.id
WHERE
   queue_group = $1
   AND status.completed = false
   AND status.in_progress = true
`

const getAllJobIDs = `
SELECT
   id
FROM
   amboy.job_status AS status
ORDER BY
   status.mod_ts DESC
`

const getNextJobsBasic = `
SELECT
   amboy.jobs.id
FROM
   amboy.jobs
   INNER JOIN amboy.job_status AS status ON amboy.jobs.id=status.id
WHERE
  status.completed = false
  AND queue_group = :group_name
  AND ((status.in_progress = false) OR (status.in_progress = true AND status.mod_ts <= :lock_expires))
`

const getNextJobsTimingTemplate = `
SELECT
   amboy.jobs.id
FROM
   amboy.jobs
   INNER JOIN amboy.job_status AS status ON amboy.jobs.id=status.id
   INNER JOIN amboy.job_time AS time_info ON amboy.jobs.id=time_info.id
WHERE
  status.completed = false
  AND queue_group = :group_name
  AND ((status.in_progress = false) OR (status.in_progress = true AND status.mod_ts <= :lock_expires))
`
