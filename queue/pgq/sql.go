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
