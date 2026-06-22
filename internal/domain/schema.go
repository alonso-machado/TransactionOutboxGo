package domain

// SchemaVersion is the major version of the outbox payload / RabbitMQ message
// envelope (Phase 5 Track 2.D). It lives in the domain because it is a
// contract shared by both sides of the pipeline: the ingest use-case stamps it
// onto every outbox row and message header, and the consume use-case rejects
// any message whose version it doesn't recognise. Keeping it here lets both
// use-cases reference one source of truth without importing each other
// (use-cases must not depend on one another).
const SchemaVersion = "1"
