package domain

import "errors"

// SchemaVersion is the major version of the outbox payload / RabbitMQ message
// envelope. It lives in the domain because it is a contract shared by both
// sides of the pipeline: the intake use-cases (order/webhook) stamp it onto
// every outbox row and message header, and the consumer-side use-cases
// (checkout/fulfillment) reject any message whose version they don't
// recognise. Keeping it here lets every use-case reference one source of
// truth without importing each other (use-cases must not depend on one
// another).
const SchemaVersion = "1"

// ErrUnknownSchemaVersion is returned by a consumer-side use-case
// (usecase/checkout, usecase/fulfillment) when a message's schemaVersion
// doesn't match SchemaVersion. AMQPConsumer treats this like any other
// processing error except it dead-letters on the FIRST attempt rather than
// retrying — a structurally-incompatible message can never succeed on retry.
var ErrUnknownSchemaVersion = errors.New("unknown schema version")
