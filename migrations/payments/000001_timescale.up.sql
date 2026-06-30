-- Lifted verbatim from internal/adapter/persistence/migrate.go's former
-- MigrateTimescale: one TimescaleDB hypertable per payment method (PIX,
-- BOLETO, TRANSFER, CARTAO_CREDITO, CARTAO_DEBITO — see
-- internal/infrastructure/rabbitmq.Methods, the canonical list) plus a
-- payments VIEW = UNION ALL of all of them.
--
-- Partitioned on occurred_at (the provider's event time), NOT created_at
-- (insert time) — see .claude/plan-phase4.md Track 2: a RabbitMQ redelivery
-- of the same message gets a new created_at but carries the same
-- occurredAt, so dedup on (source_message_id, occurred_at) survives
-- redelivery while dedup on (source_message_id, created_at) would not.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS payments_pix (
  id                  uuid        NOT NULL,
  source_message_id   text        NOT NULL,
  event_id            text,
  provider_name       text,
  provider_payment_id text,
  external_payment_id text,
  payer_id            uuid,
  recipient_id        uuid,
  amount              bigint,
  currency            text,
  method              text        NOT NULL,
  method_details      jsonb,
  occurred_at         timestamptz NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_message_id, occurred_at)
);
SELECT create_hypertable('payments_pix', 'occurred_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS payments_pix_event_id_idx ON payments_pix (event_id);

CREATE TABLE IF NOT EXISTS payments_boleto (
  id                  uuid        NOT NULL,
  source_message_id   text        NOT NULL,
  event_id            text,
  provider_name       text,
  provider_payment_id text,
  external_payment_id text,
  payer_id            uuid,
  recipient_id        uuid,
  amount              bigint,
  currency            text,
  method              text        NOT NULL,
  method_details      jsonb,
  occurred_at         timestamptz NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_message_id, occurred_at)
);
SELECT create_hypertable('payments_boleto', 'occurred_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS payments_boleto_event_id_idx ON payments_boleto (event_id);

CREATE TABLE IF NOT EXISTS payments_transfer (
  id                  uuid        NOT NULL,
  source_message_id   text        NOT NULL,
  event_id            text,
  provider_name       text,
  provider_payment_id text,
  external_payment_id text,
  payer_id            uuid,
  recipient_id        uuid,
  amount              bigint,
  currency            text,
  method              text        NOT NULL,
  method_details      jsonb,
  occurred_at         timestamptz NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_message_id, occurred_at)
);
SELECT create_hypertable('payments_transfer', 'occurred_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS payments_transfer_event_id_idx ON payments_transfer (event_id);

CREATE TABLE IF NOT EXISTS payments_cartao_credito (
  id                  uuid        NOT NULL,
  source_message_id   text        NOT NULL,
  event_id            text,
  provider_name       text,
  provider_payment_id text,
  external_payment_id text,
  payer_id            uuid,
  recipient_id        uuid,
  amount              bigint,
  currency            text,
  method              text        NOT NULL,
  method_details      jsonb,
  occurred_at         timestamptz NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_message_id, occurred_at)
);
SELECT create_hypertable('payments_cartao_credito', 'occurred_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS payments_cartao_credito_event_id_idx ON payments_cartao_credito (event_id);

CREATE TABLE IF NOT EXISTS payments_cartao_debito (
  id                  uuid        NOT NULL,
  source_message_id   text        NOT NULL,
  event_id            text,
  provider_name       text,
  provider_payment_id text,
  external_payment_id text,
  payer_id            uuid,
  recipient_id        uuid,
  amount              bigint,
  currency            text,
  method              text        NOT NULL,
  method_details      jsonb,
  occurred_at         timestamptz NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_message_id, occurred_at)
);
SELECT create_hypertable('payments_cartao_debito', 'occurred_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS payments_cartao_debito_event_id_idx ON payments_cartao_debito (event_id);

CREATE OR REPLACE VIEW payments AS
  SELECT * FROM payments_pix
  UNION ALL
  SELECT * FROM payments_boleto
  UNION ALL
  SELECT * FROM payments_transfer
  UNION ALL
  SELECT * FROM payments_cartao_credito
  UNION ALL
  SELECT * FROM payments_cartao_debito;
