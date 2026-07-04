-- The events database: everything order-worker and fulfillment-worker read
-- and write. Six tables. producers/event_areas are created for schema
-- completeness (a future data source may populate them) but nothing in this
-- pipeline writes to them yet — the order payload only carries event_id,
-- venue{id,name,city}, and per-ticket seating/price, so locations/events are
-- the only two upserted from it.
--
-- Sharding: orders/tickets both carry event_type/event_subtype (denormalized
-- from the Event they belong to) with a plain composite index rather than
-- declarative partitioning — the same (event_type, event_subtype) pair drives
-- RabbitMQ routing (internal/infrastructure/rabbitmq), but adding a new pair
-- here is registry-only (no CREATE PARTITION migration required). Revisit
-- partitioning if per-genre vacuum/retention independence becomes a real
-- operational need.

CREATE TABLE IF NOT EXISTS locations (
    id              uuid        NOT NULL PRIMARY KEY,
    name            text        NOT NULL,
    city            text,
    source_venue_id text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_locations_source_venue_id
    ON locations (source_venue_id);

CREATE TABLE IF NOT EXISTS producers (
    id          uuid NOT NULL PRIMARY KEY,
    name        text NOT NULL,
    logo_url    text,
    website_url text
);

CREATE TABLE IF NOT EXISTS events (
    id              uuid        NOT NULL PRIMARY KEY,
    event_type      text        NOT NULL,
    event_subtype   text        NOT NULL,
    name            text        NOT NULL,
    location_id     uuid        NOT NULL REFERENCES locations (id),
    producer_id     uuid        REFERENCES producers (id),
    source_event_id text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_source_event_id
    ON events (source_event_id);

CREATE INDEX IF NOT EXISTS idx_events_type_subtype
    ON events (event_type, event_subtype);

CREATE TABLE IF NOT EXISTS event_areas (
    id       uuid   NOT NULL PRIMARY KEY,
    event_id uuid   NOT NULL REFERENCES events (id),
    name     text,
    size     integer,
    price    bigint,
    currency text
);

CREATE INDEX IF NOT EXISTS idx_event_areas_event_id
    ON event_areas (event_id);

CREATE TABLE IF NOT EXISTS orders (
    id                 uuid        NOT NULL PRIMARY KEY,
    source_order_id    text        NOT NULL,
    event_type         text        NOT NULL,
    event_subtype      text        NOT NULL,
    source_event_id    text        NOT NULL,
    source_venue_id    text,
    venue_name         text,
    venue_city         text,
    items              jsonb       NOT NULL,
    customer_name      text,
    customer_email     text,
    customer_document  text,
    amount             bigint      NOT NULL,
    currency           text        NOT NULL,
    status             text        NOT NULL DEFAULT 'PENDING',
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_source_order_id
    ON orders (source_order_id);

CREATE INDEX IF NOT EXISTS idx_orders_type_subtype
    ON orders (event_type, event_subtype);

CREATE TABLE IF NOT EXISTS tickets (
    id               uuid        NOT NULL PRIMARY KEY,
    order_id         uuid        NOT NULL REFERENCES orders (id),
    event_id         uuid        NOT NULL REFERENCES events (id),
    source_ticket_id text        NOT NULL,
    section          text,
    row              text,
    seat             text,
    price            bigint      NOT NULL,
    currency         text        NOT NULL,
    buyer_name       text,
    buyer_email      text,
    qr_png           bytea,
    qr_content       text,
    validation_code  text,
    signature        text,
    status           text        NOT NULL DEFAULT 'RESERVED',
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tickets_source_ticket_id
    ON tickets (source_ticket_id);

CREATE INDEX IF NOT EXISTS idx_tickets_order_id
    ON tickets (order_id);

CREATE INDEX IF NOT EXISTS idx_tickets_event_id
    ON tickets (event_id);

CREATE TABLE IF NOT EXISTS charges (
    id           uuid        NOT NULL PRIMARY KEY,
    order_id     uuid        NOT NULL REFERENCES orders (id),
    provider     text        NOT NULL,
    provider_ref text        NOT NULL,
    checkout_url text,
    amount       bigint      NOT NULL,
    currency     text        NOT NULL,
    status       text        NOT NULL DEFAULT 'PENDING',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_charges_order_id
    ON charges (order_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_charges_provider_ref
    ON charges (provider_ref);
