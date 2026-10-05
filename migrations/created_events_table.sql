CREATE TABLE events (
    event_id UUID PRIMARY KEY,

    user_id TEXT NOT NULL,

    order_id TEXT NOT NULL,

    event_type TEXT NOT NULL
        CHECK (event_type IN ('created', 'paid', 'shipped')),

    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sent', 'failed')),

    retry_count INTEGER NOT NULL DEFAULT 0,

    error_message TEXT,
    correlation_id TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);