-- Bronze Layer Tables for SPX Data Pipeline
-- Creates the OLTP storage for ingested raw data

-- Ingestion Audit (error log for DataProvider errors)
CREATE SEQUENCE IF NOT EXISTS ingestion_audit_seq;
CREATE TABLE IF NOT EXISTS ingestion_audit (
    id            BIGINT DEFAULT NEXTVAL('ingestion_audit_seq') PRIMARY KEY,
    source        VARCHAR(50),
    ticker        VARCHAR(20),
    market_date   DATE,
    received_at   TIMESTAMP DEFAULT NOW(),
    file_hash     VARCHAR(64),
    status        VARCHAR(20) DEFAULT 'SUCCESS',
    error_message VARCHAR(500)
);

-- Raw Price Stream
CREATE SEQUENCE IF NOT EXISTS raw_price_stream_seq;
CREATE TABLE IF NOT EXISTS raw_price_stream (
    id          BIGINT DEFAULT NEXTVAL('raw_price_stream_seq') PRIMARY KEY,
    ticker      VARCHAR(20),
    date        DATE,
    open        DECIMAL(18, 6),
    high        DECIMAL(18, 6),
    low         DECIMAL(18, 6),
    close       DECIMAL(18, 6),
    adj_close   DECIMAL(18, 6),
    volume      BIGINT,
    received_at TIMESTAMP DEFAULT NOW()
);

-- Raw Fundamental Index
CREATE SEQUENCE IF NOT EXISTS raw_fundamental_index_seq;
CREATE TABLE IF NOT EXISTS raw_fundamental_index (
    id          BIGINT DEFAULT NEXTVAL('raw_fundamental_index_seq') PRIMARY KEY,
    ticker      VARCHAR(20),
    report_type VARCHAR(30),
    fiscal_date DATE,
    file_path   VARCHAR(500),
    received_at TIMESTAMP DEFAULT NOW()
);

-- Raw Transcript Index
CREATE SEQUENCE IF NOT EXISTS raw_transcript_index_seq;
CREATE TABLE IF NOT EXISTS raw_transcript_index (
    id          BIGINT DEFAULT NEXTVAL('raw_transcript_index_seq') PRIMARY KEY,
    ticker      VARCHAR(20),
    event_date  DATE,
    pdf_path    VARCHAR(500),
    text_hash   VARCHAR(64),
    received_at TIMESTAMP DEFAULT NOW()
);

-- Message queue for event-driven ingestion (replaces watchdog)
CREATE SEQUENCE IF NOT EXISTS queue_messages_seq;
CREATE TABLE IF NOT EXISTS queue_messages (
    id          BIGINT DEFAULT NEXTVAL('queue_messages_seq') PRIMARY KEY,
    source      VARCHAR(50),
    msg_type    VARCHAR(100),         -- 'price_file', 'fundamental_file', 'transcript_file'
    payload     VARCHAR(1000),        -- JSON: {"filepath": "...", "ticker": "AAPL", "date": "2024-01-15"}
    status      VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, PROCESSING, DONE, FAILED
    created_at  TIMESTAMP DEFAULT NOW(),
    consumed_at TIMESTAMP,
    error_message VARCHAR(500)
);
