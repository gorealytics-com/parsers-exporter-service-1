-- ─── PostgreSQL Init for Staging ────────────────────────────────
-- Auto-executed on first container start (empty volume).
-- Creates all source schemas with organisation_data, contacts, menu tables.
-- ────────────────────────────────────────────────────────────────

-- All schemas used in config.yaml
DO $$
DECLARE
    schema_name TEXT;
    schemas TEXT[] := ARRAY[
        'source_23', 'source_24', 'source_25', 'source_27', 'source_28',
        'source_29', 'source_31', 'source_32', 'source_33', 'source_36',
        'source_38', 'source_39', 'source_40', 'source_41', 'source_42',
        'source_43', 'source_44', 'source_45', 'source_49', 'source_51',
        'source_52', 'source_53', 'source_54', 'source_56', 'source_57',
        'source_58', 'source_61', 'source_63'
    ];
BEGIN
    FOREACH schema_name IN ARRAY schemas
    LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);

        -- organisation_data (matches create_organisation_data_model)
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.organisation_data (
                place_id            VARCHAR(40)   PRIMARY KEY,
                source_place_id     VARCHAR(512),
                batch_timestamp     TIMESTAMP     DEFAULT NOW(),
                batch_id            INTEGER,
                name                VARCHAR(255),
                categories          TEXT[],
                city                VARCHAR(255),
                country             VARCHAR(255),
                link                VARCHAR(1024),
                reviews             INTEGER,
                rate                DOUBLE PRECISION,
                lon                 DOUBLE PRECISION,
                lat                 DOUBLE PRECISION,
                address             VARCHAR(1024),
                phone               VARCHAR(1024),
                website             VARCHAR(1024),
                price_range         VARCHAR(255),
                opening_hrs         JSONB,
                data_hash           VARCHAR(40),
                has_changed         SMALLINT      DEFAULT 0,
                modification_timestamp TIMESTAMP,
                additional_info     JSONB,
                parent_place_id     VARCHAR(512),
                open_status         VARCHAR(64),
                alternative_name    VARCHAR(255),
                status              SMALLINT      DEFAULT 2,
                update_timestamp    TIMESTAMP     DEFAULT NOW(),
                update_batch_id     INTEGER
            )', schema_name);

        -- Indexes for organisation_data
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_%s_org_source_place
            ON %I.organisation_data (source_place_id)', schema_name, schema_name);
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_%s_org_has_changed
            ON %I.organisation_data (has_changed)', schema_name, schema_name);

        -- organisation_contacts (matches create_contacts_model)
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.organisation_contacts (
                place_id        VARCHAR(128)  PRIMARY KEY,
                link            VARCHAR(4096),
                web_link        VARCHAR(4096),
                dst_web_link    VARCHAR(4096),
                error_text      VARCHAR(4096),
                error_code      INTEGER       DEFAULT 0,
                mails           TEXT[],
                phones          TEXT[],
                facebook        TEXT[],
                facebook_msg    TEXT[],
                instagram       TEXT[],
                twitter         TEXT[],
                tiktok          TEXT[],
                linkedin        TEXT[],
                whatsapp        TEXT[],
                telegram        TEXT[],
                vkontakte       TEXT[],
                pinterest       TEXT[],
                viber           TEXT[],
                youtube         TEXT[],
                odnoklassniki   TEXT[],
                status          SMALLINT      DEFAULT 0
            )', schema_name);

        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_%s_contacts_weblink
            ON %I.organisation_contacts (web_link)', schema_name, schema_name);
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_%s_contacts_error
            ON %I.organisation_contacts (error_code)', schema_name, schema_name);
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_%s_contacts_status
            ON %I.organisation_contacts (status)', schema_name, schema_name);

        -- menu_data (matches create_menu_model)
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.menu_data (
                id                  VARCHAR(40)   PRIMARY KEY,
                place_id            VARCHAR(40),
                source_id           VARCHAR(512),
                name                VARCHAR(1024),
                top_category        VARCHAR(1024),
                category            VARCHAR(1024),
                price               VARCHAR(1024),
                weight              VARCHAR(1024),
                image_url           VARCHAR(1024),
                resolution          VARCHAR(1024),
                is_available        BOOLEAN       DEFAULT TRUE,
                update_timestamp    TIMESTAMP     DEFAULT NOW(),
                update_batch_id     INTEGER,
                hash                VARCHAR(40),
                additional_info     JSONB,
                description         VARCHAR(1024),
                volume              VARCHAR(1024),
                quantity            VARCHAR(1024)
            )', schema_name);

        RAISE NOTICE 'Schema % initialized with all tables', schema_name;
    END LOOP;
END $$;

-- Grant full access to staging user
DO $$
DECLARE
    schema_name TEXT;
    schemas TEXT[] := ARRAY[
        'source_23', 'source_24', 'source_25', 'source_27', 'source_28',
        'source_29', 'source_31', 'source_32', 'source_33', 'source_36',
        'source_38', 'source_39', 'source_40', 'source_41', 'source_42',
        'source_43', 'source_44', 'source_45', 'source_49', 'source_51',
        'source_52', 'source_53', 'source_54', 'source_56', 'source_57',
        'source_58', 'source_61', 'source_63'
    ];
BEGIN
    FOREACH schema_name IN ARRAY schemas
    LOOP
        EXECUTE format('GRANT ALL ON SCHEMA %I TO staging_user', schema_name);
        EXECUTE format('GRANT ALL ON ALL TABLES IN SCHEMA %I TO staging_user', schema_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON TABLES TO staging_user', schema_name);
    END LOOP;
END $$;

\echo '✅ All 28 PostgreSQL schemas initialized for staging'

-- ─── Spider Jobs (ARAGOG_SERVICE_PG) ────────────────────────────
CREATE TABLE IF NOT EXISTS public.spider_jobs (
    job_id           VARCHAR(40) PRIMARY KEY,
    pack_id          VARCHAR(100),
    spider_name      VARCHAR(100),
    project_name     VARCHAR(100),
    host             VARCHAR(20),
    port             INTEGER,
    args             JSONB,
    settings         JSONB,
    status           VARCHAR(50) NOT NULL,
    finish_type      VARCHAR(20),
    planned_time     TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC'),
    running_time     TIMESTAMP,
    turning_off_time TIMESTAMP,
    end_time         TIMESTAMP,
    CONSTRAINT status_check CHECK (status IN ('planned', 'running', 'turning_off', 'finished')),
    CONSTRAINT finish_type_check CHECK (finish_type IN ('self', 'kill', 'kill_force', 'exception'))
);

CREATE OR REPLACE FUNCTION update_time_based_on_status()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'running' THEN
        NEW.running_time := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'turning_off' THEN
        NEW.turning_off_time := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'finished' THEN
        NEW.end_time := CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_time_based_on_status ON spider_jobs;
CREATE TRIGGER set_time_based_on_status
BEFORE UPDATE ON spider_jobs
FOR EACH ROW
WHEN (NEW.status IS DISTINCT FROM OLD.status)
EXECUTE FUNCTION update_time_based_on_status();

GRANT ALL ON public.spider_jobs TO staging_user;

\echo '✅ spider_jobs table with trigger created'
