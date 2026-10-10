-- =====================================================================
-- PgSeries v0.2.0-dev — "TimescaleDB lite", pure SQL/PLpgSQL only.
--
-- Single-file install:  psql -v ON_ERROR_STOP=1 -f pgseries.sql
-- Runs in ONE transaction on stock Postgres 17+. No extensions,
-- no C, no shared_preload_libraries.
--
-- Scope: v0.1 (time partitioning) fully implemented.
--        v0.2 (retention policies + job control) implemented with a
--        SYNCHRONOUS dispatcher (run_job_now). The PgQue ticker /
--        vendored pgque-core is NOT bundled here; run_job_now is the
--        stand-in with the same per-job advisory-lock contract.
--        v0.6 (columnar compression) implemented in pure SQL: pluggable
--        per-column codecs (raw/for/delta/delta2/dict) over TOAST/lz4,
--        generated unpack, static read view + range fast path,
--        bench_codecs() for empirical codec comparison.
--        add_dimension + post-v0.6 APIs are honest stubs raising
--        SQLSTATE 0A000 (feature_not_supported).
--
-- Known deviations from SPEC.md (all forced by PG or pure-SQL limits):
--   * compressed siblings live in a sibling partition tree
--     (<table>_compressed), NOT attached as partitions of the series
--     table — partitions require identical rowtypes. Heap stays
--     attached+empty as staging; reads via <table>_read / _read_range.
--   * NULLs stored as int[] positions, not bitmaps (set-based pack;
--     sparse-friendly).
--   * floats are RAW only (float reassociation is not bit-exact);
--     lossy requantization is opt-in per column.
--   * lease gains a compressed->ready edge (decompress_chunk needs it;
--     the spec's API row lists decompress_chunk but no edge).
--
-- Deferred to v0.3 (documented, not silent):
--   * REVOKE of direct-to-leaf-partition writes. Harmless in v0.1:
--     chunks are real partitions and direct writes stay correct.
--     The bypass only matters once cagg statement triggers exist.
--   * pgseries-parallel-copy loader (a client script, not SQL).
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- 0. Schema + version
-- ---------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS pgseries;

CREATE TABLE IF NOT EXISTS pgseries.meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

INSERT INTO pgseries.meta (k, v) VALUES ('version', '0.2.0-dev')
ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v;

-- Placeholder for the v0.2-full PgQue vendoring. The schema exists so
-- later versions have a stable namespace; the engine itself is NOT
-- bundled in this dependency-free base.
CREATE SCHEMA IF NOT EXISTS pgseries_pgque;

-- ---------------------------------------------------------------------
-- 1. Catalog
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pgseries.series_table (
    series_schema  NAME NOT NULL,
    series_table   NAME NOT NULL,
    time_column    NAME NOT NULL,
    time_col_type  TEXT NOT NULL,
    chunk_interval INTERVAL NOT NULL,
    origin         TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (series_schema, series_table)
);

-- Dimension shape reserved since v0.1; only 'time' rows are written
-- until add_dimension lands (v0.2 full).
CREATE TABLE IF NOT EXISTS pgseries.dimension (
    dim_id             SERIAL PRIMARY KEY,
    series_schema      NAME NOT NULL,
    series_table       NAME NOT NULL,
    dim_name           NAME NOT NULL,
    dim_type           TEXT NOT NULL
                       CHECK (dim_type IN ('time', 'space_hash', 'space_range')),
    number_partitions  INTEGER NOT NULL DEFAULT 1,
    UNIQUE (series_schema, series_table, dim_name)
);

CREATE TABLE IF NOT EXISTS pgseries.chunk (
    chunk_id       SERIAL PRIMARY KEY,
    series_schema  NAME NOT NULL,
    series_table   NAME NOT NULL,
    chunk_schema   NAME NOT NULL,
    chunk_name     NAME NOT NULL,
    -- NULL bounds = adopted chunk whose partition expression the
    -- parser could not read. Such chunks are listed but NEVER
    -- auto-dropped (drop_chunks ignores NULL bounds).
    range_lower    TIMESTAMPTZ,
    range_upper    TIMESTAMPTZ,
    space_index    INTEGER NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chunk_schema, chunk_name)
);

CREATE INDEX IF NOT EXISTS chunk_series_lower_idx
    ON pgseries.chunk (series_schema, series_table, range_lower);

-- One row per chunk range. State machine:
--   pending -> ready -> compressing -> compressed
--                |          ^              |
--                |          |              v (recompression)
--                +--> leased_for_drop <-- compressing? no:
--   ready/compressed -> leased_for_drop (terminal; row deleted after DROP)
CREATE TABLE IF NOT EXISTS pgseries.chunk_lease (
    chunk_id   INTEGER PRIMARY KEY REFERENCES pgseries.chunk (chunk_id)
               ON DELETE CASCADE,
    state      TEXT NOT NULL DEFAULT 'pending'
               CHECK (state IN ('pending', 'ready', 'compressing',
                                'compressed', 'leased_for_drop')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- v0.2 job registry. Workers run synchronously via run_job_now()
-- until the PgQue ticker lands; the per-job advisory-lock contract
-- is already the real one.
CREATE TABLE IF NOT EXISTS pgseries.job (
    job_id        SERIAL PRIMARY KEY,
    job_name      TEXT NOT NULL,
    job_type      TEXT NOT NULL
                  CHECK (job_type IN ('retention', 'precreate', 'move_default')),
    target_schema NAME NOT NULL,
    target_table  NAME NOT NULL,
    enabled       BOOLEAN NOT NULL DEFAULT true,
    schedule      INTERVAL NOT NULL DEFAULT '1 day',
    config        JSONB NOT NULL DEFAULT '{}',
    last_run      TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- v0.6 compression policies (one row per series table).
CREATE TABLE IF NOT EXISTS pgseries.compression_policy (
    series_schema  NAME NOT NULL,
    series_table   NAME NOT NULL,
    segmentby      NAME[] NOT NULL DEFAULT '{}',
    orderby        NAME NOT NULL,
    orderby_desc   BOOLEAN NOT NULL DEFAULT false,
    segment_rows   INTEGER NOT NULL DEFAULT 1000,
    codecs         JSONB NOT NULL DEFAULT '{}',
    requant        JSONB NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (series_schema, series_table)
);

-- ---------------------------------------------------------------------
-- 2. Internal helpers (underscore = internal, but callable by tests)
-- ---------------------------------------------------------------------

-- Fetch + lock the series row, or raise if the table is not registered.
CREATE OR REPLACE FUNCTION pgseries._series(p_schema NAME, p_table NAME)
RETURNS pgseries.series_table
LANGUAGE plpgsql STABLE
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    r pgseries.series_table%ROWTYPE;
BEGIN
    SELECT * INTO r FROM pgseries.series_table
    WHERE series_schema = p_schema AND series_table = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: %.% is not a registered series table',
            p_schema, p_table
            USING ERRCODE = 'undefined_object';
    END IF;
    RETURN r;
END;
$$;

-- Bucket [lower, upper) containing ts under (interval, origin).
CREATE OR REPLACE FUNCTION pgseries._bucket_bounds(
    p_ts TIMESTAMPTZ, p_interval INTERVAL, p_origin TIMESTAMPTZ,
    OUT b_lower TIMESTAMPTZ, OUT b_upper TIMESTAMPTZ)
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT date_bin(p_interval, p_ts, p_origin),
           date_bin(p_interval, p_ts, p_origin) + p_interval;
$$;

-- DATE-space bucketing. date_bin() works on instants, so binning a
-- DATE via timestamptz shifts day boundaries by the session offset
-- (a real stranded-rows bug). Dates bin in date space instead,
-- anchored on origin::DATE; N must be a whole-day multiple.
CREATE OR REPLACE FUNCTION pgseries._date_bucket(
    p_d DATE, p_origin_d DATE, p_n INTEGER)
RETURNS DATE
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT p_d - (((p_d - p_origin_d) % p_n + p_n) % p_n);
$$;

-- Whole-day multiple of an interval, or NULL when there is a
-- sub-day component.
CREATE OR REPLACE FUNCTION pgseries._interval_days(p_iv INTERVAL)
RETURNS INTEGER
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT CASE WHEN mod(extract(EPOCH FROM p_iv)::BIGINT, 86400) = 0
                THEN (extract(EPOCH FROM p_iv)::BIGINT / 86400)::INTEGER
           END;
$$;

-- Canonical chunk name from the parent + bucket lower bound (UTC).
CREATE OR REPLACE FUNCTION pgseries._chunk_name(p_table NAME, p_lower TIMESTAMPTZ)
RETURNS NAME
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT (p_table::TEXT || '_p'
            || to_char(p_lower AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS'))::NAME;
$$;

-- Column lists for row-preserving copies. GENERATED (stored) columns
-- are excluded (recomputed); identity columns are included but need
-- OVERRIDING SYSTEM VALUE on INSERT (plus a setval fixup after a
-- conversion copy, since LIKE restarts the sequence).
CREATE OR REPLACE FUNCTION pgseries._copy_columns(
    p_schema NAME, p_table NAME,
    OUT v_all TEXT, OUT v_identities NAME[])
LANGUAGE plpgsql STABLE
SET search_path = pgseries, pg_catalog
AS $$
BEGIN
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
    INTO v_all
    FROM pg_attribute
    WHERE attrelid = to_regclass(format('%I.%I', p_schema, p_table))
      AND attnum > 0 AND NOT attisdropped AND attgenerated = '';

    SELECT COALESCE(array_agg(attname ORDER BY attnum), '{}')
    INTO v_identities
    FROM pg_attribute
    WHERE attrelid = to_regclass(format('%I.%I', p_schema, p_table))
      AND attnum > 0 AND NOT attisdropped AND attidentity <> '';
END;
$$;

-- Qualified DEFAULT partition name for a series table.
CREATE OR REPLACE FUNCTION pgseries._default_name(p_table NAME)
RETURNS NAME
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT (p_table::TEXT || '_default')::NAME;
$$;

-- FSM-enforcing lease transition. Raises on any undocumented edge.
CREATE OR REPLACE FUNCTION pgseries._lease_transition(
    p_chunk_schema NAME, p_chunk_name NAME, p_new_state TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_id  INTEGER;
    v_cur TEXT;
    v_ok  BOOLEAN := false;
BEGIN
    SELECT c.chunk_id, l.state INTO v_id, v_cur
    FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
    WHERE c.chunk_schema = p_chunk_schema AND c.chunk_name = p_chunk_name
    FOR UPDATE OF l;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: no chunk %.% with a lease row',
            p_chunk_schema, p_chunk_name
            USING ERRCODE = 'undefined_object';
    END IF;

    v_ok := CASE
        WHEN v_cur = 'pending'     AND p_new_state = 'ready'           THEN true
        WHEN v_cur = 'ready'       AND p_new_state = 'compressing'     THEN true
        WHEN v_cur = 'ready'       AND p_new_state = 'leased_for_drop' THEN true
        WHEN v_cur = 'compressing' AND p_new_state = 'compressed'      THEN true
        WHEN v_cur = 'compressed'  AND p_new_state = 'compressing'     THEN true
        WHEN v_cur = 'compressed'  AND p_new_state = 'leased_for_drop' THEN true
        -- v0.6 addition (spec gap: decompress_chunk API exists but no
        -- edge was specified): decompression returns to ready.
        WHEN v_cur = 'compressed'  AND p_new_state = 'ready'           THEN true
        ELSE false
    END;
    IF NOT v_ok THEN
        RAISE EXCEPTION 'pgseries: illegal lease transition % -> % for %.%',
            v_cur, p_new_state, p_chunk_schema, p_chunk_name
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    UPDATE pgseries.chunk_lease
    SET state = p_new_state, updated_at = now()
    WHERE chunk_id = v_id;
END;
$$;

-- Create + attach one chunk, register catalog + lease (pending->ready).
-- Runs inside the caller's transaction; caller holds the per-table
-- advisory lock.
CREATE OR REPLACE FUNCTION pgseries._create_chunk(
    p_schema NAME, p_table NAME, p_lower TIMESTAMPTZ, p_upper TIMESTAMPTZ)
RETURNS NAME
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s          pgseries.series_table%ROWTYPE;
    v_chunk    NAME;
    v_id       INTEGER;
    v_existing TIMESTAMPTZ;
    v_cols     TEXT;
    v_c        NAME;
BEGIN
    s := pgseries._series(p_schema, p_table);

    -- Idempotency: same lower bound already registered -> return it.
    SELECT range_lower INTO v_existing FROM pgseries.chunk
    WHERE series_schema = p_schema AND series_table = p_table
      AND range_lower = p_lower;
    IF FOUND THEN
        SELECT chunk_name INTO v_chunk FROM pgseries.chunk
        WHERE series_schema = p_schema AND series_table = p_table
          AND range_lower = p_lower;
        RETURN v_chunk;
    END IF;

    v_chunk := pgseries._chunk_name(p_table, p_lower);

    -- Name taken by an unrelated relation -> fail loudly, never squash.
    IF to_regclass(format('%I.%I', p_schema, v_chunk)) IS NOT NULL THEN
        RAISE EXCEPTION 'pgseries: chunk name %.% already exists with different bounds',
            p_schema, v_chunk
            USING ERRCODE = 'duplicate_table';
    END IF;

    EXECUTE format(
        'CREATE TABLE %I.%I (LIKE %I.%I '
        || 'INCLUDING DEFAULTS INCLUDING CONSTRAINTS '
        || 'INCLUDING GENERATED INCLUDING IDENTITY)',
        p_schema, v_chunk, p_schema, p_table);

    -- Leaves may not own identity columns (verified): the parent's
    -- shared identity governs. Generated columns must stay (verified:
    -- ATTACH demands them). Drop only the identity property.
    FOR v_c IN
        SELECT a.attname FROM pg_attribute a
        WHERE a.attrelid = to_regclass(format('%I.%I', p_schema, p_table))
          AND a.attidentity <> ''
    LOOP
        EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I DROP IDENTITY',
            p_schema, v_chunk, v_c);
    END LOOP;

    -- Load-then-attach (PG docs pattern): pull this bucket's rows out
    -- of DEFAULT into the unattached table FIRST, so the subsequent
    -- ATTACH finds no violating rows in DEFAULT and only fitting rows
    -- in the new table. Both validation scans then pass.
    SELECT c.v_all INTO v_cols
    FROM pgseries._copy_columns(p_schema, p_table) AS c;
    EXECUTE format(
        'WITH moved AS (DELETE FROM %I.%I '
        || 'WHERE %I >= %L::%s AND %I < %L::%s RETURNING *) '
        || 'INSERT INTO %I.%I (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM moved',
        p_schema, pgseries._default_name(p_table),
        s.time_column, p_lower::TEXT, s.time_col_type,
        s.time_column, p_upper::TEXT, s.time_col_type,
        p_schema, v_chunk, v_cols, v_cols);

    -- Bounds as typed literals of the real partition-key type.
    EXECUTE format(
        'ALTER TABLE %I.%I ATTACH PARTITION %I.%I '
        || 'FOR VALUES FROM (%L::%s) TO (%L::%s)',
        p_schema, p_table, p_schema, v_chunk,
        p_lower::TEXT, s.time_col_type,
        p_upper::TEXT, s.time_col_type);

    INSERT INTO pgseries.chunk
        (series_schema, series_table, chunk_schema, chunk_name,
         range_lower, range_upper, space_index)
    VALUES (p_schema, p_table, p_schema, v_chunk, p_lower, p_upper, 0)
    RETURNING chunk_id INTO v_id;

    INSERT INTO pgseries.chunk_lease (chunk_id, state) VALUES (v_id, 'pending');
    PERFORM pgseries._lease_transition(p_schema, v_chunk, 'ready');
    RETURN v_chunk;
END;
$$;

-- Serialize chunk-set mutations per series table (precreate / move /
-- drop racing each other or a second worker).
CREATE OR REPLACE FUNCTION pgseries._table_lock(p_schema NAME, p_table NAME)
RETURNS VOID
LANGUAGE sql
SET search_path = pgseries, pg_catalog
AS $$
    SELECT pg_advisory_xact_lock(
        hashtextextended('pgseries.table:' || p_schema::TEXT || '.' || p_table::TEXT, 0));
$$;

-- ---------------------------------------------------------------------
-- 3. v0.1 API — time partitioning
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pgseries.create_series_table(
    p_schema NAME, p_table NAME, p_time_column NAME,
    p_chunk_interval INTERVAL DEFAULT '1 day',
    p_origin TIMESTAMPTZ DEFAULT '2000-01-01 00:00:00+00',
    p_copy_data BOOLEAN DEFAULT true,
    p_create_brin BOOLEAN DEFAULT true)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_coltype   TEXT;
    v_tmp       NAME;
    v_default   NAME;
    v_names     NAME[];
    v_collists  TEXT[];
    v_ispkeys   BOOLEAN[];
    v_hastime   BOOLEAN[];
    i           INTEGER;
    v_cols      TEXT;
    v_idents    NAME[];
    v_seq       TEXT;
    v_max       BIGINT;
    v_c         NAME;
BEGIN
    IF to_regclass(format('%I.%I', p_schema, p_table)) IS NULL THEN
        RAISE EXCEPTION 'pgseries: table %.% does not exist', p_schema, p_table
            USING ERRCODE = 'undefined_table';
    END IF;
    IF EXISTS (SELECT 1 FROM pgseries.series_table
               WHERE series_schema = p_schema AND series_table = p_table) THEN
        RAISE EXCEPTION 'pgseries: %.% is already a series table (use adopt_partitioned_table for existing partitioned tables)',
            p_schema, p_table
            USING ERRCODE = 'duplicate_object';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_partitioned_table pt
               JOIN pg_class c ON c.oid = pt.partrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = p_schema::TEXT AND c.relname = p_table::TEXT) THEN
        RAISE EXCEPTION 'pgseries: %.% is already partitioned; use adopt_partitioned_table instead',
            p_schema, p_table
            USING ERRCODE = 'wrong_object_type';
    END IF;

    SELECT format_type(a.atttypid, NULL) INTO v_coltype
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = p_schema::TEXT AND c.relname = p_table::TEXT
      AND a.attname = p_time_column::TEXT AND a.attnum > 0 AND NOT a.attisdropped;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: column % does not exist on %.%',
            p_time_column, p_schema, p_table
            USING ERRCODE = 'undefined_column';
    END IF;
    IF v_coltype NOT IN ('timestamp with time zone', 'timestamp without time zone', 'date') THEN
        RAISE EXCEPTION 'pgseries: time column % has unsupported type % (need timestamptz, timestamp, or date)',
            p_time_column, v_coltype
            USING ERRCODE = 'wrong_object_type';
    END IF;
    -- DATE keys bin in date space, so the interval must be whole days.
    IF v_coltype = 'date'
       AND pgseries._interval_days(p_chunk_interval) IS NULL THEN
        RAISE EXCEPTION 'pgseries: date time column needs a whole-day chunk interval, got %',
            p_chunk_interval
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Core PG cannot ALTER a plain table into a partitioned one, so the
    -- swap is: rename original aside -> CREATE partitioned parent via
    -- LIKE -> reload rows -> drop aside -> re-add PK/UNIQUE (after the
    -- drop: the aside table owns same-named backing indexes until then).
    -- Uniqueness without the time column cannot exist on a partitioned
    -- parent (PG limitation), so such constraints are skipped with NOTICE.
    v_tmp := (p_table::TEXT || '_pgseries_swap')::NAME;

    -- Snapshot PK/UNIQUE definitions BEFORE the swap table is dropped;
    -- the oids are gone afterwards.
    SELECT array_agg(q.conname ORDER BY q.conname),
           array_agg(q.collist ORDER BY q.conname),
           array_agg(q.is_pkey ORDER BY q.conname),
           array_agg(q.has_time ORDER BY q.conname)
    INTO v_names, v_collists, v_ispkeys, v_hastime
    FROM (
        SELECT c.conname,
               (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY u.ord)
                FROM unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord)
                JOIN pg_attribute a ON a.attrelid = c.conrelid
                                   AND a.attnum = u.attnum) AS collist,
               c.contype = 'p' AS is_pkey,
               EXISTS (SELECT 1
                       FROM unnest(c.conkey) AS k(attnum)
                       JOIN pg_attribute a2 ON a2.attrelid = c.conrelid
                                           AND a2.attnum = k.attnum
                       WHERE a2.attname = p_time_column::TEXT) AS has_time
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = p_schema::TEXT AND t.relname = p_table::TEXT
          AND c.contype IN ('p', 'u')
    ) q;

    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', p_schema, p_table, v_tmp);

    EXECUTE format(
        'CREATE TABLE %I.%I (LIKE %I.%I '
        || 'INCLUDING DEFAULTS INCLUDING CONSTRAINTS '
        || 'INCLUDING GENERATED INCLUDING IDENTITY) '
        || 'PARTITION BY RANGE (%I)',
        p_schema, p_table, p_schema, v_tmp, p_time_column);

    v_default := pgseries._default_name(p_table);
    EXECUTE format('CREATE TABLE %I.%I PARTITION OF %I.%I DEFAULT',
        p_schema, v_default, p_schema, p_table);

    IF p_copy_data THEN
        SELECT c.v_all, c.v_identities INTO v_cols, v_idents
        FROM pgseries._copy_columns(p_schema, v_tmp) AS c;
        EXECUTE format('INSERT INTO %I.%I (%s) OVERRIDING SYSTEM VALUE '
                       || 'SELECT %s FROM %I.%I',
            p_schema, p_table, v_cols, v_cols, p_schema, v_tmp);
        -- LIKE restarts identity sequences; advance past copied values.
        FOREACH v_c IN ARRAY v_idents LOOP
            SELECT pg_get_serial_sequence(
                format('%I.%I', p_schema, p_table), v_c::TEXT) INTO v_seq;
            EXECUTE format('SELECT max(%I) FROM %I.%I',
                v_c, p_schema, p_table) INTO v_max;
            IF v_max IS NOT NULL THEN
                PERFORM setval(v_seq::REGCLASS, v_max);
            END IF;
        END LOOP;
    END IF;
    EXECUTE format('DROP TABLE %I.%I', p_schema, v_tmp);

    -- Re-add PK/UNIQUE covering the partition key now that the aside
    -- table (owner of the same-named backing indexes) is gone.
    IF v_names IS NOT NULL THEN
        FOR i IN 1 .. array_length(v_names, 1) LOOP
            IF v_hastime[i] THEN
                EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s (%s)',
                    p_schema, p_table, v_names[i],
                    CASE WHEN v_ispkeys[i] THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,
                    v_collists[i]);
            ELSE
                RAISE NOTICE 'pgseries: skipping constraint % (does not include partition key %)',
                    v_names[i], p_time_column;
            END IF;
        END LOOP;
    END IF;

    INSERT INTO pgseries.series_table
        (series_schema, series_table, time_column, time_col_type,
         chunk_interval, origin)
    VALUES (p_schema, p_table, p_time_column, v_coltype,
            p_chunk_interval, p_origin);

    INSERT INTO pgseries.dimension
        (series_schema, series_table, dim_name, dim_type, number_partitions)
    VALUES (p_schema, p_table, p_time_column, 'time', 1);

    IF p_create_brin THEN
        -- Cascades to every future chunk automatically (verified on
        -- PG 17/18): the pre-creator needs no per-chunk index step.
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON %I.%I USING BRIN (%I)',
            (p_table::TEXT || '_time_brin')::NAME,
            p_schema, p_table, p_time_column);
    END IF;

    -- Every series table ships with its two background jobs; the
    -- synchronous run_job_now() executes them until the v0.2 ticker lands.
    INSERT INTO pgseries.job (job_name, job_type, target_schema, target_table, schedule)
    VALUES (p_table::TEXT || ':precreate', 'precreate', p_schema, p_table, p_chunk_interval),
           (p_table::TEXT || ':move_default', 'move_default', p_schema, p_table, '5 minutes');

    EXECUTE format('GRANT SELECT ON %I.%I TO pgseries_reader',
        p_schema, p_table);
    EXECUTE format(
        'GRANT SELECT, INSERT, UPDATE, DELETE ON %I.%I TO pgseries_writer',
        p_schema, p_table);
END;
$$;

CREATE OR REPLACE FUNCTION pgseries.adopt_partitioned_table(
    p_schema NAME, p_table NAME, p_time_column NAME,
    p_chunk_interval INTERVAL DEFAULT '1 day',
    p_origin TIMESTAMPTZ DEFAULT '2000-01-01 00:00:00+00')
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_coltype TEXT;
    v_partkey TEXT;
    v_default NAME;
    r         RECORD;
    v_bnd     TEXT;
    v_lo_txt  TEXT;
    v_hi_txt  TEXT;
    v_lo      TIMESTAMPTZ;
    v_hi      TIMESTAMPTZ;
    v_id      INTEGER;
BEGIN
    IF EXISTS (SELECT 1 FROM pgseries.series_table
               WHERE series_schema = p_schema AND series_table = p_table) THEN
        RAISE EXCEPTION 'pgseries: %.% is already a series table',
            p_schema, p_table
            USING ERRCODE = 'duplicate_object';
    END IF;

    SELECT format_type(a.atttypid, NULL) INTO v_coltype
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = p_schema::TEXT AND c.relname = p_table::TEXT
      AND a.attname = p_time_column::TEXT AND a.attnum > 0 AND NOT a.attisdropped;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: column % does not exist on %.%',
            p_time_column, p_schema, p_table
            USING ERRCODE = 'undefined_column';
    END IF;

    -- Must be RANGE-partitioned with the time column as partition key.
    SELECT pg_get_partkeydef(pt.partrelid) INTO v_partkey
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = p_schema::TEXT AND c.relname = p_table::TEXT
      AND pt.partstrat = 'r';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: %.% is not RANGE-partitioned', p_schema, p_table
            USING ERRCODE = 'wrong_object_type';
    END IF;
    IF v_partkey IS NULL
       OR position(quote_ident(p_time_column::TEXT) IN v_partkey) = 0 THEN
        RAISE EXCEPTION 'pgseries: %.% is not partitioned on column % (%)',
            p_schema, p_table, p_time_column, v_partkey
            USING ERRCODE = 'wrong_object_type';
    END IF;

    INSERT INTO pgseries.series_table
        (series_schema, series_table, time_column, time_col_type,
         chunk_interval, origin)
    VALUES (p_schema, p_table, p_time_column, v_coltype,
            p_chunk_interval, p_origin);

    INSERT INTO pgseries.dimension
        (series_schema, series_table, dim_name, dim_type, number_partitions)
    VALUES (p_schema, p_table, p_time_column, 'time', 1);

    -- Register every attached leaf (except DEFAULT) without moving data.
    FOR r IN
        SELECT cn.nspname AS cschema, cc.relname AS cname,
               pg_get_expr(cc.relpartbound, cc.oid) AS bnd
        FROM pg_inherits inh
        JOIN pg_class par ON par.oid = inh.inhparent
        JOIN pg_namespace pn ON pn.oid = par.relnamespace
        JOIN pg_class cc ON cc.oid = inh.inhrelid
        JOIN pg_namespace cn ON cn.oid = cc.relnamespace
        WHERE pn.nspname = p_schema::TEXT AND par.relname = p_table::TEXT
    LOOP
        IF r.bnd = 'DEFAULT' THEN
            CONTINUE;
        END IF;
        BEGIN
            v_lo_txt := substring(r.bnd from $re$FROM \('([^']+)'\)$re$);
            v_hi_txt := substring(r.bnd from $re$TO \('([^']+)'\)$re$);
            IF v_lo_txt IS NULL OR v_hi_txt IS NULL THEN
                RAISE EXCEPTION 'unparseable bound';
            END IF;
            v_lo := v_lo_txt::TIMESTAMPTZ;
            v_hi := v_hi_txt::TIMESTAMPTZ;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pgseries: chunk %.% bounds not parsed (%), registered with NULL bounds; excluded from auto-drop',
                r.cschema, r.cname, r.bnd;
            v_lo := NULL;
            v_hi := NULL;
        END;
        INSERT INTO pgseries.chunk
            (series_schema, series_table, chunk_schema, chunk_name,
             range_lower, range_upper, space_index)
        VALUES (p_schema, p_table, r.cschema, r.cname, v_lo, v_hi, 0)
        ON CONFLICT (chunk_schema, chunk_name) DO NOTHING
        RETURNING chunk_id INTO v_id;
        IF FOUND THEN
            INSERT INTO pgseries.chunk_lease (chunk_id, state)
            VALUES (v_id, 'ready');
        END IF;
    END LOOP;

    -- Every series table needs a DEFAULT partition to stash misses.
    v_default := pgseries._default_name(p_table);
    IF to_regclass(format('%I.%I', p_schema, v_default)) IS NULL THEN
        EXECUTE format('CREATE TABLE %I.%I PARTITION OF %I.%I DEFAULT',
            p_schema, v_default, p_schema, p_table);
        RAISE NOTICE 'pgseries: created missing DEFAULT partition %.%',
            p_schema, v_default;
    END IF;

    INSERT INTO pgseries.job (job_name, job_type, target_schema, target_table, schedule)
    VALUES (p_table::TEXT || ':precreate', 'precreate', p_schema, p_table, p_chunk_interval),
           (p_table::TEXT || ':move_default', 'move_default', p_schema, p_table, '5 minutes');

    EXECUTE format('GRANT SELECT ON %I.%I TO pgseries_reader',
        p_schema, p_table);
    EXECUTE format(
        'GRANT SELECT, INSERT, UPDATE, DELETE ON %I.%I TO pgseries_writer',
        p_schema, p_table);
END;
$$;

-- Create N chunks ahead of the write frontier. Idempotent: re-runs
-- create nothing and return 0.
CREATE OR REPLACE FUNCTION pgseries.precreate_chunks(
    p_schema NAME, p_table NAME, p_n INTEGER DEFAULT 7)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s      pgseries.series_table%ROWTYPE;
    v_base TIMESTAMPTZ;
    v_lo   TIMESTAMPTZ;
    v_hi   TIMESTAMPTZ;
    i      INTEGER;
    v_made INTEGER := 0;
    v_name NAME;
    v_days INTEGER;
    v_base_d DATE;
    v_lo_d   DATE;
BEGIN
    IF p_n IS NULL OR p_n < 1 THEN
        RAISE EXCEPTION 'pgseries: n must be >= 1'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    s := pgseries._series(p_schema, p_table);
    PERFORM pgseries._table_lock(p_schema, p_table);

    -- DATE keys: frontier in date space (see _date_bucket).
    IF s.time_col_type = 'date' THEN
        v_days := pgseries._interval_days(s.chunk_interval);
        v_base_d := pgseries._date_bucket(
            CURRENT_DATE, s.origin::DATE, v_days);
        FOR i IN 0 .. p_n - 1 LOOP
            v_lo_d := v_base_d + v_days * i;
            v_lo := v_lo_d::TIMESTAMPTZ;
            v_hi := (v_lo_d + v_days)::TIMESTAMPTZ;
            IF NOT EXISTS (SELECT 1 FROM pgseries.chunk
                           WHERE series_schema = p_schema
                             AND series_table = p_table
                             AND range_lower = v_lo) THEN
                v_name := pgseries._create_chunk(p_schema, p_table, v_lo, v_hi);
                v_made := v_made + 1;
            END IF;
        END LOOP;
        RETURN v_made;
    END IF;

    SELECT date_bin(s.chunk_interval, now(), s.origin) INTO v_base;
    FOR i IN 0 .. p_n - 1 LOOP
        v_lo := v_base + (s.chunk_interval * i);
        v_hi := v_lo + s.chunk_interval;
        IF NOT EXISTS (SELECT 1 FROM pgseries.chunk
                       WHERE series_schema = p_schema
                         AND series_table = p_table
                         AND range_lower = v_lo) THEN
            v_name := pgseries._create_chunk(p_schema, p_table, v_lo, v_hi);
            v_made := v_made + 1;
        END IF;
    END LOOP;
    RETURN v_made;
END;
$$;

-- Move misrouted rows out of the DEFAULT partition into freshly
-- created chunks. Returns rows moved. Rows with NULL time cannot be
-- routed and are left in place (NOTICE).
CREATE OR REPLACE FUNCTION pgseries.move_default(p_schema NAME, p_table NAME)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s          pgseries.series_table%ROWTYPE;
    v_default  NAME;
    v_nulls    BIGINT;
    v_total    BIGINT := 0;
    v_moved    BIGINT;
    r          RECORD;
    v_days     INTEGER;
    v_origin_d DATE;
    v_cols     TEXT;
BEGIN
    s := pgseries._series(p_schema, p_table);
    PERFORM pgseries._table_lock(p_schema, p_table);
    v_default := pgseries._default_name(p_table);
    SELECT c.v_all INTO v_cols
    FROM pgseries._copy_columns(p_schema, p_table) AS c;

    EXECUTE format('SELECT count(*) FROM %I.%I WHERE %I IS NULL',
        p_schema, v_default, s.time_column) INTO v_nulls;
    IF v_nulls > 0 THEN
        RAISE NOTICE 'pgseries: % NULL-time rows stay in %.% (unroutable)',
            v_nulls, p_schema, v_default;
    END IF;

    -- DATE keys: group in date space so buckets match the partition
    -- bounds exactly (see _date_bucket).
    IF s.time_col_type = 'date' THEN
        v_days := pgseries._interval_days(s.chunk_interval);
        v_origin_d := s.origin::DATE;
        FOR r IN EXECUTE format(
            'SELECT pgseries._date_bucket(%I, %L::DATE, %s) AS b, count(*) AS n '
            'FROM %I.%I WHERE %I IS NOT NULL GROUP BY 1 ORDER BY 1',
            s.time_column, v_origin_d, v_days,
            p_schema, v_default, s.time_column)
        LOOP
            PERFORM pgseries._create_chunk(
                p_schema, p_table,
                r.b::TIMESTAMPTZ, (r.b + v_days)::TIMESTAMPTZ);
            EXECUTE format(
                'WITH moved AS (DELETE FROM %I.%I '
                || 'WHERE %I >= %L::DATE AND %I < %L::DATE RETURNING *) '
                || 'INSERT INTO %I.%I (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM moved',
                p_schema, v_default,
                s.time_column, r.b::TEXT,
                s.time_column, (r.b + v_days)::TEXT,
                p_schema, p_table, v_cols, v_cols);
            GET DIAGNOSTICS v_moved = ROW_COUNT;
            v_total := v_total + r.n + v_moved;
        END LOOP;
        RETURN v_total;
    END IF;

    FOR r IN EXECUTE format(
        'SELECT date_bin(%L::INTERVAL, %I::TIMESTAMPTZ, %L::TIMESTAMPTZ) AS b, count(*) AS n '
        'FROM %I.%I WHERE %I IS NOT NULL GROUP BY 1 ORDER BY 1',
        s.chunk_interval, s.time_column, s.origin,
        p_schema, v_default, s.time_column)
    LOOP
        -- _create_chunk drains this bucket from DEFAULT into the new
        -- table pre-ATTACH (r.n rows); the DELETE below only catches
        -- rows that arrived concurrently after the grouping snapshot.
        PERFORM pgseries._create_chunk(
            p_schema, p_table, r.b, r.b + s.chunk_interval);
        EXECUTE format(
            'WITH moved AS (DELETE FROM %I.%I '
            || 'WHERE %I >= %L::%s AND %I < %L::%s RETURNING *) '
            || 'INSERT INTO %I.%I (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM moved',
            p_schema, v_default,
            s.time_column, r.b::TEXT, s.time_col_type,
            s.time_column, (r.b + s.chunk_interval)::TEXT, s.time_col_type,
            p_schema, p_table, v_cols, v_cols);
        GET DIAGNOSTICS v_moved = ROW_COUNT;
        v_total := v_total + r.n + v_moved;
    END LOOP;
    RETURN v_total;
END;
$$;

CREATE OR REPLACE FUNCTION pgseries.show_chunks(p_schema NAME, p_table NAME)
RETURNS TABLE (
    chunk_schema NAME, chunk_name NAME,
    range_lower TIMESTAMPTZ, range_upper TIMESTAMPTZ,
    space_index INTEGER, state TEXT)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
    SELECT c.chunk_schema, c.chunk_name, c.range_lower, c.range_upper,
           c.space_index, l.state
    FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
    WHERE c.series_schema = p_schema AND c.series_table = p_table
    ORDER BY c.range_lower NULLS LAST, c.chunk_name;
$$;

-- Drop chunks with range_upper < now() - p_older_than.
-- Chunks with NULL bounds are never dropped. Each drop claims the
-- lease (ready/compressed -> leased_for_drop) inside the same
-- transaction as the DROP, closing the retention-vs-compression race.
CREATE OR REPLACE FUNCTION pgseries.drop_chunks(
    p_schema NAME, p_table NAME, p_older_than INTERVAL,
    p_dry_run BOOLEAN DEFAULT false)
RETURNS SETOF TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_cutoff TIMESTAMPTZ := now() - p_older_than;
    r        RECORD;
BEGIN
    PERFORM pgseries._series(p_schema, p_table);
    -- v0.3+ will refuse chunks overlapping a cagg refresh window here.
    -- No caggs are registered yet, so the check is a no-op by construction.
    PERFORM pgseries._table_lock(p_schema, p_table);

    FOR r IN
        SELECT c.chunk_id, c.chunk_schema, c.chunk_name
        FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
        WHERE c.series_schema = p_schema AND c.series_table = p_table
          AND c.range_upper IS NOT NULL AND c.range_upper < v_cutoff
          AND l.state IN ('ready', 'compressed')
        ORDER BY c.range_upper
        FOR UPDATE OF l
    LOOP
        IF p_dry_run THEN
            RETURN NEXT (r.chunk_schema::TEXT || '.' || r.chunk_name::TEXT);
            CONTINUE;
        END IF;
        PERFORM pgseries._lease_transition(
            r.chunk_schema, r.chunk_name, 'leased_for_drop');
        EXECUTE format('DROP TABLE %I.%I', r.chunk_schema, r.chunk_name);
        -- Compressed sibling goes with the chunk (v0.6).
        IF to_regclass(format('%I.%I', r.chunk_schema,
                              (r.chunk_name::TEXT || '_compressed')::NAME)) IS NOT NULL THEN
            EXECUTE format('DROP TABLE %I.%I', r.chunk_schema,
                           (r.chunk_name::TEXT || '_compressed')::NAME);
        END IF;
        DELETE FROM pgseries.chunk WHERE chunk_id = r.chunk_id;
        RETURN NEXT (r.chunk_schema::TEXT || '.' || r.chunk_name::TEXT);
    END LOOP;
END;
$$;

-- Print-mode GUC bundle (default). Apply mode cannot EXECUTE the
-- change itself — plpgsql functions always run inside a transaction
-- block, where ALTER SYSTEM is forbidden — so it returns the exact
-- superuser command to run instead. synchronous_commit is
-- per-role/session and is never applied (reported with a note).
CREATE OR REPLACE FUNCTION pgseries.tune(p_apply BOOLEAN DEFAULT false)
RETURNS TABLE (
    setting_name TEXT, recommended TEXT, applied BOOLEAN, note TEXT)
LANGUAGE plpgsql
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN VALUES
        ('wal_compression', 'on', 'reduces WAL volume for append-only ingest'),
        ('max_wal_size', '16GB', 'fewer checkpoints under bulk load'),
        ('checkpoint_timeout', '15min', 'fewer checkpoints under bulk load'),
        ('checkpoint_completion_target', '0.9', 'spread checkpoint I/O'),
        ('wal_buffers', '64MB', 'absorb bursty writers'),
        ('effective_io_concurrency', '200', 'NVMe-friendly planner costing')
    LOOP
        applied := false;
        setting_name := rec.column1;
        recommended := rec.column2;
        IF p_apply THEN
            note := format('run as superuser outside a transaction: '
                           || 'ALTER SYSTEM SET %I = %s; SELECT pg_reload_conf();',
                           rec.column1, rec.column2);
        ELSE
            note := rec.column3;
        END IF;
        RETURN NEXT;
    END LOOP;

    setting_name := 'synchronous_commit';
    recommended := 'off';
    applied := false;
    note := 'per-role/session only (no per-table GUC): '
         || 'ALTER ROLE app IN DATABASE db SET synchronous_commit = off';
    RETURN NEXT;
END;
$$;

-- ---------------------------------------------------------------------
-- 4. v0.2 API — retention policies + job control (synchronous runner)
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pgseries.add_retention_policy(
    p_schema NAME, p_table NAME,
    p_retention INTERVAL, p_schedule INTERVAL DEFAULT '1 day')
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_id INTEGER;
BEGIN
    PERFORM pgseries._series(p_schema, p_table);
    IF p_retention IS NULL OR p_retention <= '0 seconds'::INTERVAL THEN
        RAISE EXCEPTION 'pgseries: retention must be a positive interval'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    INSERT INTO pgseries.job (job_name, job_type, target_schema, target_table,
                              schedule, config)
    VALUES (p_table::TEXT || ':retention', 'retention', p_schema, p_table,
            p_schedule, jsonb_build_object('retention', p_retention::TEXT))
    RETURNING job_id INTO v_id;
    RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION pgseries.remove_retention_policy(p_job_id INTEGER)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
BEGIN
    DELETE FROM pgseries.job WHERE job_id = p_job_id AND job_type = 'retention';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: no retention job %', p_job_id
            USING ERRCODE = 'undefined_object';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pgseries.alter_job(
    p_job_id INTEGER, p_enabled BOOLEAN DEFAULT NULL,
    p_schedule INTERVAL DEFAULT NULL)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
BEGIN
    UPDATE pgseries.job
    SET enabled = COALESCE(p_enabled, enabled),
        schedule = COALESCE(p_schedule, schedule)
    WHERE job_id = p_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: no job %', p_job_id
            USING ERRCODE = 'undefined_object';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pgseries.pause_job(p_job_id INTEGER)
RETURNS VOID
LANGUAGE sql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
    SELECT pgseries.alter_job(p_job_id, false);
$$;

CREATE OR REPLACE FUNCTION pgseries.resume_job(p_job_id INTEGER)
RETURNS VOID
LANGUAGE sql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
    SELECT pgseries.alter_job(p_job_id, true);
$$;

-- Synchronous job runner: takes the per-job advisory lock (the same
-- contract the v0.2 PgQue ticker will use) and executes the body.
CREATE OR REPLACE FUNCTION pgseries.run_job_now(p_job_id INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    j       pgseries.job%ROWTYPE;
    v_n     BIGINT;
    v_ret   INTERVAL;
BEGIN
    SELECT * INTO j FROM pgseries.job WHERE job_id = p_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: no job %', p_job_id
            USING ERRCODE = 'undefined_object';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('pgseries.job:' || p_job_id::TEXT, 0));

    CASE j.job_type
        WHEN 'precreate' THEN
            SELECT pgseries.precreate_chunks(
                j.target_schema, j.target_table, 7) INTO v_n;
            UPDATE pgseries.job SET last_run = now() WHERE job_id = p_job_id;
            RETURN format('precreated %s chunks for %s.%s', v_n,
                j.target_schema, j.target_table);
        WHEN 'move_default' THEN
            SELECT pgseries.move_default(
                j.target_schema, j.target_table) INTO v_n;
            UPDATE pgseries.job SET last_run = now() WHERE job_id = p_job_id;
            RETURN format('moved %s rows for %s.%s', v_n,
                j.target_schema, j.target_table);
        WHEN 'retention' THEN
            v_ret := (j.config ->> 'retention')::INTERVAL;
            SELECT count(*) INTO v_n FROM pgseries.drop_chunks(
                j.target_schema, j.target_table, v_ret);
            UPDATE pgseries.job SET last_run = now() WHERE job_id = p_job_id;
            RETURN format('dropped %s chunks for %s.%s', v_n,
                j.target_schema, j.target_table);
        ELSE
            RAISE EXCEPTION 'pgseries: unknown job type %', j.job_type
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
END;
$$;

-- ---------------------------------------------------------------------
-- 5. Forward stubs — staged APIs, honest "not implemented"
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pgseries.add_dimension(
    p_schema NAME, p_table NAME, p_column NAME,
    p_type TEXT DEFAULT 'space_hash', p_n INTEGER DEFAULT 16)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pgseries, pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION 'pgseries: add_dimension ships in v0.2 full (space partitioning); catalog shape is reserved'
        USING ERRCODE = 'feature_not_supported';
END;
$$;

CREATE OR REPLACE FUNCTION pgseries.add_continuous_aggregate(
    p_cagg NAME, p_query TEXT)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pgseries, pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION 'pgseries: continuous aggregates ship in v0.3'
        USING ERRCODE = 'feature_not_supported';
END;
$$;

-- Compressed partitions reject writes unless the packing session
-- opts in (compress/decompress set the flag SET LOCAL).
CREATE OR REPLACE FUNCTION pgseries._compressed_readonly()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pgseries, pg_catalog
AS $$
BEGIN
    IF current_setting('pgseries.allow_compressed_write', true) = 'on' THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;
    RAISE EXCEPTION 'pgseries: % is compressed read-only (decompress_chunk to write; late writes go to the heap staging)',
        TG_TABLE_NAME
        USING ERRCODE = 'read_only_sql_transaction';
END;
$$;

CREATE OR REPLACE FUNCTION pgseries._compressed_no_truncate()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pgseries, pg_catalog
AS $$
BEGIN
    IF current_setting('pgseries.allow_compressed_write', true) = 'on' THEN
        RETURN NULL;
    END IF;
    RAISE EXCEPTION 'pgseries: cannot TRUNCATE compressed data directly (decompress_chunk to write)'
        USING ERRCODE = 'read_only_sql_transaction';
END;
$$;

CREATE OR REPLACE FUNCTION pgseries.add_compression_policy(
    p_schema NAME, p_table NAME,
    p_segmentby NAME[] DEFAULT '{}',
    p_orderby NAME DEFAULT NULL,
    p_orderby_desc BOOLEAN DEFAULT false,
    p_segment_rows INTEGER DEFAULT 1000,
    p_codecs JSONB DEFAULT '{}',
    p_requant JSONB DEFAULT '{}')
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s        pgseries.series_table%ROWTYPE;
    v_ob     NAME;
    v_tpl    TEXT := format('%I.%I', p_schema, p_table || '_compressed');
    v_ddl    TEXT;
    v_c      TEXT;
    v_seg    NAME;
    v_n      BIGINT;
    v_slug   TEXT := pgseries._slug(p_schema, p_table);
    v_key    TEXT;
    r        RECORD;
BEGIN
    s := pgseries._series(p_schema, p_table);
    IF EXISTS (SELECT 1 FROM pgseries.compression_policy
               WHERE series_schema = p_schema AND series_table = p_table) THEN
        RAISE EXCEPTION 'pgseries: %.% already has a compression policy (drop_compression_policy first)',
            p_schema, p_table
            USING ERRCODE = 'duplicate_object';
    END IF;
    v_ob := COALESCE(p_orderby, s.time_column);
    IF p_segment_rows IS NULL OR p_segment_rows < 1 THEN
        RAISE EXCEPTION 'pgseries: segment_rows must be >= 1'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Column existence + codec applicability, up front.
    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_schema, p_table, s.time_column,
                 COALESCE(p_segmentby, '{}')) LOOP
        IF r.kind = 'segby' AND r.attname = s.time_column THEN
            RAISE EXCEPTION 'pgseries: time column % cannot be a segmentby column',
                s.time_column
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    END LOOP;
    IF v_ob IS NULL OR NOT EXISTS (
        SELECT 1 FROM pgseries._colinfo(p_schema, p_table, s.time_column,
                                        COALESCE(p_segmentby, '{}')) c
        WHERE c.attname = v_ob AND c.kind = 'data') THEN
        RAISE EXCEPTION 'pgseries: orderby column % must be a plain data column',
            v_ob
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    FOR v_key IN SELECT jsonb_object_keys(COALESCE(p_codecs, '{}')) LOOP
        SELECT * INTO r FROM pgseries._colinfo(
            p_schema, p_table, s.time_column, COALESCE(p_segmentby, '{}')) c
        WHERE c.attname = v_key::NAME;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'pgseries: codec for unknown column %', v_key
                USING ERRCODE = 'undefined_column';
        END IF;
        IF r.kind = 'segby' THEN
            RAISE NOTICE 'pgseries: codec for segmentby column % ignored (stored scalar)',
                v_key;
            CONTINUE;
        END IF;
        IF r.kind = 'gen' THEN
            RAISE EXCEPTION 'pgseries: cannot codec generated column %', v_key
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        PERFORM pgseries._check_codec(p_codecs ->> v_key, r.class, v_key);
    END LOOP;
    FOR v_key IN SELECT jsonb_object_keys(COALESCE(p_requant, '{}')) LOOP
        SELECT * INTO r FROM pgseries._colinfo(
            p_schema, p_table, s.time_column, COALESCE(p_segmentby, '{}')) c
        WHERE c.attname = v_key::NAME AND c.class = 'float';
        IF NOT FOUND THEN
            RAISE EXCEPTION 'pgseries: requant needs a float column, got %', v_key
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
    END LOOP;
    IF NOT EXISTS (SELECT 1 FROM pgseries._colinfo(
                       p_schema, p_table, s.time_column,
                       COALESCE(p_segmentby, '{}')) c
                   WHERE c.kind = 'data') THEN
        RAISE EXCEPTION 'pgseries: nothing to encode (no data columns)'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Template tree + BRIN + read-only row trigger (verified: fires
    -- for direct-partition writes too).
    FOR v_ddl IN SELECT * FROM pgseries._compressed_ddl(
                     p_schema, p_table, v_tpl, COALESCE(p_codecs, '{}'),
                     COALESCE(p_segmentby, '{}'), true) LOOP
        EXECUTE v_ddl;
    END LOOP;
    EXECUTE format('CREATE INDEX %I ON %s USING BRIN '
                   || '(seg_min_ts timestamptz_minmax_multi_ops, '
                   || 'seg_max_ts timestamptz_minmax_multi_ops)',
                   p_table::TEXT || '_cseg_brin', v_tpl);
    FOR v_seg IN SELECT unnest(COALESCE(p_segmentby, '{}')) LOOP
        BEGIN
            EXECUTE format('CREATE INDEX %I ON %s USING BRIN (%I)',
                p_table::TEXT || '_cseg_' || v_seg::TEXT || '_brin',
                v_tpl, v_seg);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pgseries: no BRIN opclass for segmentby % (%); skipping',
                v_seg, SQLERRM;
        END;
    END LOOP;
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE '
                   || 'ON %s FOR EACH ROW EXECUTE FUNCTION '
                   || 'pgseries._compressed_readonly()',
                   p_table::TEXT || '_compressed_ro', v_tpl);

    INSERT INTO pgseries.compression_policy
        (series_schema, series_table, segmentby, orderby, orderby_desc,
         segment_rows, codecs, requant)
    VALUES (p_schema, p_table, COALESCE(p_segmentby, '{}'), v_ob,
            p_orderby_desc, p_segment_rows,
            COALESCE(p_codecs, '{}'), COALESCE(p_requant, '{}'));

    -- Generated read path: unpack + static view + range fast path.
    EXECUTE pgseries._unpack_ddl(p_schema, p_table,
                                 COALESCE(p_codecs, '{}'),
                                 COALESCE(p_segmentby, '{}'));
    FOR v_ddl IN SELECT * FROM pgseries._read_ddl(p_schema, p_table) LOOP
        EXECUTE v_ddl;
    END LOOP;

    -- Cardinality trap (spec): segmentby near segments-per-chunk
    -- collapses the ratio. One-off scan at policy time.
    IF p_segmentby IS NOT NULL AND array_length(p_segmentby, 1) IS NOT NULL THEN
        RAISE NOTICE 'pgseries: counting distinct segmentby values...';
        EXECUTE format('SELECT count(*) FROM (SELECT DISTINCT %s FROM %I.%I) t',
            (SELECT string_agg(quote_ident(x), ', ')
             FROM unnest(p_segmentby) AS x),
            p_schema, p_table) INTO v_n;
        IF v_n > 50000 THEN
            RAISE WARNING 'pgseries: % distinct segmentby values — keep below ~10^4 per chunk for the 2-4x target',
                v_n;
        END IF;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pgseries.drop_compression_policy(
    p_schema NAME, p_table NAME)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_slug TEXT := pgseries._slug(p_schema, p_table);
    v_n    BIGINT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pgseries.compression_policy
                   WHERE series_schema = p_schema
                     AND series_table = p_table) THEN
        RAISE EXCEPTION 'pgseries: %.% has no compression policy',
            p_schema, p_table
            USING ERRCODE = 'undefined_object';
    END IF;
    SELECT count(*) INTO v_n
    FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
    WHERE c.series_schema = p_schema AND c.series_table = p_table
      AND l.state IN ('compressing', 'compressed');
    IF v_n > 0 THEN
        RAISE EXCEPTION 'pgseries: % compressed chunks exist; decompress_chunk them first',
            v_n
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    EXECUTE format('DROP VIEW IF EXISTS %I.%I', p_schema, p_table || '_read');
    EXECUTE format('DROP FUNCTION IF EXISTS %I.%I(%s, %s)',
                   p_schema, p_table || '_read_range',
                   (SELECT time_col_type FROM pgseries.series_table
                    WHERE series_schema = p_schema AND series_table = p_table),
                   (SELECT time_col_type FROM pgseries.series_table
                    WHERE series_schema = p_schema AND series_table = p_table));
    EXECUTE format('DROP FUNCTION IF EXISTS pgseries.unpack_%s', v_slug);
    EXECUTE format('DROP TABLE IF EXISTS %I.%I',
                   p_schema, p_table || '_compressed');
    DELETE FROM pgseries.compression_policy
    WHERE series_schema = p_schema AND series_table = p_table;
END;
$$;

-- Static read path for a compressed table: full-union view plus a
-- range fast path whose seg-bound quals hit BRIN before the LATERAL
-- unnest fires (the spec's load-bearing plan shape).
CREATE OR REPLACE FUNCTION pgseries._read_ddl(
    p_schema NAME, p_table NAME)
RETURNS SETOF TEXT
LANGUAGE plpgsql STABLE
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s       pgseries.series_table%ROWTYPE;
    v_tpl   TEXT := format('%I.%I', p_schema, p_table || '_compressed');
    v_unpack TEXT := 'pgseries.unpack_' || pgseries._slug(p_schema, p_table);
    v_view  TEXT := format('%I.%I', p_schema, p_table || '_read');
    v_range TEXT := format('%I.%I', p_schema, p_table || '_read_range');
BEGIN
    s := pgseries._series(p_schema, p_table);
    RETURN NEXT
        format('CREATE OR REPLACE VIEW %s AS SELECT * FROM %I.%I '
               || 'UNION ALL SELECT u.* FROM %s AS c, '
               || 'LATERAL %s(c) AS u',
               v_view, p_schema, p_table, v_tpl, v_unpack);
    RETURN NEXT
        format('CREATE OR REPLACE FUNCTION %s(a %s, b %s) '
               || 'RETURNS SETOF %I.%I LANGUAGE sql STABLE '
               || 'SET search_path = pgseries, pg_catalog AS %s '
               || 'SELECT * FROM %I.%I WHERE %I >= $1 AND %I <= $2 '
               || 'UNION ALL '
               || 'SELECT u.* FROM (SELECT c.* FROM %s AS c '
               || 'WHERE c.seg_max_ts >= $1::TIMESTAMPTZ '
               || 'AND c.seg_min_ts <= $2::TIMESTAMPTZ) AS f, '
               || 'LATERAL %s(f) AS u WHERE u.%I >= $1 AND u.%I <= $2%s',
               v_range, s.time_col_type, s.time_col_type,
               p_schema, p_table,
               '$pgseries_read$',
               p_schema, p_table, s.time_column, s.time_column,
               v_tpl, v_unpack, s.time_column, s.time_column, '$pgseries_read$');
END;
$$;

-- Pack one chunk's heap rows as new segments. Uniform for first
-- compression and recompression (late-write staging merges as fresh
-- segments). Returns a human summary.
CREATE OR REPLACE FUNCTION pgseries.compress_chunk(
    p_schema NAME, p_table NAME, p_chunk NAME)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s        pgseries.series_table%ROWTYPE;
    pol      pgseries.compression_policy%ROWTYPE;
    v_state  TEXT;
    v_lo     TIMESTAMPTZ;
    v_hi     TIMESTAMPTZ;
    v_cpart  NAME;
    v_tpl    TEXT := format('%I.%I', p_schema, p_table || '_compressed');
    v_heap   BIGINT;
    v_segs   BIGINT;
    v_idx    RECORD;
BEGIN
    s := pgseries._series(p_schema, p_table);
    SELECT * INTO pol FROM pgseries.compression_policy
    WHERE series_schema = p_schema AND series_table = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: %.% has no compression policy',
            p_schema, p_table
            USING ERRCODE = 'undefined_object';
    END IF;
    SELECT l.state, c.range_lower, c.range_upper
    INTO v_state, v_lo, v_hi
    FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
    WHERE c.chunk_schema = p_schema AND c.chunk_name = p_chunk
    FOR UPDATE OF l;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: unknown chunk %.%', p_schema, p_chunk
            USING ERRCODE = 'undefined_object';
    END IF;
    IF v_state NOT IN ('ready', 'compressed') THEN
        RAISE EXCEPTION 'pgseries: chunk %.% is %, cannot compress',
            p_schema, p_chunk, v_state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF v_lo IS NULL THEN
        RAISE EXCEPTION 'pgseries: chunk %.% has unknown bounds, cannot compress',
            p_schema, p_chunk
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    PERFORM pgseries._table_lock(p_schema, p_table);

    EXECUTE format('SELECT count(*) FROM %I.%I', p_schema, p_chunk)
    INTO v_heap;
    v_cpart := (p_chunk::TEXT || '_compressed')::NAME;
    IF v_heap = 0 THEN
        IF to_regclass(format('%I.%I', p_schema, v_cpart)) IS NULL THEN
            RAISE NOTICE 'pgseries: chunk %.% heap is empty, nothing to compress',
                p_schema, p_chunk;
            RETURN 'empty heap, nothing compressed';
        END IF;
        RAISE NOTICE 'pgseries: chunk %.% already compressed, heap empty',
            p_schema, p_chunk;
        RETURN 'already compressed, 0 new rows';
    END IF;

    -- First compression (ready) and recompression (compressed) share
    -- the ->compressing edge.
    PERFORM pgseries._lease_transition(p_schema, p_chunk, 'compressing');

    IF to_regclass(format('%I.%I', p_schema, v_cpart)) IS NULL THEN
        EXECUTE format('CREATE TABLE %I.%I PARTITION OF %s '
                       || 'FOR VALUES FROM (%L::TIMESTAMPTZ) TO (%L::TIMESTAMPTZ)',
            p_schema, v_cpart, v_tpl, v_lo::TEXT, v_hi::TEXT);
        -- Explicit per-partition hardening (row trigger inherits from
        -- the template; TRUNCATE triggers do not).
        EXECUTE format('REVOKE ALL ON %I.%I FROM PUBLIC, pgseries_reader, pgseries_writer',
            p_schema, v_cpart);
        EXECUTE format('CREATE TRIGGER %I BEFORE TRUNCATE ON %I.%I '
                       || 'FOR EACH STATEMENT EXECUTE FUNCTION '
                       || 'pgseries._compressed_no_truncate()',
                       v_cpart::TEXT || '_no_trunc', p_schema, v_cpart);
    END IF;

    EXECUTE 'SET LOCAL pgseries.allow_compressed_write = ''on''';
    SELECT pgseries._pack_segments(p_schema, p_chunk, p_schema, v_cpart,
        s.time_column, s.time_col_type, pol.segmentby, pol.orderby,
        pol.orderby_desc, pol.segment_rows, pol.codecs, pol.requant)
    INTO v_segs;
    EXECUTE format('TRUNCATE %I.%I', p_schema, p_chunk);
    PERFORM pgseries._lease_transition(p_schema, p_chunk, 'compressed');

    EXECUTE format('ANALYZE %I.%I', p_schema, v_cpart);
    -- Summarize the new partition's own BRINs (parent partitioned
    -- indexes hold no storage themselves).
    FOR v_idx IN SELECT indexrelid FROM pg_index
                 WHERE indrelid = to_regclass(
                     format('%I.%I', p_schema, v_cpart)) LOOP
        BEGIN
            PERFORM brin_summarize_new_values(v_idx.indexrelid);
        EXCEPTION WHEN OTHERS THEN
            -- non-BRIN (e.g. btree on segmentby): nothing to summarize
            NULL;
        END;
    END LOOP;

    RETURN format('compressed %s rows into %s segments (%I)',
                  v_heap, v_segs, v_cpart);
END;
$$;

CREATE OR REPLACE FUNCTION pgseries.decompress_chunk(
    p_schema NAME, p_table NAME, p_chunk NAME)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s       pgseries.series_table%ROWTYPE;
    v_state TEXT;
    v_cpart NAME;
    v_cols  TEXT;
    v_n     BIGINT;
    v_unpack TEXT := 'pgseries.unpack_' || pgseries._slug(p_schema, p_table);
BEGIN
    s := pgseries._series(p_schema, p_table);
    SELECT l.state INTO v_state
    FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
    WHERE c.chunk_schema = p_schema AND c.chunk_name = p_chunk
    FOR UPDATE OF l;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: unknown chunk %.%', p_schema, p_chunk
            USING ERRCODE = 'undefined_object';
    END IF;
    IF v_state <> 'compressed' THEN
        RAISE EXCEPTION 'pgseries: chunk %.% is %, nothing to decompress',
            p_schema, p_chunk, v_state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    PERFORM pgseries._table_lock(p_schema, p_table);
    v_cpart := (p_chunk::TEXT || '_compressed')::NAME;
    IF to_regclass(format('%I.%I', p_schema, v_cpart)) IS NULL THEN
        RAISE EXCEPTION 'pgseries: compressed partition %.% missing', p_schema, v_cpart
            USING ERRCODE = 'undefined_object';
    END IF;

    PERFORM pgseries._lease_transition(p_schema, p_chunk, 'ready');
    EXECUTE 'SET LOCAL pgseries.allow_compressed_write = ''on''';
    SELECT c.v_all INTO v_cols
    FROM pgseries._copy_columns(p_schema, p_table) AS c;
    EXECUTE format('INSERT INTO %I.%I (%s) OVERRIDING SYSTEM VALUE '
                   || 'SELECT %s FROM (SELECT (%s(c)).* FROM %I.%I AS c) AS s',
        p_schema, p_chunk, v_cols, v_cols, v_unpack, p_schema, v_cpart);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    EXECUTE format('TRUNCATE %I.%I', p_schema, v_cpart);
    EXECUTE format('ANALYZE %I.%I', p_schema, p_chunk);
    RETURN format('decompressed %s rows into %I', v_n, p_chunk);
END;
$$;

-- Empirical codec comparison: packs a heap sample into temp tables
-- under each candidate and reports bytes/row. Default candidates pit
-- the spec pipeline (for+dict) against delta and delta-of-delta.
CREATE OR REPLACE FUNCTION pgseries.bench_codecs(
    p_schema NAME, p_table NAME,
    p_segmentby NAME[] DEFAULT '{}',
    p_limit INTEGER DEFAULT 200000,
    p_candidates JSONB DEFAULT NULL)
RETURNS TABLE (name TEXT, rows BIGINT, bytes BIGINT,
               bytes_per_row NUMERIC, ratio_vs_heap NUMERIC,
               segment_rows INTEGER)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s        pgseries.series_table%ROWTYPE;
    v_cands  JSONB := COALESCE(p_candidates, jsonb_build_array(
        jsonb_build_object('name', 'raw', 'temporal', 'raw', 'ints', 'raw', 'text', 'raw', 'segment_rows', 1000),
        jsonb_build_object('name', 'spec', 'temporal', 'for', 'ints', 'for', 'text', 'dict', 'segment_rows', 1000),
        jsonb_build_object('name', 'delta', 'temporal', 'delta', 'ints', 'delta', 'text', 'dict', 'segment_rows', 1000),
        jsonb_build_object('name', 'delta2', 'temporal', 'delta2', 'ints', 'delta', 'text', 'dict', 'segment_rows', 1000),
        jsonb_build_object('name', 'delta2_seg5k', 'temporal', 'delta2', 'ints', 'delta', 'text', 'dict', 'segment_rows', 5000)));
    v_cand   JSONB;
    v_codecs JSONB;
    v_seg    NAME[];
    v_rows   BIGINT;
    v_heap   BIGINT;
    v_bytes  BIGINT;
    v_ddl    TEXT;
    v_tgt    TEXT;
    i        INTEGER := 0;
    r        RECORD;
BEGIN
    s := pgseries._series(p_schema, p_table);
    EXECUTE format('CREATE TEMP TABLE bench_src AS SELECT * FROM %I.%I '
                   || 'ORDER BY %I LIMIT %s',
        p_schema, p_table, s.time_column, p_limit);
    EXECUTE 'SELECT count(*) FROM pg_temp.bench_src' INTO v_rows;
    IF v_rows = 0 THEN
        RAISE EXCEPTION 'pgseries: nothing to benchmark (empty table)'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    SELECT pg_total_relation_size('pg_temp.bench_src') INTO v_heap;

    FOR v_cand IN SELECT * FROM jsonb_array_elements(v_cands) LOOP
        i := i + 1;
        v_tgt := 'bench_c' || i;
        v_seg := COALESCE(
            (SELECT array_agg(x ORDER BY o)
             FROM jsonb_array_elements_text(
                 COALESCE(v_cand -> 'segmentby', '[]')) WITH ORDINALITY AS t(x, o)),
            '{}');
        IF array_length(v_seg, 1) IS NULL THEN
            v_seg := COALESCE(p_segmentby, '{}');
        END IF;

        -- Resolve per-column codecs by class.
        v_codecs := '{}';
        FOR r IN SELECT * FROM pgseries._colinfo(
                     'pg_temp', 'bench_src', s.time_column, v_seg) LOOP
            IF r.kind <> 'data' THEN
                CONTINUE;
            END IF;
            v_codecs := v_codecs || jsonb_build_object(r.attname::TEXT,
                CASE
                    WHEN r.typename LIKE 'timestamp%' OR r.typename = 'date'
                        THEN v_cand ->> 'temporal'
                    WHEN r.class = 'int' THEN v_cand ->> 'ints'
                    WHEN r.class = 'text' THEN v_cand ->> 'text'
                    ELSE 'raw'
                END);
        END LOOP;

        IF to_regclass('pg_temp.' || quote_ident(v_tgt)) IS NOT NULL THEN
            EXECUTE format('DROP TABLE pg_temp.%I', v_tgt);
        END IF;        FOR v_ddl IN SELECT * FROM pgseries._compressed_ddl(
                         'pg_temp', 'bench_src', 'pg_temp.' || quote_ident(v_tgt),
                         v_codecs, v_seg, false) LOOP
            EXECUTE v_ddl;
        END LOOP;
        PERFORM pgseries._pack_segments(
            'pg_temp', 'bench_src', 'pg_temp', v_tgt,
            s.time_column, s.time_col_type, v_seg, s.time_column, false,
            COALESCE((v_cand ->> 'segment_rows')::INTEGER, 1000),
            v_codecs, '{}');
        EXECUTE format('ANALYZE pg_temp.%I', v_tgt);
        EXECUTE format('SELECT pg_total_relation_size(%L)',
                       'pg_temp.' || v_tgt) INTO v_bytes;

        name := v_cand ->> 'name';
        rows := v_rows;
        bytes := v_bytes;
        bytes_per_row := round(v_bytes::NUMERIC / v_rows, 2);
        ratio_vs_heap := round(v_bytes::NUMERIC / NULLIF(v_heap, 0), 3);
        segment_rows := COALESCE((v_cand ->> 'segment_rows')::INTEGER, 1000);
        RETURN NEXT;
        EXECUTE format('DROP TABLE pg_temp.%I', v_tgt);
    END LOOP;
    EXECUTE 'DROP TABLE pg_temp.bench_src';
END;
$$;

-- PgQue ticker stand-in: with no queued engine bundled, there is
-- nothing to tick. run_job_now() is the execution path.
CREATE OR REPLACE FUNCTION pgseries_pgque.ticker()
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pgseries_pgque, pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION 'pgseries: pgseries_pgque.ticker ships with the vendored PgQue engine in v0.2 full; use pgseries.run_job_now(job_id) meanwhile'
        USING ERRCODE = 'feature_not_supported';
END;
$$;

-- ---------------------------------------------------------------------
-- 5b. v0.6 API — columnar compression (pure-SQL, TOAST/lz4 backed)
--
-- Layout per series table (created by add_compression_policy):
--   <table>_compressed  — partitioned template (RANGE seg_min_ts),
--                          one partition <chunk>_compressed per chunk.
--   unpack_<s>_<t>(seg) — generated per-table decoder, exact roundtrip
--                          (floats: RAW only, bit-exact).
--   <table>_read        — static view: heap UNION ALL lateral unpack.
--   <table>_read_range  — fast path with seg-bound pushdown (BRIN skip).
--
-- Codec set per column: raw | for | delta | delta2 | dict.
-- for/delta/delta2 apply to int/timestamp/date families only (exact
-- integer arithmetic on micros/days); floats are RAW (float
-- reassociation is not bit-exact); text-like/bool/uuid/inet/arrays
-- go through text form + dict/raw; jsonb/bytea/numeric are native RAW.
-- NULLs are int[] positions (sparse-friendly; the spec's bitmap is
-- equivalent but needs a custom aggregate in pure SQL).
-- ---------------------------------------------------------------------

-- Codec set per column: raw | for | delta | delta2 | dict.
-- for/delta/delta2 apply to int/timestamp/date families only (exact
-- integer arithmetic on micros/days); floats are RAW (float
-- reassociation is not bit-exact); text-like/bool/uuid/inet/arrays
-- go through text form + dict/raw; jsonb/bytea/numeric are native RAW.
-- NULLs are int[] positions (sparse-friendly; the spec's bitmap is
-- equivalent but needs a custom aggregate in pure SQL).
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgseries._storage_class(p_type TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT CASE
        WHEN p_type IN ('smallint', 'integer', 'bigint',
                        'timestamp with time zone',
                        'timestamp without time zone', 'date') THEN 'int'
        WHEN p_type IN ('real', 'double precision') THEN 'float'
        WHEN p_type = 'numeric' THEN 'numeric'
        WHEN p_type = 'jsonb' THEN 'jsonb'
        WHEN p_type = 'bytea' THEN 'bytea'
        ELSE 'text'
    END;
$$;

-- Default codec per storage class (spec pipeline: FOR + dict + RAW).
CREATE OR REPLACE FUNCTION pgseries._default_codec(p_class TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT CASE p_class WHEN 'int' THEN 'for'
                        WHEN 'text' THEN 'dict'
                        ELSE 'raw' END;
$$;

-- Effective codec for a column under a policy (explicit > '*' > default).
CREATE OR REPLACE FUNCTION pgseries._eff_codec(
    p_codecs JSONB, p_col TEXT, p_class TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT COALESCE(p_codecs ->> p_col, p_codecs ->> '*',
                    pgseries._default_codec(p_class));
$$;

-- Reject inapplicable codecs at policy time with a clear error.
CREATE OR REPLACE FUNCTION pgseries._check_codec(
    p_codec TEXT, p_class TEXT, p_col TEXT)
RETURNS VOID
LANGUAGE plpgsql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
BEGIN
    IF p_codec NOT IN ('raw', 'for', 'delta', 'delta2', 'dict') THEN
        RAISE EXCEPTION 'pgseries: unknown codec % for column %',
            p_codec, p_col
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_codec IN ('for', 'delta', 'delta2') AND p_class <> 'int' THEN
        RAISE EXCEPTION 'pgseries: codec % needs int/timestamp/date, not % (%)',
            p_codec, p_class, p_col
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_codec = 'dict' AND p_class NOT IN ('text') THEN
        RAISE EXCEPTION 'pgseries: codec dict needs a text-like column, not % (%)',
            p_class, p_col
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
END;
$$;

-- Significant-figure requantization (lossy, opt-in per float column).
CREATE OR REPLACE FUNCTION pgseries._requant(p_v DOUBLE PRECISION, p_n INTEGER)
RETURNS DOUBLE PRECISION
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT CASE WHEN p_v = 0 OR p_v IS NULL THEN p_v
        ELSE round(p_v::NUMERIC,
                   p_n - 1 - floor(log(10, abs(p_v::NUMERIC)))::INT)::DOUBLE PRECISION
    END;
$$;

-- Pack rows from a heap source into a compressed target. Shared by
-- compress_chunk (real partitions) and bench_codecs (temp tables).
-- Levels: L0 raw rows + _r (window); L1 + _segno; L2 group + ordered
-- arrays a<k>; L3 codec derivation + seg metadata. Returns segments.
CREATE OR REPLACE FUNCTION pgseries._pack_segments(
    p_src_schema NAME, p_src_table NAME,
    p_tgt_schema NAME, p_tgt_table NAME,
    p_timecol NAME, p_timecol_type TEXT,
    p_segmentby NAME[], p_orderby NAME, p_ob_desc BOOLEAN,
    p_segment_rows INTEGER, p_codecs JSONB, p_requant JSONB)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    r         RECORD;
    v_codec   TEXT;
    v_rq      INTEGER;
    v_pfx     TEXT;
    v_win     TEXT;   -- full window clause for _r
    v_l0      TEXT := '';
    v_l1      TEXT := '';
    v_l2arr   TEXT := '';
    v_l3sel   TEXT := '';
    v_ins     TEXT := '';
    v_segcols TEXT := '';
    v_grplist TEXT := '';
    v_ob      TEXT := quote_ident(p_orderby);
    v_obdir   TEXT := CASE WHEN p_ob_desc THEN 'DESC' ELSE 'ASC' END;
    v_a       TEXT;
    v_min     TEXT;
    v_dict    TEXT;
    v_n       BIGINT;
    v_l2key   TEXT;
BEGIN
    IF p_segmentby IS NULL OR array_length(p_segmentby, 1) IS NULL THEN
        v_win := format('ORDER BY %s %s, ctid', v_ob, v_obdir);
    ELSE
        SELECT string_agg(quote_ident(x), ', ') INTO v_segcols
        FROM unnest(p_segmentby) AS x;
        v_win := format('PARTITION BY %s ORDER BY %s %s, ctid',
                        v_segcols, v_ob, v_obdir);
    END IF;
    v_l2key := CASE WHEN v_segcols = '' THEN '_segno'
                    ELSE v_segcols || ', _segno' END;

    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_src_schema, p_src_table, p_timecol, p_segmentby) LOOP
        v_pfx := 'c' || r.attnum;
        IF r.kind = 'gen' THEN
            CONTINUE;
        ELSIF r.kind = 'segby' THEN
            -- Already present via L1's SELECT *; GROUP/INSERT lists only.
            v_grplist := v_grplist || format(', %I', r.attname);
            v_l3sel := v_l3sel || format(', %I', r.attname);
            v_ins := v_ins || format(', %I', r.attname);
            CONTINUE;
        END IF;

        v_codec := pgseries._eff_codec(p_codecs, r.attname::TEXT, r.class);
        PERFORM pgseries._check_codec(v_codec, r.class, r.attname::TEXT);

        -- L1 native expression per storage class (raw column refs only).
        IF r.class = 'int' THEN
            IF r.typename LIKE 'timestamp%' THEN
                v_l1 := v_l1 || format(
                    ', ((extract(epoch FROM %I) * 1000000))::BIGINT AS %I',
                    r.attname, 'v' || r.attnum);
            ELSIF r.typename = 'date' THEN
                v_l1 := v_l1 || format(
                    ', (%I - DATE ''1970-01-01'')::BIGINT AS %I',
                    r.attname, 'v' || r.attnum);
            ELSE
                v_l1 := v_l1 || format(', %I::BIGINT AS %I',
                    r.attname, 'v' || r.attnum);
            END IF;
        ELSIF r.class = 'float' THEN
            v_rq := (p_requant ->> r.attname::TEXT)::INTEGER;
            IF v_rq IS NOT NULL THEN
                v_l1 := v_l1 || format(
                    ', pgseries._requant(%I::DOUBLE PRECISION, %s) AS %I',
                    r.attname, v_rq, 'v' || r.attnum);
            ELSE
                v_l1 := v_l1 || format(', %I::DOUBLE PRECISION AS %I',
                    r.attname, 'v' || r.attnum);
            END IF;
        ELSIF r.class = 'text' THEN
            v_l1 := v_l1 || format(', %I::TEXT AS %I',
                r.attname, 'v' || r.attnum);
        ELSE -- numeric | jsonb | bytea: native arrays
            v_l1 := v_l1 || format(', %I AS %I',
                r.attname, 'v' || r.attnum);
        END IF;

        -- L2 ordered array; L3 derivation per codec (a<k> visible there).
        v_l2arr := v_l2arr || format(', array_agg(%I ORDER BY _r) AS %I',
                                     'v' || r.attnum, 'a' || r.attnum);
        v_a := 'a' || r.attnum;
        v_min := format('(SELECT min(x) FROM unnest(%I) AS x)', v_a);
        v_dict := format('(SELECT COALESCE(array_agg(DISTINCT x), ''{}'') FROM unnest(%I) AS x WHERE x IS NOT NULL)', v_a);
        v_l3sel := v_l3sel || ', ' || CASE
            WHEN v_codec = 'for' AND r.class = 'int' THEN
                format('%s AS %I, %s AS %I, %s AS %I',
                    v_min, v_pfx || '_base',
                    format('(SELECT array_agg(x - (%s)) FROM unnest(%I) AS x)',
                           v_min, v_a), v_pfx || '_arr',
                    format('(SELECT COALESCE(array_agg(j), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE (%I)[j] IS NULL)',
                           v_a, v_a), v_pfx || '_nulls')
            WHEN v_codec IN ('delta', 'delta2') THEN
                format('%s AS %I, %s AS %I, %s AS %I',
                    format('(%I)[1]', v_a), v_pfx || '_base',
                    CASE WHEN v_codec = 'delta' THEN
                        format('(SELECT COALESCE(array_agg((%I)[j+1] - (%I)[j]), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE j < cardinality(%I))',
                               v_a, v_a, v_a, v_a)
                    ELSE
                        format('CASE WHEN cardinality(%I) < 2 THEN ''{}''::BIGINT[] ELSE ARRAY[(%I)[2] - (%I)[1]] || COALESCE((SELECT array_agg((%I)[j+2] - 2*(%I)[j+1] + (%I)[j]) FROM generate_subscripts(%I, 1) AS j WHERE j <= cardinality(%I) - 2), ''{}'') END',
                               v_a, v_a, v_a, v_a, v_a, v_a, v_a, v_a)
                    END, v_pfx || '_arr',
                    format('(SELECT COALESCE(array_agg(j), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE (%I)[j] IS NULL)',
                           v_a, v_a), v_pfx || '_nulls')
            WHEN v_codec = 'dict' THEN
                format('%s AS %I, %s AS %I, %s AS %I',
                    v_dict, v_pfx || '_dict',
                    format('(SELECT array_agg(CASE WHEN (%I)[j] IS NULL THEN NULL ELSE array_position(%s, (%I)[j]) END) FROM generate_subscripts(%I, 1) AS j)',
                           v_a, v_dict, v_a, v_a), v_pfx || '_arr',
                    format('(SELECT COALESCE(array_agg(j), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE (%I)[j] IS NULL)',
                           v_a, v_a), v_pfx || '_nulls')
            ELSE -- raw (any class): plain array; text keeps TEXT[] arr
                format('%s AS %I, %s AS %I',
                    v_a, v_pfx || '_arr',
                    format('(SELECT COALESCE(array_agg(j), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE (%I)[j] IS NULL)',
                           v_a, v_a), v_pfx || '_nulls')
        END;
        v_ins := v_ins || CASE
            WHEN v_codec = 'dict' THEN
                format(', %I, %I, %I', v_pfx || '_dict', v_pfx || '_arr',
                       v_pfx || '_nulls')
            WHEN r.class = 'int' AND v_codec <> 'raw' THEN
                format(', %I, %I, %I', v_pfx || '_base', v_pfx || '_arr',
                       v_pfx || '_nulls')
            ELSE format(', %I, %I', v_pfx || '_arr', v_pfx || '_nulls')
        END;
    END LOOP;

    EXECUTE format(
        'WITH l0 AS (SELECT *, row_number() OVER (%s) AS _r '
        || 'FROM %I.%I), '
        || 'l1 AS (SELECT *%s, ((_r - 1) / %s) AS _segno FROM l0), '
        || 'l2 AS (SELECT %s, min(%I) AS _lo, max(%I) AS _hi, '
        || 'count(*) AS _n%s FROM l1 GROUP BY %s) '
        || 'INSERT INTO %I.%I (seg_no, seg_min_ts, seg_max_ts, '
        || 'seg_row_count%s) '
        || 'SELECT row_number() OVER (ORDER BY %s), '
        || '_lo::TIMESTAMPTZ, _hi::TIMESTAMPTZ, _n%s '
        || 'FROM l2',
        v_win, p_src_schema, p_src_table,
        v_l1, p_segment_rows,
        v_l2key, p_timecol, p_timecol, v_l2arr,
        v_l2key,
        p_tgt_schema, p_tgt_table, v_ins,
        v_l2key,
        v_l3sel);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END;
$$;
CREATE OR REPLACE FUNCTION pgseries._slug(p_schema NAME, p_table NAME)
RETURNS TEXT
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT regexp_replace(
               regexp_replace(p_schema::TEXT || '_' || p_table::TEXT,
                              '[^a-zA-Z0-9_]', '_', 'g'),
               '_+', '_', 'g');
$$;

-- Generated per-table decoder. Set-based reconstruction (running sums
-- via window functions, no plpgsql loops on the read path). Decoded
-- columns are assembled in attnum order; generated columns wrap in
-- nested levels (each sees the previous level's outputs). Returns the
-- parent rowtype so the static read view typechecks.
CREATE OR REPLACE FUNCTION pgseries._unpack_ddl(
    p_schema NAME, p_table NAME,
    p_codecs JSONB, p_segmentby NAME[])
RETURNS TEXT
LANGUAGE plpgsql STABLE
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    r         RECORD;
    v_codec   TEXT;
    v_pfx     TEXT;
    v_fname   TEXT := 'pgseries.unpack_' || pgseries._slug(p_schema, p_table);
    v_tpl     TEXT := format('%I.%I', p_schema, p_table || '_compressed');
    v_cte     TEXT := '';
    v_inner   TEXT := '';
    v_wrap    TEXT;
    v_final   TEXT := '';
    v_g       TEXT;
    v_arr     TEXT;
    v_base    TEXT;
    v_nulls   TEXT;
    v_time    NAME;
BEGIN
    SELECT time_column INTO v_time FROM pgseries.series_table
    WHERE series_schema = p_schema AND series_table = p_table;

    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_schema, p_table, v_time, p_segmentby) LOOP
        v_pfx := 'c' || r.attnum;
        IF r.kind = 'gen' THEN
            CONTINUE; -- handled in wrapper chain below
        ELSIF r.kind = 'segby' THEN
            v_inner := v_inner || format(', seg.%I AS %I', r.attname, r.attname);
            CONTINUE;
        END IF;

        v_codec := pgseries._eff_codec(p_codecs, r.attname::TEXT, r.class);
        v_arr := format('seg.%I', v_pfx || '_arr');
        v_base := format('seg.%I', v_pfx || '_base');
        v_nulls := format('seg.%I', v_pfx || '_nulls');

        IF v_codec = 'delta' AND r.class = 'int' THEN
            v_cte := v_cte || format(
                ', d%s AS (SELECT COALESCE(array_agg(s ORDER BY o), ''{}'') AS a '
                || 'FROM (SELECT sum(x) OVER (ORDER BY o) AS s, o '
                || 'FROM unnest(%s) WITH ORDINALITY AS u(x, o)) t)',
                v_pfx, v_arr);
        ELSIF v_codec = 'delta2' THEN
            v_cte := v_cte || format(
                ', d%s AS (SELECT COALESCE(array_agg(s ORDER BY o), ''{}'') AS a '
                || 'FROM (SELECT sum(x) OVER (ORDER BY o) AS s, o '
                || 'FROM unnest(%s) WITH ORDINALITY AS u(x, o)) t)',
                v_pfx, v_arr);
            v_cte := v_cte || format(
                ', e%s AS (SELECT COALESCE(array_agg(s ORDER BY o), ''{}'') AS a '
                || 'FROM (SELECT sum(x) OVER (ORDER BY o) AS s, o '
                || 'FROM unnest((SELECT a FROM d%s)) WITH ORDINALITY AS u(x, o)) t)',
                v_pfx, v_pfx);
        END IF;

        v_g := CASE
            WHEN v_codec = 'for' THEN
                format('(%s + (%s)[i])', v_base, v_arr)
            WHEN v_codec = 'delta' THEN
                format('(CASE WHEN i = 1 THEN %s ELSE %s + (SELECT a FROM d%s)[i-1] END)',
                       v_base, v_base, v_pfx)
            WHEN v_codec = 'delta2' THEN
                format('(CASE WHEN i = 1 THEN %s ELSE %s + (SELECT a FROM e%s)[i-1] END)',
                       v_base, v_base, v_pfx)
            WHEN v_codec = 'dict' THEN
                format('(CASE WHEN i = ANY(%s) THEN NULL ELSE (%s)[(%s)[i]] END)',
                       v_nulls,
                       format('seg.%I', v_pfx || '_dict'), v_arr)
            ELSE
                format('(%s)[i]', v_arr)
        END;

        IF v_codec <> 'dict' THEN
            v_g := format('(CASE WHEN i = ANY(%s) THEN NULL ELSE %s END)',
                          v_nulls, v_g);
        END IF;

        -- Cast back to the source type (unpack must match parent rowtype).
        v_g := CASE
            WHEN r.class = 'int' AND r.typename LIKE 'timestamp%' THEN
                format('(%s)',
                    CASE WHEN r.typename = 'timestamp with time zone'
                        THEN format('TIMESTAMPTZ ''epoch'' + (%s) * INTERVAL ''1 microsecond''', v_g)
                        ELSE format('TIMESTAMP ''epoch'' + (%s) * INTERVAL ''1 microsecond''', v_g) END)
            WHEN r.class = 'int' AND r.typename = 'date' THEN
                format('(DATE ''1970-01-01'' + (%s)::INTEGER)', v_g)
            WHEN r.class = 'int' THEN
                format('((%s)::%s)', v_g, r.typename)
            WHEN r.class = 'float' AND r.typename = 'real' THEN
                format('((%s)::REAL)', v_g)
            WHEN r.class = 'text' AND r.typename <> 'text' THEN
                format('((%s)::%s)', v_g, r.typename)
            ELSE v_g
        END;

        v_inner := v_inner || format(', %s AS %I', v_g, r.attname);
    END LOOP;

    -- Wrapper chain for generated columns. Final list rebuilt in
    -- attnum order (a generated column may sit mid-rowtype).
    v_wrap := format('SELECT %s FROM generate_series(1, seg.seg_row_count) AS g(i)',
                     ltrim(v_inner, ', '));
    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_schema, p_table, v_time, p_segmentby)
             WHERE kind = 'gen' ORDER BY attnum LOOP
        v_wrap := format('SELECT *, (%s) AS %I FROM (%s)', r.gexpr,
                         r.attname, v_wrap);
    END LOOP;
    v_final := '';
    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_schema, p_table, v_time, p_segmentby)
             ORDER BY attnum LOOP
        v_final := v_final || format(', %I', r.attname);
    END LOOP;

    RETURN format(
        'CREATE OR REPLACE FUNCTION %s(seg %s) RETURNS SETOF %I.%I '
        || 'LANGUAGE sql STABLE SET search_path = pgseries, pg_catalog AS %s '
        || '%s SELECT %s FROM (%s) AS qfinal '
        || '%s',
        v_fname, v_tpl, p_schema, p_table,
        '$pgseries_unpack$',
        CASE WHEN v_cte = '' THEN '' ELSE 'WITH ' || ltrim(v_cte, ', ') END,
        ltrim(v_final, ', '), v_wrap,
        '$pgseries_unpack$');
END;
$$;

-- Column inventory of a series table (one row per live source column).
-- consumed_by: 'segby' | 'time' | 'data'. gexpr set for generated cols.
CREATE OR REPLACE FUNCTION pgseries._colinfo(
    p_schema NAME, p_table NAME,
    p_timecol NAME, p_segmentby NAME[])
RETURNS TABLE (
    attnum INTEGER, attname NAME, typename TEXT, class TEXT,
    kind TEXT, gexpr TEXT)
LANGUAGE sql STABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT a.attnum::INT, a.attname, format_type(a.atttypid, a.atttypmod),
           pgseries._storage_class(format_type(a.atttypid, NULL)),
           CASE WHEN a.attname = ANY (p_segmentby) THEN 'segby'
                WHEN a.attgenerated <> '' THEN 'gen'
                ELSE 'data' END,
           CASE WHEN a.attgenerated <> ''
                THEN pg_get_expr(d.adbin, d.adrelid) END
    FROM pg_attribute a
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE a.attrelid = to_regclass(format('%I.%I', p_schema, p_table))
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum;
$$;

-- DDL statements for a compressed table (real template when
-- p_partitioned, else a plain table for benchmarks). Physical columns
-- follow c<attnum>_<role>; only roles the codec needs are created.
CREATE OR REPLACE FUNCTION pgseries._compressed_ddl(
    p_schema NAME, p_table NAME, p_target TEXT,
    p_codecs JSONB, p_segmentby NAME[], p_partitioned BOOLEAN)
RETURNS SETOF TEXT
LANGUAGE plpgsql STABLE
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    r       RECORD;
    v_codec TEXT;
    v_defs  TEXT := '';
    v_col   TEXT;
    v_first BOOLEAN := true;
    v_time  NAME;
BEGIN
    SELECT time_column INTO v_time FROM pgseries.series_table
    WHERE series_schema = p_schema AND series_table = p_table;

    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_schema, p_table, v_time, p_segmentby) LOOP
        IF r.kind = 'gen' THEN
            CONTINUE; -- recomputed at unpack
        ELSIF r.kind = 'segby' THEN
            v_col := format('%I %s', r.attname, r.typename);
        ELSE
            v_codec := pgseries._eff_codec(p_codecs, r.attname::TEXT, r.class);
            PERFORM pgseries._check_codec(v_codec, r.class, r.attname::TEXT);
            IF r.class = 'int' THEN
                v_col := format('%I BIGINT, %I BIGINT[], %I INT[]',
                    'c' || r.attnum || '_base', 'c' || r.attnum || '_arr',
                    'c' || r.attnum || '_nulls');
            ELSIF r.class = 'float' THEN
                v_col := format('%I DOUBLE PRECISION[], %I INT[]',
                    'c' || r.attnum || '_arr', 'c' || r.attnum || '_nulls');
            ELSIF r.class = 'numeric' THEN
                v_col := format('%I NUMERIC[], %I INT[]',
                    'c' || r.attnum || '_arr', 'c' || r.attnum || '_nulls');
            ELSIF r.class = 'text' THEN
                IF v_codec = 'dict' THEN
                    v_col := format('%I TEXT[], %I INT[], %I INT[]',
                        'c' || r.attnum || '_dict', 'c' || r.attnum || '_arr',
                        'c' || r.attnum || '_nulls');
                ELSE -- raw: values live in a TEXT[] arr, no dict
                    v_col := format('%I TEXT[], %I INT[]',
                        'c' || r.attnum || '_arr',
                        'c' || r.attnum || '_nulls');
                END IF;
            ELSIF r.class = 'jsonb' THEN
                v_col := format('%I JSONB[], %I INT[]',
                    'c' || r.attnum || '_arr', 'c' || r.attnum || '_nulls');
            ELSE -- bytea
                v_col := format('%I BYTEA[], %I INT[]',
                    'c' || r.attnum || '_arr', 'c' || r.attnum || '_nulls');
            END IF;
        END IF;
        v_defs := v_defs || CASE WHEN v_first THEN '' ELSE ', ' END || v_col;
        v_first := false;
    END LOOP;

    v_defs := v_defs || ', seg_no INTEGER, '
        || 'seg_min_ts TIMESTAMPTZ NOT NULL, '
        || 'seg_max_ts TIMESTAMPTZ NOT NULL, '
        || 'seg_row_count INTEGER NOT NULL';

    RETURN NEXT format('CREATE TABLE %s (%s)%s', p_target, v_defs,
        CASE WHEN p_partitioned THEN ' PARTITION BY RANGE (seg_min_ts)'
             ELSE '' END);

    -- TOAST out-of-line + lz4 on every array/dict column (verified:
    -- the cluster default may still be pglz on managed PG).
    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_schema, p_table, v_time, p_segmentby) LOOP
        IF r.kind <> 'data' THEN
            CONTINUE;
        END IF;
        v_codec := pgseries._eff_codec(p_codecs, r.attname::TEXT, r.class);
        IF r.class = 'int' THEN
            RETURN NEXT format(
                'ALTER TABLE %s ALTER COLUMN %I SET STORAGE extended, '
                || 'ALTER COLUMN %I SET COMPRESSION lz4',
                p_target, 'c' || r.attnum || '_arr',
                'c' || r.attnum || '_arr');
        ELSIF r.class = 'text' AND v_codec = 'dict' THEN
            RETURN NEXT format(
                'ALTER TABLE %s ALTER COLUMN %I SET STORAGE extended, '
                || 'ALTER COLUMN %I SET COMPRESSION lz4, '
                || 'ALTER COLUMN %I SET STORAGE extended, '
                || 'ALTER COLUMN %I SET COMPRESSION lz4',
                p_target, 'c' || r.attnum || '_dict',
                'c' || r.attnum || '_dict',
                'c' || r.attnum || '_arr', 'c' || r.attnum || '_arr');
        ELSIF r.class = 'text' THEN
            RETURN NEXT format(
                'ALTER TABLE %s ALTER COLUMN %I SET STORAGE extended, '
                || 'ALTER COLUMN %I SET COMPRESSION lz4',
                p_target, 'c' || r.attnum || '_arr',
                'c' || r.attnum || '_arr');
        ELSE
            RETURN NEXT format(
                'ALTER TABLE %s ALTER COLUMN %I SET STORAGE extended, '
                || 'ALTER COLUMN %I SET COMPRESSION lz4',
                p_target, 'c' || r.attnum || '_arr',
                'c' || r.attnum || '_arr');
        END IF;
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------------
-- 6. Observability surface (subset; full surface by v0.8)
-- ---------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS pgseries_information;

CREATE OR REPLACE VIEW pgseries_information.series_tables AS
SELECT series_schema, series_table, time_column, time_col_type,
       chunk_interval, origin, created_at
FROM pgseries.series_table;

CREATE OR REPLACE VIEW pgseries_information.chunks AS
SELECT c.series_schema, c.series_table,
       c.chunk_schema, c.chunk_name,
       c.range_lower, c.range_upper, c.space_index, l.state,
       c.created_at
FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id);

CREATE OR REPLACE VIEW pgseries_information.dimensions AS
SELECT series_schema, series_table, dim_name, dim_type, number_partitions
FROM pgseries.dimension;

CREATE OR REPLACE VIEW pgseries_information.jobs AS
SELECT job_id, job_name, job_type, target_schema, target_table,
       enabled, schedule, config, last_run, created_at
FROM pgseries.job;

CREATE OR REPLACE VIEW pgseries_information.compression_settings AS
SELECT series_schema, series_table, segmentby, orderby, orderby_desc,
       segment_rows, codecs, requant, created_at
FROM pgseries.compression_policy;

-- Per-chunk compression stats (function, not view: sibling tables are
-- dynamic names, so this needs dynamic SQL per chunk).
CREATE OR REPLACE FUNCTION pgseries.compressed_chunk_stats(
    p_schema NAME, p_table NAME)
RETURNS TABLE (chunk_name NAME, state TEXT, segments BIGINT,
               compressed_rows BIGINT, compressed_bytes BIGINT,
               heap_rows BIGINT)
LANGUAGE plpgsql STABLE
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    r       RECORD;
    v_cpart TEXT;
BEGIN
    PERFORM pgseries._series(p_schema, p_table);
    FOR r IN SELECT c.chunk_name, l.state
             FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
             WHERE c.series_schema = p_schema AND c.series_table = p_table
             ORDER BY c.range_lower NULLS LAST LOOP
        chunk_name := r.chunk_name;
        state := r.state;
        segments := 0;
        compressed_rows := 0;
        compressed_bytes := 0;
        v_cpart := format('%I.%I', p_schema, r.chunk_name::TEXT || '_compressed');
        IF to_regclass(v_cpart) IS NOT NULL THEN
            EXECUTE format('SELECT count(*), COALESCE(sum(seg_row_count), 0) FROM %s',
                           v_cpart) INTO segments, compressed_rows;
            SELECT pg_total_relation_size(to_regclass(v_cpart))
            INTO compressed_bytes;
        END IF;
        EXECUTE format('SELECT count(*) FROM %I.%I', p_schema, r.chunk_name)
        INTO heap_rows;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------------
-- 7. Roles + grants
-- ---------------------------------------------------------------------

DO $$
BEGIN
    CREATE ROLE pgseries_reader NOLOGIN;
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$$;

DO $$
BEGIN
    CREATE ROLE pgseries_writer NOLOGIN;
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$$;

DO $$
BEGIN
    CREATE ROLE pgseries_admin NOLOGIN;
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$$;

REVOKE ALL ON ALL TABLES IN SCHEMA pgseries FROM PUBLIC;
GRANT ALL ON ALL TABLES IN SCHEMA pgseries TO pgseries_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pgseries TO pgseries_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA pgseries_information TO pgseries_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA pgseries_information TO pgseries_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA pgseries_information TO pgseries_admin;

COMMIT;
