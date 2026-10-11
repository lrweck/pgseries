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
--        v0.2 space partitioning: add_dimension() with hash
--        sub-partitioning (in-place fan-out, tuple-keyed chunks);
--        space_range deferred, parallel-copy loader in scripts/.
--        v0.6 (columnar compression) implemented in pure SQL: pluggable
--        per-column codecs (raw/for/delta/auto/dict) over TOAST/lz4,
--        generated unpack, transparent base-table reads (UNION view +
--        INSTEAD OF DML), set-based multi-chunk packing via
--        compress_table(), range fast path, bench_codecs() for
--        empirical codec comparison.
--        add_dimension + post-v0.6 APIs are honest stubs raising
--        SQLSTATE 0A000 (feature_not_supported).
--
-- Known deviations from SPEC.md (all forced by PG or pure-SQL limits):
--   * compressed siblings live in a sibling partition tree
--     (<table>_compressed), NOT attached as partitions of the series
--     table — partitions require identical rowtypes. Heap stays
--     attached+empty as staging; the BASE TABLE NAME becomes a UNION
--     view at add_compression_policy time (the spec's
--     `create view <table>`), with the heap renamed to <table>_heap.
--     Bulk ingest (COPY) and internal movers address <table>_heap
--     directly: INSTEAD OF row triggers cost ~11x per row (measured:
--     COPY 200K rows 1.01s via view vs 0.09s heap-direct), so the hot
--     path never goes through them. TRUNCATE must also address the
--     heap (PG forbids TRUNCATE triggers on views).
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

-- v0.2 migrations (idempotent; earlier installs predate them).
ALTER TABLE pgseries.chunk ADD COLUMN IF NOT EXISTS parent_chunk NAME;
-- seg_space on every compressed tree (cascades to partitions).
DO $$ DECLARE r RECORD; BEGIN
    FOR r IN SELECT series_schema, series_table
             FROM pgseries.compression_policy LOOP
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS '
                       || 'seg_space INTEGER NOT NULL DEFAULT 0',
            r.series_schema, r.series_table || '_compressed');
    END LOOP;
END $$;
-- Pre-dimension rows are their own parent (single time-chunk chunks).
UPDATE pgseries.chunk SET parent_chunk = chunk_name
WHERE parent_chunk IS NULL;

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
DECLARE
    v_rel TEXT := pgseries._write_parent(p_schema, p_table);
BEGIN
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
    INTO v_all
    FROM pg_attribute
    WHERE attrelid = to_regclass(v_rel)
      AND attnum > 0 AND NOT attisdropped AND attgenerated = '';

    SELECT COALESCE(array_agg(attname ORDER BY attnum), '{}')
    INTO v_identities
    FROM pg_attribute
    WHERE attrelid = to_regclass(v_rel)
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

-- Bounded representative sample of a series table as a single SQL
-- string: UNION ALL over the K most recent leaves per branch (heap
-- leaves raw, compressed leaves via unpack), each LIMIT-capped, so
-- cost is bounded by ~2x p_nrows rows read — no full scan, no sort,
-- no TABLESAMPLE page-granularity flakiness (SYSTEM returns 0 rows
-- too often across many small partitions), no reltuples trust.
-- Deterministic for a fixed catalog (leaves ordered by range).
-- Input order is irrelevant: _pack_segments sorts by orderby itself.
CREATE OR REPLACE FUNCTION pgseries._sample_sql(
    p_schema NAME, p_table NAME, p_nrows INTEGER)
RETURNS TEXT
LANGUAGE plpgsql VOLATILE
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_heapq  TEXT := pgseries._write_parent(p_schema, p_table);
    v_ctpl   TEXT := format('%I.%I', p_schema, p_table || '_compressed');
    v_unpack TEXT := 'pgseries.unpack_' || pgseries._slug(p_schema, p_table);
    v_cap    INTEGER := GREATEST(p_nrows / 16, 25);
    v_segcap INTEGER := GREATEST(p_nrows / 1000 / 4, 2);
    v_heap   TEXT;
    v_comp   TEXT;
BEGIN
    -- One catalog scan into a staging set: recent-leaning spread of
    -- leaves (range order; adopted chunks without parsed bounds sort
    -- last and still participate). Recent-only sampling misses
    -- backfilled tables whose newest chunks are pre-created empties,
    -- so the oldest leaves join too. Caps keep every leaf scan
    -- bounded (LIMIT stops early; empty leaves are ~1ms).
    IF to_regclass('pg_temp.pgseries_sample_leaves') IS NULL THEN
        CREATE TEMP TABLE pgseries_sample_leaves (
            cschema NAME, cname NAME, lo TIMESTAMPTZ) ON COMMIT DROP;
    END IF;
    DELETE FROM pgseries_sample_leaves;
    INSERT INTO pgseries_sample_leaves
    SELECT c.chunk_schema, c.chunk_name, c.range_lower
    FROM pgseries.chunk c
    WHERE c.series_schema = p_schema AND c.series_table = p_table
    ORDER BY c.range_lower DESC NULLS LAST, c.chunk_name DESC
    LIMIT 24;
    INSERT INTO pgseries_sample_leaves
    SELECT c.chunk_schema, c.chunk_name, c.range_lower
    FROM pgseries.chunk c
    WHERE c.series_schema = p_schema AND c.series_table = p_table
      AND (c.chunk_schema, c.chunk_name) NOT IN (
          SELECT cschema, cname FROM pgseries_sample_leaves)
    ORDER BY c.range_lower ASC NULLS LAST, c.chunk_name ASC
    LIMIT 8;
    SELECT COALESCE(string_agg(
               format('(SELECT * FROM %I.%I LIMIT %s)',
                      cschema, cname, v_cap),
               ' UNION ALL '), '')
    INTO v_heap
    FROM pgseries_sample_leaves;

    -- Recent-leaning spread of heap leaves (catalog range order;
    -- adopted chunks without parsed bounds sort last and still
    -- participate). Recent-only sampling misses backfilled tables
    -- whose newest chunks are pre-created empties, so the oldest
    -- leaves outside the recent set are added too. Caps keep every
    -- leaf scan bounded (LIMIT stops early; empty leaves are ~1ms).

    -- K most recent compressed leaves, unpacked (cap in segments).
    IF to_regclass(v_ctpl) IS NOT NULL
       AND to_regproc(v_unpack) IS NOT NULL THEN
        -- Shared node siblings (one per time-chunk); unpack sees all
        -- slices, no space filter needed for a sample.
        SELECT COALESCE(string_agg(
                   format('(SELECT u.* FROM (SELECT * FROM %I.%I LIMIT %s) AS c, '
                          || 'LATERAL %s(c) AS u)',
                          t.cschema,
                          (t.node::TEXT || '_compressed')::NAME,
                          v_segcap, v_unpack),
                   ' UNION ALL '), '')
        INTO v_comp
        FROM (SELECT DISTINCT t.cschema,
                              COALESCE(c.parent_chunk, c.chunk_name) AS node
              FROM pgseries_sample_leaves t
              JOIN pgseries.chunk c
                ON c.chunk_schema = t.cschema AND c.chunk_name = t.cname
              JOIN pgseries.chunk_lease l USING (chunk_id)
              WHERE l.state IN ('compressing', 'compressed')
                AND to_regclass(format('%I.%I', t.cschema,
                                       (COALESCE(c.parent_chunk, c.chunk_name)::TEXT
                                        || '_compressed')::NAME))
                    IS NOT NULL
              ORDER BY 2 DESC LIMIT 8) t;
    ELSE
        v_comp := '';
    END IF;
    IF v_heap = '' AND v_comp = '' THEN
        -- No cataloged chunks (unregistered table?): fall back to the
        -- parents directly, LIMIT-capped with early-stop scans.
        v_heap := format('(SELECT * FROM %s LIMIT %s)', v_heapq, v_cap);
    END IF;
    RETURN v_heap
        || CASE WHEN v_heap <> '' AND v_comp <> ''
                THEN ' UNION ALL ' ELSE '' END
        || v_comp;
END;
$$;
-- Write-parent resolution. Before any compression policy exists the
-- series table is a plain partitioned table and writes target it
-- directly. add_compression_policy renames it to <table>_heap and
-- installs a transparent UNION view under the base name; from then on
-- all internal DML/DDL must address the heap explicitly (the view's
-- INSTEAD OF triggers are user-convenience only — measured ~11x per
-- row — so bulk paths never go through them).
CREATE OR REPLACE FUNCTION pgseries._heap_name(p_table NAME)
RETURNS NAME
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT (p_table::TEXT || '_heap')::NAME;
$$;

CREATE OR REPLACE FUNCTION pgseries._is_transparent(p_schema NAME, p_table NAME)
RETURNS BOOLEAN
LANGUAGE sql STABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT to_regclass(format('%I.%I', p_schema,
        pgseries._heap_name(p_table))) IS NOT NULL;
$$;

-- Qualified write target: heap when the transparent view is installed,
-- else the base table itself.
CREATE OR REPLACE FUNCTION pgseries._write_parent(p_schema NAME, p_table NAME)
RETURNS TEXT
LANGUAGE sql STABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT CASE WHEN pgseries._is_transparent(p_schema, p_table)
                THEN format('%I.%I', p_schema, pgseries._heap_name(p_table))
                ELSE format('%I.%I', p_schema, p_table) END;
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
-- Fan a hash-partitioned node out into N leaves (<node>_s0..sN-1).
-- Leaves inherit the rowtype (identity dropped, generated kept —
-- same rule as _create_chunk); indexes cascade from the series
-- parent at ATTACH time (verified through two levels).
CREATE OR REPLACE FUNCTION pgseries._subpartition_node(
    p_schema NAME, p_node NAME, p_col NAME, p_n INTEGER)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    i     INTEGER;
    v_lf  NAME;
    v_c   NAME;
BEGIN
    FOR i IN 0 .. p_n - 1 LOOP
        v_lf := (p_node::TEXT || '_s' || i)::NAME;
        IF to_regclass(format('%I.%I', p_schema, v_lf)) IS NOT NULL THEN
            RAISE EXCEPTION 'pgseries: sub-partition name %.% already exists',
                p_schema, v_lf
                USING ERRCODE = 'duplicate_table';
        END IF;
        EXECUTE format(
            'CREATE TABLE %I.%I (LIKE %I.%I '
            || 'INCLUDING DEFAULTS INCLUDING CONSTRAINTS '
            || 'INCLUDING GENERATED INCLUDING IDENTITY)',
            p_schema, v_lf, p_schema, p_node);
        FOR v_c IN
            SELECT a.attname FROM pg_attribute a
            WHERE a.attrelid = to_regclass(
                      format('%I.%I', p_schema, p_node))
              AND a.attidentity <> ''
        LOOP
            EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I DROP IDENTITY',
                p_schema, v_lf, v_c);
        END LOOP;
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I '
                       || 'FOR VALUES WITH (MODULUS %s, REMAINDER %s)',
            p_schema, p_node, p_schema, v_lf, p_n, i);
    END LOOP;
END;
$$;

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
    v_cols     TEXT;
    v_c        NAME;
    v_wp       TEXT := pgseries._write_parent(p_schema, p_table);
    v_sdim     NAME;
    v_sn       INTEGER;
    v_leaf     NAME;
    i          INTEGER;
BEGIN
    s := pgseries._series(p_schema, p_table);

    -- Idempotency: same lower bound already registered -> return it
    -- (lowest space_index for determinism on fanned-out chunks).
    PERFORM 1 FROM pgseries.chunk
    WHERE series_schema = p_schema AND series_table = p_table
      AND range_lower = p_lower;
    IF FOUND THEN
        SELECT chunk_name INTO v_chunk FROM pgseries.chunk
        WHERE series_schema = p_schema AND series_table = p_table
          AND range_lower = p_lower
        ORDER BY space_index LIMIT 1;
        RETURN v_chunk;
    END IF;

    v_chunk := pgseries._chunk_name(p_table, p_lower);

    -- Name taken by an unrelated relation -> fail loudly, never squash.
    IF to_regclass(format('%I.%I', p_schema, v_chunk)) IS NOT NULL THEN
        RAISE EXCEPTION 'pgseries: chunk name %.% already exists with different bounds',
            p_schema, v_chunk
            USING ERRCODE = 'duplicate_table';
    END IF;

    -- Space fan-out (if any): the node itself is hash-partitioned so
    -- _subpartition_node can hang leaves off it below.
    SELECT dim_name, number_partitions
    INTO v_sdim, v_sn
    FROM pgseries.dimension
    WHERE series_schema = p_schema AND series_table = p_table
      AND dim_type = 'space_hash';
    EXECUTE format(
        'CREATE TABLE %I.%I (LIKE %s '
        || 'INCLUDING DEFAULTS INCLUDING CONSTRAINTS '
        || 'INCLUDING GENERATED INCLUDING IDENTITY)%s',
        p_schema, v_chunk, v_wp,
        CASE WHEN FOUND
             THEN format(' PARTITION BY HASH (%I)', v_sdim)
             ELSE '' END);

    -- Leaves may not own identity columns (verified): the parent's
    -- shared identity governs. Generated columns must stay (verified:
    -- ATTACH demands them). Drop only the identity property.
    FOR v_c IN
        SELECT a.attname FROM pg_attribute a
        WHERE a.attrelid = to_regclass(v_wp)
          AND a.attidentity <> ''
    LOOP
        EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I DROP IDENTITY',
            p_schema, v_chunk, v_c);
    END LOOP;

    -- Fan out before the drain so it lands straight in leaves.
    IF v_sdim IS NOT NULL THEN
        PERFORM pgseries._subpartition_node(
            p_schema, v_chunk, v_sdim, v_sn);
    END IF;

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
        'ALTER TABLE %s ATTACH PARTITION %I.%I '
        || 'FOR VALUES FROM (%L::%s) TO (%L::%s)',
        v_wp, p_schema, v_chunk,
        p_lower::TEXT, s.time_col_type,
        p_upper::TEXT, s.time_col_type);

    -- Space fan-out: one catalog row per leaf (space_index set).
    -- Leaves were created before the drain above; the router fanned
    -- rows into them.
    IF v_sdim IS NOT NULL THEN
        FOR i IN 0 .. v_sn - 1 LOOP
            v_leaf := (v_chunk::TEXT || '_s' || i)::NAME;
            INSERT INTO pgseries.chunk
                (series_schema, series_table, chunk_schema, chunk_name,
                 range_lower, range_upper, space_index, parent_chunk)
            VALUES (p_schema, p_table, p_schema, v_leaf,
                    p_lower, p_upper, i, v_chunk)
            RETURNING chunk_id INTO v_id;
            INSERT INTO pgseries.chunk_lease (chunk_id, state)
            VALUES (v_id, 'pending');
            PERFORM pgseries._lease_transition(p_schema, v_leaf, 'ready');
        END LOOP;
    ELSE
        INSERT INTO pgseries.chunk
            (series_schema, series_table, chunk_schema, chunk_name,
             range_lower, range_upper, space_index, parent_chunk)
        VALUES (p_schema, p_table, p_schema, v_chunk,
                p_lower, p_upper, 0, v_chunk)
        RETURNING chunk_id INTO v_id;
        INSERT INTO pgseries.chunk_lease (chunk_id, state)
        VALUES (v_id, 'pending');
        PERFORM pgseries._lease_transition(p_schema, v_chunk, 'ready');
    END IF;
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
    -- Multi-level sources are refused: sub-partitioned children would
    -- register as single chunks (wrong granularity); adopt the top
    -- level of a 1-level table, or add_dimension() afterwards.
    IF EXISTS (SELECT 1
               FROM pg_inherits inh
               JOIN pg_class par ON par.oid = inh.inhparent
               JOIN pg_namespace pn ON pn.oid = par.relnamespace
               JOIN pg_class cc ON cc.oid = inh.inhrelid
               WHERE pn.nspname = p_schema::TEXT AND par.relname = p_table::TEXT
                 AND cc.relkind = 'p') THEN
        RAISE EXCEPTION 'pgseries: %.% has sub-partitioned children; adopt 1-level tables only',
            p_schema, p_table
            USING ERRCODE = 'feature_not_supported';
    END IF;
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
             range_lower, range_upper, space_index, parent_chunk)
        VALUES (p_schema, p_table, r.cschema, r.cname, v_lo, v_hi, 0,
                r.cname)
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
                PERFORM pgseries._create_chunk(p_schema, p_table, v_lo, v_hi);
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
            PERFORM pgseries._create_chunk(p_schema, p_table, v_lo, v_hi);
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
    v_wp       TEXT;
BEGIN
    s := pgseries._series(p_schema, p_table);
    PERFORM pgseries._table_lock(p_schema, p_table);
    v_wp := pgseries._write_parent(p_schema, p_table);
    v_default := pgseries._default_name(p_table);
    SELECT c.v_all INTO v_cols
    FROM pgseries._copy_columns(p_schema, p_table) AS c;

    EXECUTE format('SELECT count(*) FROM %I.%I WHERE %I IS NULL',
        p_schema, v_default, s.time_column) INTO v_nulls;
    IF v_nulls > 0 THEN
        RAISE NOTICE 'pgseries: % NULL-time rows stay in %.% (unroutable)',
            v_nulls, p_schema, v_default;
    END IF;

    -- Set-based data move: one grouping scan finds every missing bucket,
    -- _create_chunk drains each bucket pre-ATTACH (its own DELETE+INSERT,
    -- required so the ATTACH validation scan passes), then a SINGLE
    -- sweep relocates whatever arrived concurrently mid-operation.
    -- Previously the sweep was per-bucket (2N statements, N scans).
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
            v_total := v_total + r.n;
        END LOOP;
        EXECUTE format(
            'WITH moved AS (DELETE FROM %I.%I '
            || 'WHERE %I IS NOT NULL RETURNING *) '
            || 'INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM moved',
            p_schema, v_default, s.time_column,
            v_wp, v_cols, v_cols);
        GET DIAGNOSTICS v_moved = ROW_COUNT;
        RETURN v_total + v_moved;
    END IF;

    FOR r IN EXECUTE format(
        'SELECT date_bin(%L::INTERVAL, %I::TIMESTAMPTZ, %L::TIMESTAMPTZ) AS b, count(*) AS n '
        'FROM %I.%I WHERE %I IS NOT NULL GROUP BY 1 ORDER BY 1',
        s.chunk_interval, s.time_column, s.origin,
        p_schema, v_default, s.time_column)
    LOOP
        -- _create_chunk drains this bucket from DEFAULT into the new
        -- table pre-ATTACH (r.n rows).
        PERFORM pgseries._create_chunk(
            p_schema, p_table, r.b, r.b + s.chunk_interval);
        v_total := v_total + r.n;
    END LOOP;
    EXECUTE format(
        'WITH moved AS (DELETE FROM %I.%I '
        || 'WHERE %I IS NOT NULL RETURNING *) '
        || 'INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM moved',
        p_schema, v_default, s.time_column,
        v_wp, v_cols, v_cols);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    RETURN v_total + v_moved;
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
    v_node   TEXT := '';
    v_cschema NAME;
    q        RECORD;
BEGIN
    PERFORM pgseries._series(p_schema, p_table);
    -- v0.3+ will refuse chunks overlapping a cagg refresh window here.
    -- No caggs are registered yet, so the check is a no-op by construction.
    PERFORM pgseries._table_lock(p_schema, p_table);

    -- Leaf rows ordered so siblings are adjacent; each time-chunk node
    -- (possibly hash-sub-partitioned) drops once with all its leaves.
    -- Returns node names (== chunk names on single-dimension tables).
    FOR r IN
        SELECT c.chunk_id, c.chunk_schema, c.chunk_name,
               COALESCE(c.parent_chunk, c.chunk_name) AS node
        FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
        WHERE c.series_schema = p_schema AND c.series_table = p_table
          AND c.range_upper IS NOT NULL AND c.range_upper < v_cutoff
          AND l.state IN ('ready', 'compressed')
        ORDER BY c.range_upper, node
        FOR UPDATE OF l
    LOOP
        IF r.node::TEXT = v_node THEN
            -- Sibling leaf of an already-dropped node (its catalog row
            -- went with the node loop below; locks were taken upfront).
            CONTINUE;
        END IF;
        v_node := r.node::TEXT;
        SELECT chunk_schema INTO v_cschema FROM pgseries.chunk
        WHERE series_schema = p_schema AND series_table = p_table
          AND COALESCE(parent_chunk, chunk_name) = r.node
        LIMIT 1;
        IF p_dry_run THEN
            RETURN NEXT (v_cschema::TEXT || '.' || r.node::TEXT);
            CONTINUE;
        END IF;
        -- Flip every leaf first so the refresh below drops the
        -- node's leg; the subsequent DROPs then hit no dependents.
        FOR q IN
            SELECT c2.chunk_schema, c2.chunk_name
            FROM pgseries.chunk c2
            WHERE c2.series_schema = p_schema AND c2.series_table = p_table
              AND COALESCE(c2.parent_chunk, c2.chunk_name) = r.node
        LOOP
            PERFORM pgseries._lease_transition(
                q.chunk_schema, q.chunk_name, 'leased_for_drop');
        END LOOP;
        PERFORM pgseries.refresh_read_view(p_schema, p_table);
        -- The node DROP cascades to hash leaves; the shared
        -- compressed sibling goes with it.
        EXECUTE format('DROP TABLE %I.%I', v_cschema, r.node);
        IF to_regclass(format('%I.%I', v_cschema,
                              (r.node::TEXT || '_compressed')::NAME)) IS NOT NULL THEN
            EXECUTE format('DROP TABLE %I.%I', v_cschema,
                           (r.node::TEXT || '_compressed')::NAME);
        END IF;
        DELETE FROM pgseries.chunk
        WHERE series_schema = p_schema AND series_table = p_table
          AND chunk_schema = v_cschema
          AND COALESCE(parent_chunk, chunk_name) = r.node;
        RETURN NEXT (v_cschema::TEXT || '.' || r.node::TEXT);
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
        ('effective_io_concurrency', '200', 'NVMe-friendly planner costing'),
        ('constraint_exclusion', 'on', 'UNION-leg pruning on the transparent base view (plan-time)')
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

-- Space partitioning: fan every time-chunk out into p_n hash
-- leaves on p_column, in place and without losing rows. Each chunk is
-- converted in turn: build a hash-partitioned twin, move rows
-- (single DELETE+INSERT, set-based), swap names, re-attach with the
-- original time bounds (typed literals — bare literals misfire
-- validation with a misleading hash-leaf error, verified). New chunks
-- fan out via _create_chunk from here on. Writes should quiesce
-- during the call: a concurrent insert landing in DEFAULT inside the
-- drain-attach window aborts the whole (transactional, retry-safe)
-- conversion.
CREATE OR REPLACE FUNCTION pgseries.add_dimension(
    p_schema NAME, p_table NAME, p_column NAME,
    p_type TEXT DEFAULT 'space_hash', p_n INTEGER DEFAULT 16)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s        pgseries.series_table%ROWTYPE;
    v_wp     TEXT := pgseries._write_parent(p_schema, p_table);
    v_coltype TEXT;
    v_tmp    NAME;
    v_cols   TEXT;
    r        RECORD;
    q        RECORD;
    i        INTEGER;
    v_id     INTEGER;
BEGIN
    s := pgseries._series(p_schema, p_table);
    PERFORM pgseries._table_lock(p_schema, p_table);
    IF p_type <> 'space_hash' THEN
        RAISE EXCEPTION 'pgseries: space_range dimensions ship separately (space_hash only for now)'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    IF p_n IS NULL OR p_n < 2 OR p_n > 128 THEN
        RAISE EXCEPTION 'pgseries: n must be between 2 and 128, got %', p_n
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    SELECT format_type(a.atttypid, NULL) INTO v_coltype
    FROM pg_attribute a
    WHERE a.attrelid = to_regclass(v_wp)
      AND a.attname = p_column::TEXT AND a.attnum > 0 AND NOT a.attisdropped;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: column % does not exist on %.%',
            p_column, p_schema, p_table
            USING ERRCODE = 'undefined_column';
    END IF;
    IF p_column = s.time_column THEN
        RAISE EXCEPTION 'pgseries: time column % cannot be a space dimension',
            p_column
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF EXISTS (SELECT 1 FROM pgseries.dimension
               WHERE series_schema = p_schema AND series_table = p_table
                 AND dim_type <> 'time') THEN
        RAISE EXCEPTION 'pgseries: %.% already has a space dimension (one max)',
            p_schema, p_table
            USING ERRCODE = 'duplicate_object';
    END IF;
    SELECT count(*) INTO i
    FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
    WHERE c.series_schema = p_schema AND c.series_table = p_table
      AND l.state IN ('compressing', 'compressed');
    IF i > 0 THEN
        RAISE EXCEPTION 'pgseries: % compressed chunks exist; decompress_chunk them first',
            i
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    -- Hash-partitioned leaves inherit the parent's unique indexes, so
    -- every PK/UNIQUE must cover the space column or ATTACH fails.
    FOR q IN
        SELECT c.conname
        FROM pg_constraint c
        WHERE c.conrelid = to_regclass(v_wp)
          AND c.contype IN ('p', 'u')
          AND NOT EXISTS (
              SELECT 1 FROM unnest(c.conkey) AS k(attnum)
              JOIN pg_attribute a ON a.attrelid = c.conrelid
                                 AND a.attnum = k.attnum
              WHERE a.attname = p_column::TEXT)
    LOOP
        RAISE EXCEPTION 'pgseries: constraint % lacks the space column % (drop or redefine it first)',
            q.conname, p_column
            USING ERRCODE = 'invalid_parameter_value';
    END LOOP;

    INSERT INTO pgseries.dimension
        (series_schema, series_table, dim_name, dim_type, number_partitions)
    VALUES (p_schema, p_table, p_column, 'space_hash', p_n);

    SELECT c.v_all INTO v_cols
    FROM pgseries._copy_columns(p_schema, p_table) AS c;
    FOR r IN SELECT c.chunk_schema, c.chunk_name,
                    c.range_lower, c.range_upper
             FROM pgseries.chunk c
             WHERE c.series_schema = p_schema AND c.series_table = p_table
             ORDER BY c.range_lower NULLS LAST LOOP
        IF r.range_lower IS NULL OR r.range_upper IS NULL THEN
            RAISE EXCEPTION 'pgseries: chunk %.% has unknown bounds (adopted); drop it before add_dimension',
                r.chunk_schema, r.chunk_name
                USING ERRCODE = 'object_not_in_prerequisite_state';
        END IF;
        v_tmp := (r.chunk_name::TEXT || '_new')::NAME;
        IF to_regclass(format('%I.%I', r.chunk_schema, v_tmp)) IS NOT NULL THEN
            RAISE EXCEPTION 'pgseries: scratch name %.% already exists',
                r.chunk_schema, v_tmp
                USING ERRCODE = 'duplicate_table';
        END IF;
        EXECUTE format(
            'CREATE TABLE %I.%I (LIKE %I.%I '
            || 'INCLUDING DEFAULTS INCLUDING CONSTRAINTS '
            || 'INCLUDING GENERATED INCLUDING IDENTITY) '
            || 'PARTITION BY HASH (%I)',
            r.chunk_schema, v_tmp, r.chunk_schema, r.chunk_name,
            p_column);
        -- Neither the node nor its leaves may own identity (ATTACH
        -- rejects it); the series parent's shared identity governs,
        -- same rule as _create_chunk.
        FOR q IN
            SELECT a.attname FROM pg_attribute a
            WHERE a.attrelid = to_regclass(
                      format('%I.%I', r.chunk_schema, v_tmp))
              AND a.attidentity <> ''
        LOOP
            EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I DROP IDENTITY',
                r.chunk_schema, v_tmp, q.attname);
        END LOOP;
        PERFORM pgseries._subpartition_node(
            r.chunk_schema, v_tmp, p_column, p_n);
        EXECUTE format(
            'WITH moved AS (DELETE FROM %I.%I RETURNING *) '
            || 'INSERT INTO %I.%I (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM moved',
            r.chunk_schema, r.chunk_name,
            r.chunk_schema, v_tmp, v_cols, v_cols);
        -- Concurrent arrivals stashed in DEFAULT meanwhile: drain first
        -- so the ATTACH validation scan passes.
        EXECUTE format(
            'WITH moved AS (DELETE FROM %I.%I '
            || 'WHERE %I >= %L::%s AND %I < %L::%s RETURNING *) '
            || 'INSERT INTO %I.%I (%s) OVERRIDING SYSTEM VALUE SELECT %s FROM moved',
            p_schema, pgseries._default_name(p_table),
            s.time_column, r.range_lower::TEXT, s.time_col_type,
            s.time_column, r.range_upper::TEXT, s.time_col_type,
            r.chunk_schema, v_tmp, v_cols, v_cols);
        EXECUTE format('ALTER TABLE %s DETACH PARTITION %I.%I',
            v_wp, r.chunk_schema, r.chunk_name);
        EXECUTE format('DROP TABLE %I.%I', r.chunk_schema, r.chunk_name);
        EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',
            r.chunk_schema, v_tmp, r.chunk_name);
        -- Leaves keep their scratch names on RENAME; fix them so the
        -- catalog (<node>_s<i>) matches reality.
        FOR i IN 0 .. p_n - 1 LOOP
            EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',
                r.chunk_schema,
                (r.chunk_name::TEXT || '_new_s' || i)::NAME,
                (r.chunk_name::TEXT || '_s' || i)::NAME);
        END LOOP;
        EXECUTE format(
            'ALTER TABLE %s ATTACH PARTITION %I.%I '
            || 'FOR VALUES FROM (%L::%s) TO (%L::%s)',
            v_wp, r.chunk_schema, r.chunk_name,
            r.range_lower::TEXT, s.time_col_type,
            r.range_upper::TEXT, s.time_col_type);
        -- The pre-dim 1-level sibling (if any) is vacated: every
        -- chunk here is ready/compressed-free by the guard above, so
        -- its sibling holds zero segments; fail loudly otherwise.
        IF to_regclass(format('%I.%I', r.chunk_schema,
                               (r.chunk_name::TEXT || '_compressed')::NAME))
           IS NOT NULL THEN
            EXECUTE format('SELECT count(*) FROM %I.%I',
                r.chunk_schema,
                (r.chunk_name::TEXT || '_compressed')::NAME)
            INTO v_id;
            IF v_id > 0 THEN
                RAISE EXCEPTION 'pgseries: sibling %.% is not empty; decompress first',
                    r.chunk_schema,
                    (r.chunk_name::TEXT || '_compressed')::NAME
                    USING ERRCODE = 'object_not_in_prerequisite_state';
            END IF;
            EXECUTE format('DROP TABLE %I.%I',
                r.chunk_schema,
                (r.chunk_name::TEXT || '_compressed')::NAME);
        END IF;
        DELETE FROM pgseries.chunk
        WHERE series_schema = p_schema AND series_table = p_table
          AND chunk_schema = r.chunk_schema AND chunk_name = r.chunk_name;
        FOR i IN 0 .. p_n - 1 LOOP
            INSERT INTO pgseries.chunk
                (series_schema, series_table, chunk_schema, chunk_name,
                 range_lower, range_upper, space_index, parent_chunk)
            VALUES (p_schema, p_table, r.chunk_schema,
                    (r.chunk_name::TEXT || '_s' || i)::NAME,
                    r.range_lower, r.range_upper, i, r.chunk_name)
            RETURNING chunk_id INTO v_id;
            INSERT INTO pgseries.chunk_lease (chunk_id, state)
            VALUES (v_id, 'ready');
        END LOOP;
    END LOOP;
    PERFORM pgseries.refresh_read_view(p_schema, p_table);
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

-- DML trigger bodies for the transparent base view. Generated per
-- series table at swap time (column lists are static then). INSERT
-- routes to the heap with generated-stored columns omitted and
-- OVERRIDING SYSTEM VALUE so explicit identity values keep working
-- (DEFAULT still generates). UPDATE/DELETE match heap rows on
-- whole-row equality with OLD; when nothing matches, the row either
-- never existed (standard 0-row no-op) or lives in the compressed
-- store — the seg-range probe distinguishes and raises read-only.
-- Identity and generated-stored columns are excluded from SET
-- (GENERATED ALWAYS rejects updates); all columns participate in the
-- match, floats included (RAW storage is bit-exact).
CREATE OR REPLACE FUNCTION pgseries._write_trg_ddl(
    p_schema NAME, p_table NAME)
RETURNS SETOF TEXT
LANGUAGE plpgsql STABLE
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s        pgseries.series_table%ROWTYPE;
    v_heap   TEXT := format('%I.%I', p_schema, pgseries._heap_name(p_table));
    v_tpl    TEXT := format('%I.%I', p_schema, p_table || '_compressed');
    v_unpack TEXT := 'pgseries.unpack_' || pgseries._slug(p_schema, p_table);
    v_base   TEXT := format('%I.%I', p_schema, p_table);
    v_fbase  TEXT := 'pgseries.write_' || pgseries._slug(p_schema, p_table);
    v_ins    TEXT := '';
    v_vals   TEXT := '';
    v_set    TEXT := '';
    v_match  TEXT := '';
    r        RECORD;
    v_id     TEXT;
    v_gen    TEXT;
BEGIN
    s := pgseries._series(p_schema, p_table);

    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_schema, p_table, s.time_column, '{}') LOOP
        IF r.kind <> 'gen' THEN
            v_ins := v_ins || format(', %I', r.attname);
            v_vals := v_vals || format(', NEW.%I', r.attname);
        END IF;
        -- Plain data columns are updatable; identity/generated match only.
        SELECT attidentity, attgenerated INTO v_id, v_gen FROM pg_attribute
        WHERE attrelid = to_regclass(v_heap)
          AND attname = r.attname AND attnum > 0 AND NOT attisdropped;
        IF FOUND AND v_id = '' AND v_gen = '' THEN
            v_set := v_set || format(', %I = NEW.%I', r.attname, r.attname);
        END IF;
    END LOOP;
    -- Whole-row match on every physical column (heap stores the
    -- generated values too, so they participate here).
    v_match := '';
    FOR r IN SELECT quote_ident(attname) AS c FROM pg_attribute
             WHERE attrelid = to_regclass(v_heap)
               AND attnum > 0 AND NOT attisdropped
             ORDER BY attnum LOOP
        v_match := v_match || format(' AND h.%s IS NOT DISTINCT FROM OLD.%s',
                                     r.c, r.c);
    END LOOP;
    v_match := substring(v_match from 6);
    v_ins := substring(v_ins from 3);
    v_vals := substring(v_vals from 3);
    v_set := substring(v_set from 3);

    RETURN NEXT format(
        'CREATE OR REPLACE FUNCTION %s_ins() RETURNS trigger '
        || 'LANGUAGE plpgsql SET search_path = pgseries, pg_catalog AS %s '
        || 'DECLARE r %s%%ROWTYPE; BEGIN '
        || 'INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE VALUES (%s) '
        || 'RETURNING * INTO r; NEW := r; RETURN NEW; END %s',
        v_fbase, '$pgseries_w$', v_heap, v_heap, v_ins, v_vals,
        '$pgseries_w$');
    RETURN NEXT format(
        'CREATE OR REPLACE FUNCTION %s_upd() RETURNS trigger '
        || 'LANGUAGE plpgsql SET search_path = pgseries, pg_catalog AS %s '
        || 'DECLARE v_n BIGINT; v_hit BOOLEAN; BEGIN '
        || 'UPDATE %s AS h SET %s WHERE %s; '
        || 'GET DIAGNOSTICS v_n = ROW_COUNT; '
        || 'IF v_n = 0 THEN '
        || 'SELECT EXISTS (SELECT 1 FROM %s AS c '
        || 'WHERE c.seg_max_ts >= (OLD.%I)::TIMESTAMPTZ '
        || 'AND c.seg_min_ts <= (OLD.%I)::TIMESTAMPTZ '
        || 'AND EXISTS (SELECT 1 FROM %s(c) AS u WHERE %s)) INTO v_hit; '
        || 'IF v_hit THEN RAISE EXCEPTION %s USING ERRCODE = %s; END IF; '
        || 'END IF; RETURN NEW; END %s',
        v_fbase, '$pgseries_w$', v_heap, v_set, v_match,
        v_tpl, s.time_column, s.time_column, v_unpack,
        (SELECT string_agg(format('u.%I IS NOT DISTINCT FROM OLD.%I',
                                  attname, attname), ' AND ')
         FROM pgseries._colinfo(p_schema, p_table, s.time_column, '{}')
         WHERE kind <> 'gen'),
        quote_literal('pgseries: row lives in the compressed store '
                      || '(read-only; decompress_chunk to write)'),
        quote_literal('read_only_sql_transaction'),
        '$pgseries_w$');
    RETURN NEXT format(
        'CREATE OR REPLACE FUNCTION %s_del() RETURNS trigger '
        || 'LANGUAGE plpgsql SET search_path = pgseries, pg_catalog AS %s '
        || 'DECLARE v_n BIGINT; v_hit BOOLEAN; BEGIN '
        || 'DELETE FROM %s AS h WHERE %s; '
        || 'GET DIAGNOSTICS v_n = ROW_COUNT; '
        || 'IF v_n = 0 THEN '
        || 'SELECT EXISTS (SELECT 1 FROM %s AS c '
        || 'WHERE c.seg_max_ts >= (OLD.%I)::TIMESTAMPTZ '
        || 'AND c.seg_min_ts <= (OLD.%I)::TIMESTAMPTZ '
        || 'AND EXISTS (SELECT 1 FROM %s(c) AS u WHERE %s)) INTO v_hit; '
        || 'IF v_hit THEN RAISE EXCEPTION %s USING ERRCODE = %s; END IF; '
        || 'END IF; RETURN OLD; END %s',
        v_fbase, '$pgseries_w$', v_heap, v_match,
        v_tpl, s.time_column, s.time_column, v_unpack,
        (SELECT string_agg(format('u.%I IS NOT DISTINCT FROM OLD.%I',
                                  attname, attname), ' AND ')
         FROM pgseries._colinfo(p_schema, p_table, s.time_column, '{}')
         WHERE kind <> 'gen'),
        quote_literal('pgseries: row lives in the compressed store '
                      || '(read-only; decompress_chunk to write)'),
        quote_literal('read_only_sql_transaction'),
        '$pgseries_w$');
    RETURN NEXT format(
        'CREATE TRIGGER %I INSTEAD OF INSERT ON %s '
        || 'FOR EACH ROW EXECUTE FUNCTION %s_ins()',
        p_table::TEXT || '_w_ins', v_base, v_fbase);
    RETURN NEXT format(
        'CREATE TRIGGER %I INSTEAD OF UPDATE ON %s '
        || 'FOR EACH ROW EXECUTE FUNCTION %s_upd()',
        p_table::TEXT || '_w_upd', v_base, v_fbase);
    RETURN NEXT format(
        'CREATE TRIGGER %I INSTEAD OF DELETE ON %s '
        || 'FOR EACH ROW EXECUTE FUNCTION %s_del()',
        p_table::TEXT || '_w_del', v_base, v_fbase);
END;
$$;

-- Swap the base table name to a transparent UNION view: rename the
-- heap parent aside, install INSTEAD OF DML triggers + read views.
-- Reads via the base name then include compressed rows; bulk ingest
-- (COPY) and internal movers keep addressing <table>_heap directly.
CREATE OR REPLACE FUNCTION pgseries._make_transparent(
    p_schema NAME, p_table NAME)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_heap NAME := pgseries._heap_name(p_table);
    v_ddl  TEXT;
    s      pgseries.series_table%ROWTYPE;
BEGIN
    s := pgseries._series(p_schema, p_table);
    IF to_regclass(format('%I.%I', p_schema, v_heap)) IS NOT NULL THEN
        RAISE EXCEPTION 'pgseries: %.% already exists (name reserved for the heap)',
            p_schema, v_heap
            USING ERRCODE = 'duplicate_table';
    END IF;
    -- Upgrade path: an install predating transparency already owns
    -- <table>_read / _read_range bound to the table rowtype; the new
    -- definitions bind to the view rowtype, so drop first (recreated
    -- below — data untouched).
    IF to_regclass(format('%I.%I', p_schema, p_table || '_read')) IS NOT NULL THEN
        EXECUTE format('DROP VIEW %I.%I', p_schema, p_table || '_read');
    END IF;
    -- Upgrade path: a pre-transparency _read_range is bound to the
    -- table rowtype and would block the view-bound replacement.
    IF to_regprocedure(format('%I.%I(%s, %s)', p_schema,
                              p_table || '_read_range',
                              s.time_col_type, s.time_col_type)) IS NOT NULL THEN
        EXECUTE format('DROP FUNCTION %I.%I(%s, %s)',
                       p_schema, p_table || '_read_range',
                       s.time_col_type, s.time_col_type);
    END IF;
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',
                   p_schema, p_table, v_heap);
    -- Unpack now (heap rowtype exists from this point on), then views.
    -- Generated here rather than in add_compression_policy so the
    -- return type binds the heap OID (see _unpack_ddl).
    EXECUTE pgseries._unpack_ddl(
        p_schema, p_table,
        (SELECT codecs FROM pgseries.compression_policy
         WHERE series_schema = p_schema AND series_table = p_table),
        (SELECT segmentby FROM pgseries.compression_policy
         WHERE series_schema = p_schema AND series_table = p_table));
    -- Views first: the INSTEAD OF triggers require the base view.
    FOR v_ddl IN SELECT * FROM pgseries._read_ddl(p_schema, p_table) LOOP
        EXECUTE v_ddl;
    END LOOP;
    FOR v_ddl IN SELECT * FROM pgseries._write_trg_ddl(p_schema, p_table) LOOP
        EXECUTE v_ddl;
    END LOOP;
    EXECUTE format('GRANT SELECT ON %I.%I TO pgseries_reader',
                   p_schema, p_table);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %I.%I '
                   || 'TO pgseries_writer', p_schema, p_table);
END;
$$;

-- Undo _make_transparent: drop the view (its INSTEAD OF triggers go
-- with it), drop the generated functions/alias/range path, rename the
-- heap back. The table is a plain partitioned table again.
CREATE OR REPLACE FUNCTION pgseries._make_opaque(
    p_schema NAME, p_table NAME)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s      pgseries.series_table%ROWTYPE;
    v_heap NAME := pgseries._heap_name(p_table);
    v_slug TEXT := pgseries._slug(p_schema, p_table);
BEGIN
    s := pgseries._series(p_schema, p_table);
    IF NOT pgseries._is_transparent(p_schema, p_table) THEN
        RETURN;
    END IF;
    EXECUTE 'SET LOCAL pgseries.suppress_drop_trigger = ''on''';
    EXECUTE format('DROP VIEW IF EXISTS %I.%I', p_schema, p_table || '_read');
    EXECUTE format('DROP FUNCTION IF EXISTS %I.%I(%s, %s)',
                   p_schema, p_table || '_read_range',
                   s.time_col_type, s.time_col_type);
    EXECUTE format('DROP VIEW IF EXISTS %I.%I', p_schema, p_table);
    EXECUTE format('DROP FUNCTION IF EXISTS %s_ins()', 'pgseries.write_' || v_slug);
    EXECUTE format('DROP FUNCTION IF EXISTS %s_upd()', 'pgseries.write_' || v_slug);
    EXECUTE format('DROP FUNCTION IF EXISTS %s_del()', 'pgseries.write_' || v_slug);
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',
                   p_schema, v_heap, p_table);
END;
$$;

-- Resolve 'auto' int codecs by measurement and dict-on-high-cardinality
-- to raw, on bounded samples. Pure function of (table, policy draft):
-- returns the stored resolved codec map. Extracted from
-- add_compression_policy; behavior identical.
CREATE OR REPLACE FUNCTION pgseries._resolve_codecs(
    p_schema NAME, p_table NAME,
    p_timecol NAME, p_timecol_type TEXT,
    p_segmentby NAME[], p_orderby NAME, p_orderby_desc BOOLEAN,
    p_segment_rows INTEGER, p_codecs JSONB, p_requant JSONB)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_nrows BIGINT;
    v_base  JSONB;
    v_map_f JSONB;
    v_map_d JSONB;
    v_bf    BIGINT;
    v_bd    BIGINT;
    v_ddl   TEXT;
    v_dcols TEXT;
    v_nd    BIGINT;
    v_out   JSONB;
    r       RECORD;
    r2      RECORD;
BEGIN
    v_out := COALESCE(p_codecs, '{}');
        EXECUTE format('SELECT count(*) FROM %I.%I', p_schema, p_table)
        INTO v_nrows;
        IF v_nrows = 0 THEN
            -- Empty table: nothing to measure; for everywhere.
            FOR r IN SELECT * FROM pgseries._colinfo(
                         p_schema, p_table, p_timecol,
                         COALESCE(p_segmentby, '{}'))
                     WHERE kind = 'data' LOOP
                IF pgseries._eff_codec(v_out,
                                       r.attname::TEXT, r.class) = 'auto' THEN
                    RAISE NOTICE 'pgseries: empty table, auto codec for % -> for',
                        r.attname;
                    v_out := COALESCE(v_out, '{}')
                                || jsonb_build_object(r.attname::TEXT, 'for');
                END IF;
            END LOOP;
        ELSE
            -- One shared sample. NOTE: built from SELECT *, so generated
            -- columns materialize as plain data here; the bout maps are
            -- built from the SAMPLE's columns, not the parent's.
            EXECUTE format('CREATE TEMP TABLE auto_src AS SELECT * FROM %I.%I '
                           || 'ORDER BY %I %s LIMIT 20000',
                p_schema, p_table, p_orderby,
                CASE WHEN p_orderby_desc THEN 'DESC' ELSE 'ASC' END);
            FOR r IN SELECT * FROM pgseries._colinfo(
                         p_schema, p_table, p_timecol,
                         COALESCE(p_segmentby, '{}'))
                     WHERE kind = 'data' LOOP
                IF pgseries._eff_codec(v_out,
                                       r.attname::TEXT, r.class) <> 'auto' THEN
                    CONTINUE;
                END IF;
                -- Base map over SAMPLE columns: parent-explicit entries
                -- win, anything else (incl. materialized generated cols
                -- and other auto cols) falls back to for.
                v_base := '{}';
                FOR r2 IN SELECT * FROM pgseries._colinfo(
                             'pg_temp', 'auto_src', p_timecol,
                             COALESCE(p_segmentby, '{}'))
                         WHERE kind = 'data' LOOP
                    v_base := v_base || jsonb_build_object(r2.attname::TEXT,
                        CASE WHEN v_out ->> r2.attname::TEXT
                                  NOT IN ('auto', NULL)
                             THEN v_out ->> r2.attname::TEXT
                             WHEN pgseries._default_codec(r2.class) = 'auto'
                             THEN 'for'
                             ELSE pgseries._default_codec(r2.class) END);
                END LOOP;
                -- for-vs-delta bout on the sample; keep smaller bytes.
                v_map_f := v_base || jsonb_build_object(r.attname::TEXT, 'for');
                v_map_d := v_base || jsonb_build_object(r.attname::TEXT, 'delta');
                FOR v_ddl IN SELECT * FROM pgseries._compressed_ddl(
                                 'pg_temp', 'auto_src', 'pg_temp.auto_f',
                                 v_map_f, COALESCE(p_segmentby, '{}'), false) LOOP
                    EXECUTE v_ddl;
                END LOOP;
                PERFORM pgseries._pack_segments(
                    'pg_temp', 'auto_src', 'pg_temp', 'auto_f',
                    p_timecol, p_timecol_type, COALESCE(p_segmentby, '{}'),
                    p_timecol, false, LEAST(p_segment_rows, 1000),
                    v_map_f, COALESCE(p_requant, '{}'));
                SELECT pg_total_relation_size('pg_temp.auto_f') INTO v_bf;
                FOR v_ddl IN SELECT * FROM pgseries._compressed_ddl(
                                 'pg_temp', 'auto_src', 'pg_temp.auto_d',
                                 v_map_d, COALESCE(p_segmentby, '{}'), false) LOOP
                    EXECUTE v_ddl;
                END LOOP;
                PERFORM pgseries._pack_segments(
                    'pg_temp', 'auto_src', 'pg_temp', 'auto_d',
                    p_timecol, p_timecol_type, COALESCE(p_segmentby, '{}'),
                    p_timecol, false, LEAST(p_segment_rows, 1000),
                    v_map_d, COALESCE(p_requant, '{}'));
                SELECT pg_total_relation_size('pg_temp.auto_d') INTO v_bd;
                IF v_bd < v_bf THEN
                    RAISE NOTICE 'pgseries: auto codec for % -> delta (% vs % bytes)',
                        r.attname, v_bd, v_bf;
                    v_out := COALESCE(v_out, '{}')
                                || jsonb_build_object(r.attname::TEXT, 'delta');
                ELSE
                    RAISE NOTICE 'pgseries: auto codec for % -> for (% vs % bytes)',
                        r.attname, v_bf, v_bd;
                    v_out := COALESCE(v_out, '{}')
                                || jsonb_build_object(r.attname::TEXT, 'for');
                END IF;
                DROP TABLE pg_temp.auto_f;
                DROP TABLE pg_temp.auto_d;
            END LOOP;
            DROP TABLE pg_temp.auto_src;
        END IF;

        -- dict on high-cardinality text is pure loss: the per-segment
        -- dictionary dwarfs the data (measured: 340 KB of uuid dict per
        -- 1000-row segment for zero gain) and pack's
        -- array_position(dict, x) is O(n*d) per segment. Resolve to raw
        -- when the sample distinct count exceeds one segment's worth of
        -- rows; stored resolved like auto.
        SELECT string_agg(quote_ident(c.attname), ', ')
        INTO v_dcols
        FROM pgseries._colinfo(p_schema, p_table, p_timecol,
                               COALESCE(p_segmentby, '{}')) c
        WHERE c.kind = 'data' AND c.class = 'text'
          AND pgseries._eff_codec(v_out,
                                  c.attname::TEXT, c.class) = 'dict';
        IF v_dcols IS NOT NULL THEN
            -- Bounded recent-rows sample (heap leaves + unpacked
            -- compressed segs); distinct counts only over dict cols.
            EXECUTE 'CREATE TEMP TABLE dict_src AS SELECT '
                    || v_dcols || ' FROM ('
                    || pgseries._sample_sql(p_schema, p_table, 20000)
                    || ') s LIMIT 20000';
            FOR r IN SELECT * FROM pgseries._colinfo(
                         p_schema, p_table, p_timecol,
                         COALESCE(p_segmentby, '{}'))
                     WHERE kind = 'data' AND class = 'text' LOOP
                IF pgseries._eff_codec(v_out,
                                       r.attname::TEXT, r.class) <> 'dict' THEN
                    CONTINUE;
                END IF;
                EXECUTE format('SELECT count(DISTINCT %I) FROM pg_temp.dict_src',
                               r.attname) INTO v_nd;
                IF v_nd > p_segment_rows THEN
                    RAISE NOTICE 'pgseries: dict on high-cardinality % (% distinct in sample) -> raw',
                        r.attname, v_nd;
                    v_out := COALESCE(v_out, '{}')
                                || jsonb_build_object(r.attname::TEXT, 'raw');
                END IF;
            END LOOP;
            DROP TABLE pg_temp.dict_src;
        END IF;
    RETURN v_out;
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
    v_seg    NAME;
    v_n      BIGINT;
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

    -- Resolve 'auto' int codecs by measurement, not proxy: mini-bench
    -- the column as for vs delta on a 20K-row sample and keep the
    -- smaller bytes. Stored resolved so pack/unpack never see 'auto'.
    SELECT pgseries._resolve_codecs(
        p_schema, p_table, s.time_column, s.time_col_type,
        COALESCE(p_segmentby, '{}'), v_ob, p_orderby_desc,
        p_segment_rows, COALESCE(p_codecs, '{}'),
        COALESCE(p_requant, '{}'))
    INTO p_codecs;


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

    -- The swap renames the heap aside and generates the unpack
    -- (heap-bound return type), transparent base view + DML triggers.
    PERFORM pgseries._make_transparent(p_schema, p_table);

    -- Cardinality trap (spec): segmentby near segments-per-chunk
    -- collapses the ratio. Estimated on a bounded sample (a full
    -- DISTINCT sort at policy time is O(table) on every big ingest).
    IF p_segmentby IS NOT NULL AND array_length(p_segmentby, 1) IS NOT NULL THEN
        EXECUTE 'SELECT count(*) FROM (SELECT DISTINCT '
                || (SELECT string_agg(quote_ident(x), ', ')
                    FROM unnest(p_segmentby) AS x)
                || ' FROM ('
                || pgseries._sample_sql(p_schema, p_table, 20000)
                || ') s) t'
        INTO v_n;
        IF v_n > 10000 THEN
            RAISE WARNING 'pgseries: ~% distinct segmentby values in sample — keep below ~10^4 per chunk for the 2-4x target',
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
    EXECUTE 'SET LOCAL pgseries.suppress_drop_trigger = ''on''';
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
    PERFORM pgseries._make_opaque(p_schema, p_table);
    EXECUTE format('DROP FUNCTION IF EXISTS pgseries.unpack_%s', v_slug);
    EXECUTE format('DROP TABLE IF EXISTS %I.%I',
                   p_schema, p_table || '_compressed');
    DELETE FROM pgseries.compression_policy
    WHERE series_schema = p_schema AND series_table = p_table;
END;
$$;

-- Idempotent full unregister: drops every pgseries-owned object by
-- name (IF EXISTS) and deletes all catalog rows. The DROP TABLE/ VIEW
-- event trigger calls this when the user drops a series table by
-- hand; drop_series_table() shares it via the same suppression flag.
-- Safe to run twice (second pass is all no-ops).
CREATE OR REPLACE FUNCTION pgseries._unregister_series(
    p_schema NAME, p_table NAME)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_heap NAME := pgseries._heap_name(p_table);
    v_slug TEXT := pgseries._slug(p_schema, p_table);
    r      RECORD;
    s      pgseries.series_table%ROWTYPE;
BEGIN
    EXECUTE 'SET LOCAL pgseries.suppress_drop_trigger = ''on''';
    SELECT * INTO s FROM pgseries.series_table
    WHERE series_schema = p_schema AND series_table = p_table;
    -- Views first (INSTEAD OF triggers die with the base view).
    EXECUTE format('DROP VIEW IF EXISTS %I.%I', p_schema, p_table);
    EXECUTE format('DROP VIEW IF EXISTS %I.%I', p_schema, p_table || '_read');
    IF FOUND THEN
        EXECUTE format('DROP FUNCTION IF EXISTS %I.%I(%s, %s)',
                       p_schema, p_table || '_read_range',
                       s.time_col_type, s.time_col_type);
    END IF;
    -- Heap (chunks + DEFAULT go with it) and compressed tree.
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', p_schema, v_heap);
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                   p_schema, p_table || '_compressed');
    -- Generated functions (any signature — types may be gone already).
    FOR r IN SELECT format('DROP FUNCTION IF EXISTS %I.%I(%s)',
                           n.nspname, p.proname,
                           pg_get_function_identity_arguments(p.oid)) AS stmt
             FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname IN ('pgseries', p_schema::TEXT)
               AND p.proname IN ('unpack_' || v_slug,
                                 'write_' || v_slug || '_ins',
                                 'write_' || v_slug || '_upd',
                                 'write_' || v_slug || '_del',
                                 p_table::TEXT || '_read_range') LOOP
        EXECUTE r.stmt;
    END LOOP;
    DELETE FROM pgseries.compression_policy
    WHERE series_schema = p_schema AND series_table = p_table;
    DELETE FROM pgseries.chunk
    WHERE series_schema = p_schema AND series_table = p_table;
    DELETE FROM pgseries.dimension
    WHERE series_schema = p_schema AND series_table = p_table;
    DELETE FROM pgseries.job
    WHERE target_schema = p_schema AND target_table = p_table;
    DELETE FROM pgseries.series_table
    WHERE series_schema = p_schema AND series_table = p_table;
END;
$$;

-- sql_drop hook: plain DROP TABLE/VIEW on a series table (or
-- its heap) unregisters everything. Internal DROPs set
-- pgseries.suppress_drop_trigger (SET LOCAL) first.
CREATE OR REPLACE FUNCTION pgseries._on_series_drop()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    r RECORD;
    s RECORD;
BEGIN
    IF current_setting('pgseries.suppress_drop_trigger', true) = 'on' THEN
        RETURN;
    END IF;
    -- sql_drop (not ddl_command_end): only dropped_objects() lists
    -- DROPs — ddl_commands() never does. Same-transaction atomicity:
    -- if the user's DROP fails, this unregister rolls back with it.
    FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
        IF r.object_type NOT IN ('table', 'view') THEN
            CONTINUE;
        END IF;
        SELECT * INTO s FROM pgseries.series_table
        WHERE series_schema::TEXT = r.schema_name
          AND (series_table::TEXT = r.object_name
               OR pgseries._heap_name(series_table)::TEXT = r.object_name);
        IF FOUND THEN
            PERFORM pgseries._unregister_series(
                s.series_schema, s.series_table);
        END IF;
    END LOOP;
END;
$$;

-- Remove a series table from management. With p_drop_data (default)
-- the heap, chunks, DEFAULT partition, compressed tree and views go
-- away; with false the table stays as a plain partitioned table
-- (reverse of adopt_partitioned_table) and only the catalog, jobs
-- and policy are removed. Compressed chunks are decompressed first
-- so no rows are ever lost silently.
CREATE OR REPLACE FUNCTION pgseries.drop_series_table(
    p_schema NAME, p_table NAME, p_drop_data BOOLEAN DEFAULT true)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    r       RECORD;
    v_n     BIGINT;
BEGIN
    PERFORM pgseries._series(p_schema, p_table);
    PERFORM pgseries._table_lock(p_schema, p_table);
    EXECUTE 'SET LOCAL pgseries.suppress_drop_trigger = ''on''';

    FOR r IN SELECT c.chunk_name
             FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
             WHERE c.series_schema = p_schema AND c.series_table = p_table
               AND l.state IN ('compressing', 'compressed')
             ORDER BY c.range_lower NULLS LAST LOOP
        PERFORM pgseries.decompress_chunk(p_schema, p_table, r.chunk_name);
    END LOOP;

    IF EXISTS (SELECT 1 FROM pgseries.compression_policy
               WHERE series_schema = p_schema AND series_table = p_table) THEN
        PERFORM pgseries.drop_compression_policy(p_schema, p_table);
    END IF;

    IF p_drop_data THEN
        EXECUTE format('DROP TABLE %I.%I', p_schema, p_table);
        -- Chunk catalog goes with the data (leases cascade).
        DELETE FROM pgseries.chunk
        WHERE series_schema = p_schema AND series_table = p_table;
        GET DIAGNOSTICS v_n = ROW_COUNT;
    ELSE
        SELECT count(*) INTO v_n FROM pgseries.chunk
        WHERE series_schema = p_schema AND series_table = p_table;
        DELETE FROM pgseries.chunk
        WHERE series_schema = p_schema AND series_table = p_table;
    END IF;
    DELETE FROM pgseries.dimension
    WHERE series_schema = p_schema AND series_table = p_table;
    DELETE FROM pgseries.job
    WHERE target_schema = p_schema AND target_table = p_table;
    DELETE FROM pgseries.series_table
    WHERE series_schema = p_schema AND series_table = p_table;
    IF p_drop_data THEN
        RETURN format('dropped series table %I.%I (%s chunks)',
                      p_schema, p_table, v_n);
    ELSE
        RETURN format('unregistered series table %I.%I, data kept (%s chunks detached from management)',
                      p_schema, p_table, v_n);
    END IF;
END;
$$;

-- Transparent read path for a compressed table: the UNION view under
-- the BASE name (the spec's `create view <table>`), a
-- backwards-compatible <table>_read alias, and a range fast path whose
-- seg-bound quals hit BRIN before the LATERAL unnest fires (the
-- spec's load-bearing plan shape). Heap is addressed explicitly: bulk
-- readers that want heap-only (movers, benchmarks) bypass the view.
-- Rebuild the read views/legs in place (idempotent). Called after
-- every operation that changes the sibling set: compress,
-- decompress, retention drops, add_dimension.
CREATE OR REPLACE FUNCTION pgseries.refresh_read_view(
    p_schema NAME, p_table NAME)
RETURNS VOID
LANGUAGE plpgsql VOLATILE
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_ddl TEXT;
BEGIN
    PERFORM pgseries._series(p_schema, p_table);
    -- No transparent view installed (pre-policy tables): nothing to do.
    IF NOT pgseries._is_transparent(p_schema, p_table) THEN
        RETURN;
    END IF;
    FOR v_ddl IN SELECT * FROM pgseries._read_ddl(p_schema, p_table) LOOP
        EXECUTE v_ddl;
    END LOOP;
END;
$$;

-- Leg fragments for one compressed node: time bounds always; when
-- the policy has a single segmentby column with few distinct values
-- (exact DISTINCT over the node's own segments — tiny scan, no stats
-- staleness, no uniform-distribution hole), one leg per value plus a
-- catch-all for the rest. Equality on u.seg lets ranged AND un-ranged
-- segby queries prune per value at plan time (same
-- constraint-exclusion machinery as the time legs). Multi-col
-- segmentby or >33 distinct keeps the plain time leg.
CREATE OR REPLACE FUNCTION pgseries._node_legs(
    p_schema NAME, p_table NAME, p_node NAME, p_cschema NAME,
    p_lo TIMESTAMPTZ, p_hi TIMESTAMPTZ)
RETURNS TEXT
LANGUAGE plpgsql STABLE
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    pol      pgseries.compression_policy%ROWTYPE;
    s        pgseries.series_table%ROWTYPE;
    v_unpack TEXT := 'pgseries.unpack_' || pgseries._slug(p_schema, p_table);
    v_cpart  TEXT := format('%I.%I', p_cschema,
                            (p_node::TEXT || '_compressed')::NAME);
    v_seg    NAME;
    v_segt   TEXT;
    v_vals   TEXT[];
    v_out    TEXT := '';
    v        TEXT;
BEGIN
    SELECT * INTO pol FROM pgseries.compression_policy
    WHERE series_schema = p_schema AND series_table = p_table;
    IF NOT FOUND THEN
        RETURN '';
    END IF;
    SELECT * INTO s FROM pgseries.series_table
    WHERE series_schema = p_schema AND series_table = p_table;
    IF COALESCE(array_length(pol.segmentby, 1), 0) <> 1 THEN
        RETURN format(' UNION ALL SELECT u.* FROM %s AS c, LATERAL %s(c) AS u '
                      || 'WHERE u.%I >= %L::%s AND u.%I < %L::%s',
                      v_cpart, v_unpack,
                      s.time_column, p_lo::TEXT, s.time_col_type,
                      s.time_column, p_hi::TEXT, s.time_col_type);
    END IF;
    v_seg := pol.segmentby[1];
    SELECT format_type(a.atttypid, a.atttypmod) INTO v_segt
    FROM pg_attribute a
    WHERE a.attrelid = to_regclass(
              pgseries._write_parent(p_schema, p_table))
      AND a.attname = v_seg AND a.attnum > 0 AND NOT a.attisdropped;
    EXECUTE format('SELECT array_agg(d::TEXT) FROM '
                   || '(SELECT DISTINCT seg.%I AS d FROM %s AS seg '
                   || 'WHERE seg.%I IS NOT NULL LIMIT 34) t',
                   v_seg, v_cpart, v_seg)
    INTO v_vals;
    -- 34 hit = 33+ distinct: keep the plain leg (conservative).
    IF v_vals IS NULL OR array_length(v_vals, 1) IS NULL
       OR array_length(v_vals, 1) >= 34 THEN
        RETURN format(' UNION ALL SELECT u.* FROM %s AS c, LATERAL %s(c) AS u '
                      || 'WHERE u.%I >= %L::%s AND u.%I < %L::%s',
                      v_cpart, v_unpack,
                      s.time_column, p_lo::TEXT, s.time_col_type,
                      s.time_column, p_hi::TEXT, s.time_col_type);
    END IF;
    FOREACH v IN ARRAY v_vals LOOP
        v_out := v_out || format(
            ' UNION ALL SELECT u.* FROM %s AS c, LATERAL %s(c) AS u '
            || 'WHERE u.%I >= %L::%s AND u.%I < %L::%s AND u.%I = %L::%s',
            v_cpart, v_unpack,
            s.time_column, p_lo::TEXT, s.time_col_type,
            s.time_column, p_hi::TEXT, s.time_col_type,
            v_seg, v, v_segt);
    END LOOP;
    v_out := v_out || format(
        ' UNION ALL SELECT u.* FROM %s AS c, LATERAL %s(c) AS u '
        || 'WHERE u.%I >= %L::%s AND u.%I < %L::%s '
        || 'AND (u.%I <> ALL (%L::%s[]) OR u.%I IS NULL)',
        v_cpart, v_unpack,
        s.time_column, p_lo::TEXT, s.time_col_type,
        s.time_column, p_hi::TEXT, s.time_col_type,
        v_seg, v_vals, v_segt, v_seg);
    RETURN v_out;
END;
$$;

CREATE OR REPLACE FUNCTION pgseries._read_ddl(
    p_schema NAME, p_table NAME)
RETURNS SETOF TEXT
LANGUAGE plpgsql STABLE
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s       pgseries.series_table%ROWTYPE;
    v_heap  TEXT := format('%I.%I', p_schema, pgseries._heap_name(p_table));
    v_tpl   TEXT := format('%I.%I', p_schema, p_table || '_compressed');
    v_unpack TEXT := 'pgseries.unpack_' || pgseries._slug(p_schema, p_table);
    v_view  TEXT := format('%I.%I', p_schema, p_table || '_read');
    v_range TEXT := format('%I.%I', p_schema, p_table || '_read_range');
    v_legs  TEXT := '';
    v_orpha TEXT := '';
    r       RECORD;
BEGIN
    s := pgseries._series(p_schema, p_table);
    -- Per-sibling legs via _node_legs (time bounds always, plus
    -- per-value legs for narrow single-column segmentby).
    v_legs := '';
    FOR r IN
        SELECT DISTINCT COALESCE(ch.parent_chunk, ch.chunk_name) AS node,
                        ch.chunk_schema AS cs,
                        min(ch.range_lower) AS lo, max(ch.range_upper) AS hi
        FROM pgseries.chunk ch
        JOIN pgseries.chunk_lease lh USING (chunk_id)
        WHERE ch.series_schema = p_schema
          AND ch.series_table = p_table
          AND lh.state IN ('compressing', 'compressed')
          AND to_regclass(format('%I.%I', ch.chunk_schema,
                   (COALESCE(ch.parent_chunk, ch.chunk_name)::TEXT
                    || '_compressed')::NAME)) IS NOT NULL
        GROUP BY 1, 2
        HAVING min(ch.range_lower) IS NOT NULL
    LOOP
        v_legs := v_legs || pgseries._node_legs(
            p_schema, p_table, r.node, r.cs, r.lo, r.hi);
    END LOOP;
    -- Siblings without catalog bounds: unprunable fallback legs
    -- (always correct, never skipped).
    SELECT COALESCE(string_agg(
               format(' UNION ALL SELECT u.* FROM %I.%I AS c, '
                      || 'LATERAL %s(c) AS u',
                      n.nspname, co.relname, v_unpack), ' '), '')
    INTO v_orpha
    FROM pg_class co
    JOIN pg_namespace n ON n.oid = co.relnamespace
    JOIN pg_inherits h ON h.inhrelid = co.oid
    WHERE h.inhparent = to_regclass(v_tpl)
      AND NOT EXISTS (
          SELECT 1 FROM pgseries.chunk cc
          WHERE cc.series_schema = p_schema
            AND cc.series_table = p_table
            AND (COALESCE(cc.parent_chunk, cc.chunk_name)::TEXT
                 || '_compressed') = co.relname
            AND cc.chunk_schema = n.nspname
            AND cc.range_lower IS NOT NULL);
    RETURN NEXT
        format('CREATE OR REPLACE VIEW %I.%I AS SELECT * FROM %s%s%s',
               p_schema, p_table, v_heap, v_legs, v_orpha);
    RETURN NEXT
        format('CREATE OR REPLACE VIEW %s AS SELECT * FROM %I.%I',
               v_view, p_schema, p_table);
    RETURN NEXT
        format('CREATE OR REPLACE FUNCTION %s(a %s, b %s) '
               || 'RETURNS SETOF %I.%I LANGUAGE sql STABLE PARALLEL RESTRICTED '
               || 'SET search_path = pgseries, pg_catalog AS %s '
               || 'SELECT * FROM %s WHERE %I >= $1 AND %I <= $2 '
               || 'UNION ALL '
               || 'SELECT u.* FROM (SELECT c.* FROM %s AS c '
               || 'WHERE c.seg_max_ts >= $1::TIMESTAMPTZ '
               || 'AND c.seg_min_ts <= $2::TIMESTAMPTZ) AS f, '
               || 'LATERAL %s(f) AS u WHERE u.%I >= $1 AND u.%I <= $2%s',
               v_range, s.time_col_type, s.time_col_type,
               p_schema, p_table,
               '$pgseries_read$',
               v_heap, s.time_column, s.time_column,
               v_tpl, v_unpack, s.time_column, s.time_column, '$pgseries_read$');
END;
$$;

-- Pack one chunk's heap rows as new segments. Uniform for first
-- compression and recompression (late-write staging merges as fresh
-- segments). Returns a human summary.
DROP FUNCTION IF EXISTS pgseries.compress_chunk(NAME, NAME, NAME);
CREATE OR REPLACE FUNCTION pgseries.compress_chunk(
    p_schema NAME, p_table NAME, p_chunk NAME,
    p_min_rows INTEGER DEFAULT NULL)
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
    v_node   NAME;
    v_space  INTEGER;
    v_cpart  NAME;
    v_tpl    TEXT := format('%I.%I', p_schema, p_table || '_compressed');
    v_heap   BIGINT;
    v_segs   BIGINT;
    v_idx    RECORD;
    v_min    INTEGER;
BEGIN
    s := pgseries._series(p_schema, p_table);
    SELECT * INTO pol FROM pgseries.compression_policy
    WHERE series_schema = p_schema AND series_table = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: %.% has no compression policy',
            p_schema, p_table
            USING ERRCODE = 'undefined_object';
    END IF;
    SELECT l.state, c.range_lower, c.range_upper,
           COALESCE(c.parent_chunk, c.chunk_name), c.space_index
    INTO v_state, v_lo, v_hi, v_node, v_space
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
    -- One compressed partition per time-chunk node, shared by its
    -- space leaves (seg_space tells slices apart); single-dimension
    -- tables: node == chunk, identical behavior to before.
    v_cpart := (v_node::TEXT || '_compressed')::NAME;
    -- Cold AND big enough: below one segment's worth of rows the
    -- per-segment overhead dominates (measured: a 500-row chunk grew
    -- 64 kB -> 136 kB). Skipped chunks stay ready for a later tick.
    v_min := COALESCE(p_min_rows, pol.segment_rows);
    IF v_heap > 0 AND v_heap < v_min THEN
        RAISE NOTICE 'pgseries: chunk %.% has % rows (< % minimum), skipping compression',
            p_schema, p_chunk, v_heap, v_min;
        RETURN format('too small (%s rows < %s minimum), skipped', v_heap, v_min);
    END IF;
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
        pol.orderby_desc, pol.segment_rows, pol.codecs, pol.requant,
        '', NULL, v_space)
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

    PERFORM pgseries.refresh_read_view(p_schema, p_table);

    RETURN format('compressed %s rows into %s segments (%I)',
                  v_heap, v_segs, v_cpart);
END;
$$;

-- Set-based multi-chunk compression: packs every eligible ready chunk
-- in ONE INSERT..SELECT over the heap parent (partition pruning limits
-- the scan; the window resets per chunk bucket), then truncates the
-- heaps and flips the leases. Cold chunks only by default
-- (p_include_hot opts the live chunk in); p_older_than further
-- restricts to chunks below a cutoff. Empty heaps are skipped via a
-- single UNION ALL count probe — pre-created future chunks must never
-- flip to compressed. Chunks below p_min_rows (default: one segment's
-- worth) are skipped: cold AND big enough, or not at all.
-- Returns a human summary.
DROP FUNCTION IF EXISTS pgseries.compress_table(NAME, NAME, INTERVAL, BOOLEAN);
CREATE OR REPLACE FUNCTION pgseries.compress_table(
    p_schema NAME, p_table NAME,
    p_older_than INTERVAL DEFAULT NULL,
    p_include_hot BOOLEAN DEFAULT false,
    p_min_rows INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s        pgseries.series_table%ROWTYPE;
    pol      pgseries.compression_policy%ROWTYPE;
    v_heap   NAME := pgseries._heap_name(p_table);
    v_tpl    TEXT := format('%I.%I', p_schema, p_table || '_compressed');
    v_cutoff TIMESTAMPTZ;
    v_nchunk INTEGER;
    v_n      BIGINT;
    v_nrows  BIGINT;
    v_nsegs  BIGINT;
    v_min    TIMESTAMPTZ;
    v_max    TIMESTAMPTZ;
    v_bucket TEXT;
    v_where  TEXT;
    v_sql    TEXT;
    v_cpart  NAME;
    v_minrows INTEGER;
    v_skipped INTEGER;
    v_hasdim BOOLEAN;
    r        RECORD;
BEGIN
    s := pgseries._series(p_schema, p_table);
    SELECT * INTO pol FROM pgseries.compression_policy
    WHERE series_schema = p_schema AND series_table = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: %.% has no compression policy',
            p_schema, p_table
            USING ERRCODE = 'undefined_object';
    END IF;
    IF NOT pgseries._is_transparent(p_schema, p_table) THEN
        RAISE EXCEPTION 'pgseries: %.% is not transparent (add_compression_policy bug)',
            p_schema, p_table
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    PERFORM pgseries._table_lock(p_schema, p_table);
    v_cutoff := CASE WHEN p_older_than IS NULL THEN NULL
                     ELSE now() - p_older_than END;

    IF to_regclass('pg_temp.pgseries_compress_list') IS NULL THEN
        CREATE TEMP TABLE pgseries_compress_list (
            cn NAME PRIMARY KEY,
            lo TIMESTAMPTZ NOT NULL, hi TIMESTAMPTZ NOT NULL,
            sp INTEGER NOT NULL DEFAULT 0,
            node NAME NOT NULL,
            heap_n BIGINT NOT NULL DEFAULT -1) ON COMMIT DROP;
    END IF;
    DELETE FROM pgseries_compress_list;
    INSERT INTO pgseries_compress_list (cn, lo, hi, sp, node)
    SELECT c.chunk_name, c.range_lower, c.range_upper,
           c.space_index, COALESCE(c.parent_chunk, c.chunk_name)
    FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
    WHERE c.series_schema = p_schema AND c.series_table = p_table
      AND l.state = 'ready'
      AND c.range_lower IS NOT NULL
      AND (v_cutoff IS NULL OR c.range_upper < v_cutoff)
      AND (p_include_hot OR c.range_upper < now());

    -- Single set-based emptiness probe (one UNION ALL, one EXECUTE).
    SELECT string_agg(
               format('SELECT %L::NAME AS cn, count(*) AS n FROM %I.%I',
                      cn, p_schema, cn), ' UNION ALL ')
    INTO v_sql FROM pgseries_compress_list;
    IF v_sql IS NOT NULL THEN
        EXECUTE 'WITH h AS (' || v_sql || ') '
            || 'UPDATE pgseries_compress_list l SET heap_n = h.n '
            || 'FROM h WHERE h.cn = l.cn';
    END IF;
    DELETE FROM pgseries_compress_list WHERE heap_n <= 0;
    v_minrows := COALESCE(p_min_rows, pol.segment_rows);
    SELECT count(*) INTO v_skipped FROM pgseries_compress_list
    WHERE heap_n < v_minrows;
    DELETE FROM pgseries_compress_list WHERE heap_n < v_minrows;
    IF v_skipped > 0 THEN
        RAISE NOTICE 'pgseries: skipping % tiny chunks (< % rows)',
            v_skipped, v_minrows;
    END IF;
    SELECT count(*) INTO v_nchunk FROM pgseries_compress_list;
    IF v_nchunk = 0 THEN
        RETURN 'nothing eligible to compress';
    END IF;

    -- Guarded set flip of the whole batch (no per-chunk transition
    -- calls; concurrent modification aborts instead of half-packing).
    UPDATE pgseries.chunk_lease l SET state = 'compressing', updated_at = now()
    FROM pgseries.chunk c JOIN pgseries_compress_list t ON t.cn = c.chunk_name
    WHERE l.chunk_id = c.chunk_id AND l.state = 'ready'
      AND c.series_schema = p_schema AND c.series_table = p_table;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> v_nchunk THEN
        RAISE EXCEPTION 'pgseries: %.% lost a chunk race (want %, flipped %)',
            p_schema, p_table, v_nchunk, v_n
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Compressed partitions up front, one per time-chunk node shared
    -- by its space leaves (DDL per node; names only).
    FOR r IN SELECT DISTINCT node, min(lo) AS lo, max(hi) AS hi
             FROM pgseries_compress_list GROUP BY node ORDER BY 2 LOOP
        v_cpart := (r.node::TEXT || '_compressed')::NAME;
        IF to_regclass(format('%I.%I', p_schema, v_cpart)) IS NULL THEN
            EXECUTE format('CREATE TABLE %I.%I PARTITION OF %s '
                           || 'FOR VALUES FROM (%L::TIMESTAMPTZ) TO (%L::TIMESTAMPTZ)',
                p_schema, v_cpart, v_tpl, r.lo::TEXT, r.hi::TEXT);
            EXECUTE format('REVOKE ALL ON %I.%I FROM PUBLIC, pgseries_reader, pgseries_writer',
                p_schema, v_cpart);
            EXECUTE format('CREATE TRIGGER %I BEFORE TRUNCATE ON %I.%I '
                           || 'FOR EACH STATEMENT EXECUTE FUNCTION '
                           || 'pgseries._compressed_no_truncate()',
                           v_cpart::TEXT || '_no_trunc', p_schema, v_cpart);
        END IF;
    END LOOP;

    -- The bucket expression must reproduce partition bounds exactly
    -- (same origin/interval math as precreate/move).
    IF s.time_col_type = 'date' THEN
        v_bucket := format('pgseries._date_bucket(%I, %L::DATE, %s)::TIMESTAMPTZ',
            s.time_column, s.origin::DATE,
            pgseries._interval_days(s.chunk_interval));
    ELSE
        v_bucket := format('date_bin(%L::INTERVAL, %I, %L::TIMESTAMPTZ)',
            s.chunk_interval, s.time_column, s.origin);
    END IF;
    SELECT min(lo), max(hi) INTO v_min, v_max FROM pgseries_compress_list;
    v_where := format('%I >= %L::TIMESTAMPTZ AND %I < %L::TIMESTAMPTZ AND (%s) IN (SELECT lo FROM pgseries_compress_list)',
        s.time_column, v_min, s.time_column, v_max, v_bucket);

    EXECUTE 'SET LOCAL pgseries.allow_compressed_write = ''on''';
    SELECT EXISTS (SELECT 1 FROM pgseries.dimension
                   WHERE series_schema = p_schema
                     AND series_table = p_table
                     AND dim_type = 'space_hash')
    INTO v_hasdim;
    IF NOT v_hasdim THEN
        SELECT pgseries._pack_segments(p_schema, v_heap, p_schema,
            (p_table || '_compressed')::NAME,
            s.time_column, s.time_col_type, pol.segmentby, pol.orderby,
            pol.orderby_desc, pol.segment_rows, pol.codecs, pol.requant,
            v_where, v_bucket, 0)
        INTO v_nsegs;
    ELSE
        -- One set-based pack per leaf straight into its node's shared
        -- sibling (space slices never mix: seg_space stays exact).
        v_nsegs := 0;
        FOR r IN SELECT * FROM pgseries_compress_list ORDER BY lo, sp LOOP
            v_cpart := (r.node::TEXT || '_compressed')::NAME;
            SELECT pgseries._pack_segments(p_schema, r.cn, p_schema, v_cpart,
                s.time_column, s.time_col_type, pol.segmentby, pol.orderby,
                pol.orderby_desc, pol.segment_rows, pol.codecs, pol.requant,
                '', NULL, r.sp)
            INTO v_n;
            v_nsegs := v_nsegs + v_n;
        END LOOP;
    END IF;

    -- Heaps drained (one TRUNCATE per chunk; DDL, no scans), leases out.
    FOR r IN SELECT * FROM pgseries_compress_list ORDER BY lo LOOP
        EXECUTE format('TRUNCATE %I.%I', p_schema, r.cn);
    END LOOP;
    UPDATE pgseries.chunk_lease l SET state = 'compressed', updated_at = now()
    FROM pgseries.chunk c JOIN pgseries_compress_list t ON t.cn = c.chunk_name
    WHERE l.chunk_id = c.chunk_id AND l.state = 'compressing'
      AND c.series_schema = p_schema AND c.series_table = p_table;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> v_nchunk THEN
        RAISE EXCEPTION 'pgseries: %.% lost a chunk race on close (want %, flipped %)',
            p_schema, p_table, v_nchunk, v_n
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    EXECUTE format('SELECT COALESCE(sum(seg_row_count), 0) FROM %s '
                   || 'WHERE seg_min_ts >= %L::TIMESTAMPTZ '
                   || 'AND seg_min_ts < %L::TIMESTAMPTZ',
                   v_tpl, v_min, v_max) INTO v_nrows;
    EXECUTE format('ANALYZE %s', v_tpl);
    -- Per-partition BRINs summarized exactly (parent partitioned
    -- indexes hold no storage themselves).
    FOR r IN SELECT i.indexrelid
             FROM pg_index i
             JOIN pg_inherits h ON h.inhrelid = i.indrelid
             WHERE h.inhparent = to_regclass(v_tpl) LOOP
        BEGIN
            PERFORM brin_summarize_new_values(r.indexrelid);
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
    END LOOP;

    PERFORM pgseries.refresh_read_view(p_schema, p_table);

    RETURN format('compressed %s chunks, %s rows into %s segments',
                  v_nchunk, v_nrows, v_nsegs);
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
    v_space INTEGER;
    v_node  NAME;
    v_cpart NAME;
    v_cols  TEXT;
    v_n     BIGINT;
    v_left  BIGINT;
    v_unpack TEXT := 'pgseries.unpack_' || pgseries._slug(p_schema, p_table);
BEGIN
    s := pgseries._series(p_schema, p_table);
    SELECT l.state, c.space_index, COALESCE(c.parent_chunk, c.chunk_name)
    INTO v_state, v_space, v_node
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
    -- Shared node sibling: only this leaf's slice (seg_space) moves
    -- back; the node goes away only when no slice remains.
    v_cpart := (v_node::TEXT || '_compressed')::NAME;
    IF to_regclass(format('%I.%I', p_schema, v_cpart)) IS NULL THEN
        RAISE EXCEPTION 'pgseries: compressed partition %.% missing', p_schema, v_cpart
            USING ERRCODE = 'undefined_object';
    END IF;

    PERFORM pgseries._lease_transition(p_schema, p_chunk, 'ready');
    EXECUTE 'SET LOCAL pgseries.allow_compressed_write = ''on''';
    SELECT c.v_all INTO v_cols
    FROM pgseries._copy_columns(p_schema, p_table) AS c;
    EXECUTE format('INSERT INTO %I.%I (%s) OVERRIDING SYSTEM VALUE '
                   || 'SELECT %s FROM (SELECT (%s(c)).* FROM %I.%I AS c '
                   || 'WHERE c.seg_space = %s) AS s',
        p_schema, p_chunk, v_cols, v_cols, v_unpack, p_schema, v_cpart, v_space);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    EXECUTE format('DELETE FROM %I.%I WHERE seg_space = %s',
        p_schema, v_cpart, v_space);
    -- Refresh first: the leg is already gone (state flipped above),
    -- so the conditional DROP below never hits view dependents.
    PERFORM pgseries.refresh_read_view(p_schema, p_table);
    EXECUTE format('SELECT count(*) FROM %I.%I', p_schema, v_cpart)
    INTO v_left;
    IF v_left = 0 THEN
        EXECUTE format('DROP TABLE %I.%I', p_schema, v_cpart);
    END IF;
    EXECUTE format('ANALYZE %I.%I', p_schema, p_chunk);
    RETURN format('decompressed %s rows into %I', v_n, p_chunk);
END;
$$;

-- Empirical codec comparison: packs a bounded recent-rows sample
-- (leaf-based, no full scan/sort — see _sample_sql) into temp tables
-- under each candidate and reports bytes/row. Default candidates pit
-- the spec pipeline (for+dict) against delta and delta-of-delta.
-- Per-candidate 'codecs' objects override per-column codecs; dict
-- columns get the policy guard so bench measures what a policy stores.
DROP FUNCTION IF EXISTS pgseries.bench_codecs(NAME, NAME, NAME[], INTEGER, JSONB);
CREATE OR REPLACE FUNCTION pgseries.bench_codecs(
    p_schema NAME, p_table NAME,
    p_segmentby NAME[] DEFAULT '{}',
    p_limit INTEGER DEFAULT 200000,
    p_candidates JSONB DEFAULT NULL)
RETURNS TABLE (name TEXT, rows BIGINT, bytes BIGINT,
               bytes_per_row NUMERIC, ratio_vs_heap NUMERIC,
               segment_rows INTEGER, pack_ms BIGINT)
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
        jsonb_build_object('name', 'delta_seg5k', 'temporal', 'delta', 'ints', 'delta', 'text', 'dict', 'segment_rows', 5000)));
    v_cand   JSONB;
    v_codecs JSONB;
    v_seg    NAME[];
    v_rows   BIGINT;
    v_heap   BIGINT;
    v_bytes  BIGINT;
    v_nd     BIGINT;
    v_ddl    TEXT;
    v_tgt    TEXT;
    i        INTEGER := 0;
    r        RECORD;
    v_t0     TIMESTAMPTZ;
    v_t1     TIMESTAMPTZ;
BEGIN
    s := pgseries._series(p_schema, p_table);
    -- Bounded sample of recent rows: UNION ALL over the K most recent
    -- leaves per branch (heap raw + sampled compressed segments
    -- unpacked), each LIMIT-capped. No full scan, no sort, no
    -- TABLESAMPLE (slow and flaky across many small partitions).
    -- Input order is irrelevant: _pack_segments sorts by orderby.
    EXECUTE 'CREATE TEMP TABLE bench_src AS SELECT * FROM ('
            || pgseries._sample_sql(p_schema, p_table, p_limit)
            || ') s LIMIT ' || p_limit::TEXT;
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

        -- Resolve per-column codecs by class; an optional per-column
        -- 'codecs' object on the candidate wins (suggest uses it to
        -- keep high-cardinality text out of dict while kneeing).
        -- Unmentioned dict columns get the policy guard: dict on
        -- high-cardinality text is pure loss (O(n*d) pack + fat
        -- dicts), so bench measures what a policy would store.
        v_codecs := COALESCE(v_cand -> 'codecs', '{}');
        FOR r IN SELECT * FROM pgseries._colinfo(
                     'pg_temp', 'bench_src', s.time_column, v_seg) LOOP
            IF r.kind <> 'data' THEN
                CONTINUE;
            END IF;
            IF v_codecs ? r.attname::TEXT THEN
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
        FOR r IN SELECT * FROM pgseries._colinfo(
                     'pg_temp', 'bench_src', s.time_column, v_seg)
                 WHERE kind = 'data' AND class = 'text' LOOP
            IF v_codecs ->> r.attname::TEXT <> 'dict'
               OR (v_cand -> 'codecs') ? r.attname::TEXT THEN
                CONTINUE;
            END IF;
            EXECUTE format('SELECT count(DISTINCT %I) FROM pg_temp.bench_src',
                           r.attname) INTO v_nd;
            IF v_nd > COALESCE((v_cand ->> 'segment_rows')::INTEGER, 1000) THEN
                RAISE NOTICE 'pgseries: bench %: dict on high-cardinality % (% distinct) -> raw',
                    v_cand ->> 'name', r.attname, v_nd;
                v_codecs := v_codecs || jsonb_build_object(r.attname::TEXT, 'raw');
            END IF;
        END LOOP;

        IF to_regclass('pg_temp.' || quote_ident(v_tgt)) IS NOT NULL THEN
            EXECUTE format('DROP TABLE pg_temp.%I', v_tgt);
        END IF;        FOR v_ddl IN SELECT * FROM pgseries._compressed_ddl(
                         'pg_temp', 'bench_src', 'pg_temp.' || quote_ident(v_tgt),
                         v_codecs, v_seg, false) LOOP
            EXECUTE v_ddl;
        END LOOP;
        v_t0 := clock_timestamp();
        PERFORM pgseries._pack_segments(
            'pg_temp', 'bench_src', 'pg_temp', v_tgt,
            s.time_column, s.time_col_type, v_seg, s.time_column, false,
            COALESCE((v_cand ->> 'segment_rows')::INTEGER, 1000),
            v_codecs, '{}');
        v_t1 := clock_timestamp();
        -- No ANALYZE: sizes come from pg_total_relation_size, which
        -- needs no stats (an ANALYZE per candidate was a wasted scan).
        EXECUTE format('SELECT pg_total_relation_size(%L)',
                       'pg_temp.' || v_tgt) INTO v_bytes;

        name := v_cand ->> 'name';
        rows := v_rows;
        bytes := v_bytes;
        bytes_per_row := round(v_bytes::NUMERIC / v_rows, 2);
        ratio_vs_heap := round(v_bytes::NUMERIC / NULLIF(v_heap, 0), 3);
        segment_rows := COALESCE((v_cand ->> 'segment_rows')::INTEGER, 1000);
        pack_ms := round(extract(epoch FROM (v_t1 - v_t0)) * 1000)::BIGINT;
        RETURN NEXT;
        EXECUTE format('DROP TABLE pg_temp.%I', v_tgt);
    END LOOP;
    EXECUTE 'DROP TABLE pg_temp.bench_src';
END;
$$;

-- Actionable benchmark: samples the table, picks a segmentby by
-- cardinality, knees the segment_rows curve (smallest N within 5% of
-- the best bytes), and returns the exact policy call to run.
-- This is the answer to "what do I do with bench numbers".
CREATE OR REPLACE FUNCTION pgseries.suggest_compression(
    p_schema NAME, p_table NAME, p_sample_rows INTEGER DEFAULT 50000)
RETURNS TABLE (segmentby NAME[], segment_rows INTEGER, codecs JSONB,
               expected_ratio NUMERIC, policy_sql TEXT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    s        pgseries.series_table%ROWTYPE;
    v_seg    NAME[] := '{}';
    v_col    NAME;
    v_n      BIGINT;
    v_best   BIGINT;
    r        RECORD;
BEGIN
    s := pgseries._series(p_schema, p_table);

    -- Segmentby heuristic: lowest-cardinality text-like column from
    -- planner stats (no scan). Warms the stats with one ANALYZE when
    -- missing — still a single scan versus one sorted
    -- count(DISTINCT) per text column before. Counts are recorded for
    -- the dict guard below.
    -- Segmentby heuristic: lowest-cardinality text-like column.
    -- Counts come from planner stats when usable, never a full scan:
    -- (a) heap parent stats if the heap holds rows, (b) stats of the
    -- most recent compressed sibling (compress ANALYZEs every cpart;
    -- segby columns are scalars there), (c) exact counts on a bounded
    -- 20K sample only for columns still missing. Heuristic either
    -- way; per-chunk estimates beat table-wide guesses for packing.
    <<pick_segby>>
    DECLARE
        v_best_n BIGINT := NULL;
        v_heapq  TEXT := pgseries._write_parent(p_schema, p_table);
        v_nd     NUMERIC;
        v_rel    BIGINT;
        v_src    TEXT;
        v_css    NAME;
        v_cpart  NAME;
        v_built  BOOLEAN := false;
    BEGIN
        IF to_regclass('pg_temp.suggest_nd') IS NULL THEN
            CREATE TEMP TABLE suggest_nd (col NAME PRIMARY KEY, nd BIGINT)
                ON COMMIT DROP;
        END IF;
        DELETE FROM suggest_nd;
        SELECT reltuples::BIGINT INTO v_rel FROM pg_class
        WHERE oid = to_regclass(v_heapq);
        IF COALESCE(v_rel, 0) > 0 THEN
            FOR r IN SELECT * FROM pgseries._colinfo(
                         p_schema, p_table, s.time_column, '{}')
                     WHERE kind = 'data' AND class = 'text' LOOP
                SELECT n_distinct INTO v_nd FROM pg_stats
                WHERE schemaname = p_schema::TEXT
                  AND tablename IN (pgseries._heap_name(p_table)::TEXT,
                                    p_table::TEXT)
                  AND attname = r.attname::TEXT
                ORDER BY tablename DESC LIMIT 1;
                IF v_nd IS NULL THEN
                    CONTINUE;
                END IF;
                v_n := CASE WHEN v_nd < 0
                            THEN ((-v_nd) * COALESCE(v_rel, 0))::BIGINT
                            ELSE v_nd::BIGINT END;
                INSERT INTO suggest_nd VALUES (r.attname, v_n)
                ON CONFLICT (col) DO UPDATE SET nd = EXCLUDED.nd;
            END LOOP;
            v_src := 'planner stats';
        END IF;
        -- Compressed scalars: most recent sibling with stats.
        SELECT c.chunk_schema,
               (COALESCE(c.parent_chunk, c.chunk_name)::TEXT
                || '_compressed')::NAME
        INTO v_css, v_cpart
        FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
        WHERE c.series_schema = p_schema AND c.series_table = p_table
          AND l.state IN ('compressing', 'compressed')
        ORDER BY c.range_upper DESC NULLS LAST LIMIT 1;
        IF FOUND THEN
            SELECT reltuples::BIGINT INTO v_rel FROM pg_class
            WHERE oid = to_regclass(format('%I.%I', v_css, v_cpart));
            FOR r IN SELECT * FROM pgseries._colinfo(
                         p_schema, p_table, s.time_column, '{}')
                     WHERE kind = 'data' AND class = 'text' LOOP
                IF EXISTS (SELECT 1 FROM suggest_nd
                           WHERE col = r.attname) THEN
                    CONTINUE;
                END IF;
                SELECT n_distinct INTO v_nd FROM pg_stats
                WHERE schemaname = v_css::TEXT
                  AND tablename = v_cpart::TEXT
                  AND attname = r.attname::TEXT;
                IF v_nd IS NULL THEN
                    CONTINUE;
                END IF;
                v_n := CASE WHEN v_nd < 0
                            THEN ((-v_nd) * COALESCE(v_rel, 0))::BIGINT
                            ELSE v_nd::BIGINT END;
                INSERT INTO suggest_nd VALUES (r.attname, v_n)
                ON CONFLICT (col) DO UPDATE SET nd = EXCLUDED.nd;
            END LOOP;
            v_src := COALESCE(v_src || ' + compressed stats', 'compressed stats');
        END IF;
        -- Exact sample only for columns still missing.
        IF EXISTS (SELECT 1 FROM pgseries._colinfo(
                       p_schema, p_table, s.time_column, '{}') c
                   WHERE c.kind = 'data' AND c.class = 'text'
                     AND NOT EXISTS (SELECT 1 FROM suggest_nd
                                     WHERE col = c.attname)) THEN
            RAISE NOTICE 'pgseries: sampling for segmentby...';
            EXECUTE 'CREATE TEMP TABLE segby_src AS SELECT * FROM ('
                    || pgseries._sample_sql(p_schema, p_table, 20000)
                    || ') s LIMIT 20000';
            v_built := true;
            FOR r IN SELECT * FROM pgseries._colinfo(
                         p_schema, p_table, s.time_column, '{}')
                     WHERE kind = 'data' AND class = 'text' LOOP
                IF EXISTS (SELECT 1 FROM suggest_nd
                           WHERE col = r.attname) THEN
                    CONTINUE;
                END IF;
                EXECUTE format('SELECT count(DISTINCT %I) FROM pg_temp.segby_src',
                               r.attname) INTO v_n;
                INSERT INTO suggest_nd VALUES (r.attname, v_n)
                ON CONFLICT (col) DO UPDATE SET nd = EXCLUDED.nd;
            END LOOP;
            DROP TABLE pg_temp.segby_src;
            v_src := COALESCE(v_src || ' + exact sample', 'exact sample');
        END IF;
        FOR r IN SELECT col, nd FROM suggest_nd LOOP
            IF r.nd > 1 AND r.nd <= 10000
               AND (v_best_n IS NULL OR r.nd < v_best_n) THEN
                v_best_n := r.nd;
                v_col := r.col;
            END IF;
        END LOOP;
        IF v_col IS NOT NULL THEN
            v_seg := ARRAY[v_col];
            RAISE NOTICE 'pgseries: segmentby % (~% distinct values, %)',
                v_col, v_best_n, v_src;
        ELSE
            RAISE NOTICE 'pgseries: no suitable segmentby column, packing ungrouped';
        END IF;
    END pick_segby;

    -- Knee over segment sizes (delta family throughout). High-card
    -- text stays raw even here via per-candidate codec overrides —
    -- otherwise the knee would pay dict's O(n*d) on every candidate.
    CREATE TEMP TABLE suggest_bench AS
    SELECT * FROM pgseries.bench_codecs(p_schema, p_table, v_seg,
        LEAST(p_sample_rows, 200000),
        (SELECT jsonb_agg(c)
         FROM (VALUES
           ('d_500', 500), ('d_1k', 1000), ('d_2k', 2000), ('d_5k', 5000))
         AS k(nm, sr),
         LATERAL (SELECT jsonb_build_object(
             'name', nm, 'temporal', 'delta', 'ints', 'delta',
             'text', 'dict', 'segment_rows', sr, 'codecs',
             (SELECT COALESCE(jsonb_object_agg(col, 'raw'), '{}')
              FROM suggest_nd WHERE nd > sr)) AS c) t));
    SELECT min(bytes) INTO v_best FROM suggest_bench;
    SELECT b.segment_rows INTO segment_rows FROM suggest_bench b
    WHERE b.bytes <= v_best * 1.05
    ORDER BY b.segment_rows LIMIT 1;

    segmentby := v_seg;
    codecs := '{}';
    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_schema, p_table, s.time_column, v_seg)
             WHERE kind = 'data' LOOP
        codecs := codecs || jsonb_build_object(r.attname::TEXT,
            CASE WHEN r.typename LIKE 'timestamp%' OR r.typename = 'date'
                      THEN 'delta'
                 WHEN r.class = 'int' THEN 'delta'
                 WHEN r.class = 'text'
                      AND COALESCE((SELECT nd FROM suggest_nd
                                    WHERE col = r.attname),
                                   segment_rows) <= segment_rows
                      THEN 'dict'
                 WHEN r.class = 'text' THEN 'raw'
                 ELSE 'raw' END);
    END LOOP;
    SELECT round(1 / NULLIF(min(
        CASE WHEN b.segment_rows = suggest_compression.segment_rows
             THEN b.ratio_vs_heap END), 0), 2)
    INTO expected_ratio FROM suggest_bench b;
    policy_sql := format(
        'SELECT pgseries.add_compression_policy(%L, %L, %L, %L, false, %s);',
        p_schema::TEXT, p_table::TEXT,
        CASE WHEN array_length(v_seg, 1) IS NULL THEN '{}'
             ELSE '{' || array_to_string(v_seg, ',') || '}' END,
        s.time_column::TEXT, segment_rows);
    RETURN NEXT;
    DROP TABLE suggest_bench;
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
--   <table>_heap        — the renamed partitioned heap parent (bulk
--                          ingest COPY/INSERT and internal movers address
--                          this directly; the view triggers are ~11x/row).
--   <table>             — transparent UNION view: heap + lateral unpack
--                          of the compressed sibling, with INSTEAD OF
--                          INSERT/UPDATE/DELETE routing to the heap.
--                          UPDATE/DELETE touching compressed rows raise
--                          read-only. TRUNCATE must address the heap
--                          (PG forbids TRUNCATE triggers on views).
--   <table>_compressed  — partitioned template (RANGE seg_min_ts),
--                          one partition <chunk>_compressed per chunk.
--   unpack_<s>_<t>(seg) — generated per-table decoder, exact roundtrip
--                          (floats: RAW only, bit-exact).
--   <table>_read        — compat alias over the base view.
--   <table>_read_range  — fast path with seg-bound pushdown (BRIN skip).
--
-- Codec set per column: raw | for | delta | auto | dict.
-- for/delta apply to int/timestamp/date families only (exact
-- integer arithmetic on micros/days); floats are RAW (float
-- reassociation is not bit-exact); text-like/bool/uuid/inet/arrays
-- go through text form + dict/raw; jsonb/bytea/numeric are native RAW.
-- dict resolves to raw at policy time when the sample distinct count
-- exceeds one segment's rows (high-card dicts are pure loss: fat
-- per-segment dictionaries plus O(n*d) pack lookups — measured 340 KB
-- of uuid dict per 1000-row segment for zero gain).
-- NULLs are int[] positions (sparse-friendly; the spec's bitmap is
-- equivalent but needs a custom aggregate in pure SQL).
--
-- DELIBERATELY ABSENT: delta-of-delta. Measured on 500K regular rows
-- (10s cadence) and jittered rows (+-3s): delta2 == delta to the byte
-- in every candidate (0.23 B/row ts-only, 8.81 narrow, 16.94 jittered).
-- Mechanism: int64 little-endian already exposes long zero-runs in the
-- high bytes for both encodings, and lz4 (LZ77 only, no Huffman stage)
-- eats constant runs either way — the second-order difference has no
-- entropy left to remove. delta2 would only win under an entropy coder
-- (simple-8b/Huffman), unreachable in pure SQL. It also costs an extra
-- window pass on every unpack. If you miss it, re-read this paragraph
-- after re-running bench_codecs on your own data.
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

-- Default codec per storage class. int defaults to auto (resolved
-- to for/delta at policy time by a correlation probe; see below).
CREATE OR REPLACE FUNCTION pgseries._default_codec(p_class TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT CASE p_class WHEN 'int' THEN 'auto'
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
    IF p_codec = 'delta2' THEN
        RAISE EXCEPTION 'pgseries: codec delta2 was removed (measured equivalent to delta under lz4, slower unpack; see design note above _storage_class)'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_codec NOT IN ('raw', 'for', 'delta', 'auto', 'dict') THEN
        RAISE EXCEPTION 'pgseries: unknown codec % for column %',
            p_codec, p_col
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_codec IN ('for', 'delta', 'auto') AND p_class <> 'int' THEN
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

-- L1 native expression for one column (raw column refs only).
-- Extracted from _pack_segments; byte-identical output.
CREATE OR REPLACE FUNCTION pgseries._pack_l1(
    p_attname NAME, p_typename TEXT, p_class TEXT, p_requant INTEGER,
    p_alias TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
    SELECT CASE
        WHEN p_class = 'int' AND p_typename LIKE 'timestamp%' THEN
            format(', ((extract(epoch FROM %I) * 1000000))::BIGINT AS %I',
                   p_attname, p_alias)
        WHEN p_class = 'int' AND p_typename = 'date' THEN
            format(', (%I - DATE ''1970-01-01'')::BIGINT AS %I',
                   p_attname, p_alias)
        WHEN p_class = 'int' THEN
            format(', %I::BIGINT AS %I', p_attname, p_alias)
        WHEN p_class = 'float' AND p_requant IS NOT NULL THEN
            format(', pgseries._requant(%I::DOUBLE PRECISION, %s) AS %I',
                   p_attname, p_requant, p_alias)
        WHEN p_class = 'float' THEN
            format(', %I::DOUBLE PRECISION AS %I', p_attname, p_alias)
        WHEN p_class = 'text' THEN
            format(', %I::TEXT AS %I', p_attname, p_alias)
        ELSE format(', %I AS %I', p_attname, p_alias)
    END;
$$;

-- L3 derivation for one column: select fragment (no leading comma)
-- plus insert fragment (with leading comma), per codec.
-- Extracted from _pack_segments; byte-identical output.
CREATE OR REPLACE FUNCTION pgseries._pack_l3(
    p_codec TEXT, p_class TEXT, p_pfx TEXT, p_attnum INTEGER,
    OUT v_sel TEXT, OUT v_ins TEXT)
LANGUAGE plpgsql IMMUTABLE
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_a    TEXT := 'a' || p_attnum;
    v_min  TEXT := format('(SELECT min(x) FROM unnest(%I) AS x)', 'a' || p_attnum);
    v_dict TEXT := format('(SELECT COALESCE(array_agg(DISTINCT x), ''{}'') FROM unnest(%I) AS x WHERE x IS NOT NULL)', 'a' || p_attnum);
BEGIN
    v_sel := CASE
        WHEN p_codec = 'for' THEN
            format('%s AS %I, %s AS %I, %s AS %I',
                v_min, p_pfx || '_base',
                format('(SELECT array_agg(x - (%s)) FROM unnest(%I) AS x)',
                       v_min, v_a), p_pfx || '_arr',
                format('(SELECT COALESCE(array_agg(j), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE (%I)[j] IS NULL)',
                       v_a, v_a), p_pfx || '_nulls')
        WHEN p_codec = 'delta' THEN
            format('%s AS %I, %s AS %I, %s AS %I',
                format('(%I)[1]', v_a), p_pfx || '_base',
                format('(SELECT COALESCE(array_agg((%I)[j+1] - (%I)[j]), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE j < cardinality(%I))',
                       v_a, v_a, v_a, v_a), p_pfx || '_arr',
                format('(SELECT COALESCE(array_agg(j), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE (%I)[j] IS NULL)',
                       v_a, v_a), p_pfx || '_nulls')
        WHEN p_codec = 'dict' THEN
            format('%s AS %I, %s AS %I, %s AS %I',
                v_dict, p_pfx || '_dict',
                format('(SELECT array_agg(m.p ORDER BY u.o) FROM unnest(%I) WITH ORDINALITY AS u(x, o) LEFT JOIN (SELECT d, row_number() OVER () AS p FROM unnest(%s) AS d) AS m ON m.d = u.x)',
                       v_a, v_dict), p_pfx || '_arr',
                format('(SELECT COALESCE(array_agg(j), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE (%I)[j] IS NULL)',
                       v_a, v_a), p_pfx || '_nulls')
        ELSE format('%s AS %I, %s AS %I',
                v_a, p_pfx || '_arr',
                format('(SELECT COALESCE(array_agg(j), ''{}'') FROM generate_subscripts(%I, 1) AS j WHERE (%I)[j] IS NULL)',
                       v_a, v_a), p_pfx || '_nulls')
    END;
    v_ins := CASE
        WHEN p_codec = 'dict' THEN
            format(', %I, %I, %I', p_pfx || '_dict', p_pfx || '_arr',
                   p_pfx || '_nulls')
        WHEN p_class = 'int' AND p_codec <> 'raw' THEN
            format(', %I, %I, %I', p_pfx || '_base', p_pfx || '_arr',
                   p_pfx || '_nulls')
        ELSE format(', %I, %I', p_pfx || '_arr', p_pfx || '_nulls')
    END;
END;
$$;

-- Pack rows from a heap source into a compressed target. Shared by
-- compress_chunk (real partitions) and bench_codecs (temp tables).
-- Levels: L0 raw rows + _r (window); L1 + _segno; L2 group + ordered
-- arrays a<k>; L3 codec derivation + seg metadata. Returns segments.
DROP FUNCTION IF EXISTS pgseries._pack_segments(
    NAME, NAME, NAME, NAME, NAME, TEXT, NAME[], NAME, BOOLEAN,
    INTEGER, JSONB, JSONB);
DROP FUNCTION IF EXISTS pgseries._pack_segments(
    NAME, NAME, NAME, NAME, NAME, TEXT, NAME[], NAME, BOOLEAN,
    INTEGER, JSONB, JSONB, TEXT, TEXT);
CREATE OR REPLACE FUNCTION pgseries._pack_segments(
    p_src_schema NAME, p_src_table NAME,
    p_tgt_schema NAME, p_tgt_table NAME,
    p_timecol NAME, p_timecol_type TEXT,
    p_segmentby NAME[], p_orderby NAME, p_ob_desc BOOLEAN,
    p_segment_rows INTEGER, p_codecs JSONB, p_requant JSONB,
    p_where TEXT DEFAULT '', p_bucket TEXT DEFAULT NULL,
    p_space INTEGER DEFAULT 0)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    r         RECORD;
    v_codec   TEXT;
    v_pfx     TEXT;
    v_win     TEXT;   -- full window clause for _r
    v_l1      TEXT := '';
    v_l2arr   TEXT := '';
    v_l3sel   TEXT := '';
    v_ins     TEXT := '';
    v_segcols TEXT := '';
    v_ob      TEXT := quote_ident(p_orderby);
    v_obdir   TEXT := CASE WHEN p_ob_desc THEN 'DESC' ELSE 'ASC' END;
    v_n       BIGINT;
    v_l3s     TEXT;
    v_l3i     TEXT;
    v_l2key   TEXT;
    v_part    TEXT;   -- window PARTITION BY prefix (bucket and/or segmentby)
BEGIN
    IF p_segmentby IS NULL OR array_length(p_segmentby, 1) IS NULL THEN
        v_part := '';
    ELSE
        SELECT string_agg(quote_ident(x), ', ') INTO v_segcols
        FROM unnest(p_segmentby) AS x;
        v_part := v_segcols;
    END IF;
    -- Multi-chunk packs (compress_table) reset segments per source
    -- bucket: the bucket expression becomes the leading window key
    -- (inlined — a SELECT-list alias is not visible inside its own
    -- OVER clause; the _bkt alias below exists only for GROUP BY).
    IF p_bucket IS NOT NULL THEN
        v_part := CASE WHEN v_part = '' THEN p_bucket
                       ELSE p_bucket || ', ' || v_part END;
    END IF;
    IF v_part = '' THEN
        v_win := format('ORDER BY %s %s, ctid', v_ob, v_obdir);
    ELSE
        v_win := format('PARTITION BY %s ORDER BY %s %s, ctid',
                        v_part, v_ob, v_obdir);
    END IF;
    v_l2key := CASE WHEN v_segcols = '' THEN '_segno'
                    ELSE v_segcols || ', _segno' END;
    IF p_bucket IS NOT NULL THEN
        v_l2key := '_bkt, ' || v_l2key;
    END IF;

    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_src_schema, p_src_table, p_timecol, p_segmentby) LOOP
        v_pfx := 'c' || r.attnum;
        IF r.kind = 'gen' THEN
            CONTINUE;
        ELSIF r.kind = 'segby' THEN
            -- Already present via L1's SELECT *; GROUP/INSERT lists only.
            v_l3sel := v_l3sel || format(', %I', r.attname);
            v_ins := v_ins || format(', %I', r.attname);
            CONTINUE;
        END IF;

        v_codec := pgseries._eff_codec(p_codecs, r.attname::TEXT, r.class);
        PERFORM pgseries._check_codec(v_codec, r.class, r.attname::TEXT);
        IF v_codec = 'auto' THEN
            RAISE EXCEPTION 'pgseries: codec auto for % was not resolved (policies resolve at creation)',
                r.attname
                USING ERRCODE = 'invalid_parameter_value';
        END IF;

        -- L1/L3 fragments per column (builders above).
        v_l1 := v_l1 || pgseries._pack_l1(
            r.attname, r.typename, r.class,
            (p_requant ->> r.attname::TEXT)::INTEGER, 'v' || r.attnum);

        -- L2 ordered array; L3 derivation per codec (a<k> visible there).
        v_l2arr := v_l2arr || format(', array_agg(%I ORDER BY _r) AS %I',
                                     'v' || r.attnum, 'a' || r.attnum);
        SELECT l3.v_sel, l3.v_ins INTO v_l3s, v_l3i
        FROM pgseries._pack_l3(v_codec, r.class, v_pfx, r.attnum) AS l3;
        v_l3sel := v_l3sel || ', ' || v_l3s;
        v_ins := v_ins || v_l3i;
    END LOOP;

    EXECUTE format(
        'WITH l0 AS (SELECT *, row_number() OVER (%s) AS _r%s '
        || 'FROM %I.%I%s), '
        || 'l1 AS (SELECT *%s, ((_r - 1) / %s) AS _segno FROM l0), '
        || 'l2 AS (SELECT %s, min(%I) AS _lo, max(%I) AS _hi, '
        || 'count(*) AS _n%s FROM l1 GROUP BY %s) '
        || 'INSERT INTO %I.%I (seg_no, seg_min_ts, seg_max_ts, '
        || 'seg_row_count, seg_space%s) '
        || 'SELECT row_number() OVER (ORDER BY %s), '
        || '_lo::TIMESTAMPTZ, _hi::TIMESTAMPTZ, _n, %s%s '
        || 'FROM l2',
        v_win,
        CASE WHEN p_bucket IS NULL THEN '' ELSE format(', (%s) AS _bkt', p_bucket) END,
        p_src_schema, p_src_table,
        CASE WHEN p_where = '' THEN '' ELSE ' WHERE ' || p_where END,
        v_l1, p_segment_rows,
        v_l2key, p_timecol, p_timecol, v_l2arr,
        v_l2key,
        p_tgt_schema, p_tgt_table, v_ins,
        v_l2key, p_space,
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

-- Generated per-table decoder. Single-pass reconstruction: one
-- multi-argument unnest per segment (no per-row array subscripts —
-- subscripting a TOASTed array detoasts it on every row, O(n^2) per
-- segment; measured 3x read speedup), delta via an exclusive running
-- sum, no plpgsql loops on the read path. Decoded columns are
-- assembled in attnum order; generated columns wrap in nested levels
-- (each sees the previous level's outputs). Returns the parent
-- rowtype so the static read view typechecks.
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
    v_uargs   TEXT := '';
    v_ualias  TEXT := '';
    v_inner   TEXT := '';
    v_wrap    TEXT;
    v_final   TEXT := '';
    v_g       TEXT;
    v_arr     TEXT;
    v_base    TEXT;
    v_nulls   TEXT;
    v_time    NAME;
    v_has_n   BOOLEAN := false;
BEGIN
    SELECT time_column INTO v_time FROM pgseries.series_table
    WHERE series_schema = p_schema AND series_table = p_table;

    FOR r IN SELECT * FROM pgseries._colinfo(
                 p_schema, p_table, v_time, p_segmentby) LOOP
        IF r.kind = 'data' AND pgseries._eff_codec(
               p_codecs, r.attname::TEXT, r.class) <> 'delta' THEN
            v_has_n := true;
        END IF;
    END LOOP;

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
        PERFORM pgseries._check_codec(v_codec, r.class, r.attname::TEXT);
        IF v_codec = 'auto' THEN
            RAISE EXCEPTION 'pgseries: codec auto for % was not resolved (policies resolve at creation)',
                r.attname
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        v_arr := format('seg.%I', v_pfx || '_arr');
        v_base := format('seg.%I', v_pfx || '_base');
        v_nulls := format('seg.%I', v_pfx || '_nulls');

        -- Unnest driver args (one multi-argument unnest below; alias
        -- order matches). Delta diffs are short (n-1); with no
        -- full-length array in the table the first delta array is
        -- zero-padded so the last row is not dropped (the pad only
        -- ever feeds the running sum of nonexistent later rows).
        IF v_codec = 'delta' AND NOT v_has_n THEN
            v_has_n := true; -- pad exactly once
            v_uargs := v_uargs || format(', (%s || 0)', v_arr);
        ELSE
            v_uargs := v_uargs || format(', %s', v_arr);
        END IF;
        v_ualias := v_ualias || CASE WHEN v_codec = 'delta'
            THEN format(', d%s', r.attnum)
            WHEN v_codec = 'dict'
            THEN format(', x%s', r.attnum)
            ELSE format(', v%s', r.attnum) END;

        v_g := CASE
            WHEN v_codec = 'for' THEN
                format('(CASE WHEN u.i = ANY(%s) THEN NULL ELSE %s + u.v%s END)',
                       v_nulls, v_base, r.attnum)
            WHEN v_codec = 'delta' THEN
                format('(CASE WHEN u.i = ANY(%s) THEN NULL ELSE %s + '
                       || 'COALESCE(sum(u.d%s) OVER (ORDER BY u.i ROWS BETWEEN '
                       || 'UNBOUNDED PRECEDING AND 1 PRECEDING), 0) END)',
                       v_nulls, v_base, r.attnum)
            WHEN v_codec = 'dict' THEN
                format('(CASE WHEN u.i = ANY(%s) THEN NULL ELSE (%s)[u.x%s] END)',
                       v_nulls,
                       format('seg.%I', v_pfx || '_dict'), r.attnum)
            ELSE
                format('(CASE WHEN u.i = ANY(%s) THEN NULL ELSE u.v%s END)',
                       v_nulls, r.attnum)
        END;

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
    v_wrap := format('SELECT %s FROM unnest(%s) WITH ORDINALITY AS u(%s, i) '
                     || 'WHERE u.i <= seg.seg_row_count',
                     ltrim(v_inner, ', '),
                     ltrim(v_uargs, ', '), ltrim(v_ualias, ', '));
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

    -- RETURNS the heap rowtype, not the base view: the heap OID is
    -- stable (the table OID follows the rename), so future
    -- regenerations REPLACE cleanly; a view-bound return type would
    -- demand DROP-first upgrades.
    RETURN format(
        'CREATE OR REPLACE FUNCTION %s(seg %s) RETURNS SETOF %I.%I '
        || 'LANGUAGE sql STABLE PARALLEL SAFE SET search_path = pgseries, pg_catalog AS %s '
        || 'SELECT %s FROM (%s) AS qfinal '
        || '%s',
        v_fname, v_tpl, p_schema, pgseries._heap_name(p_table),
        '$pgseries_unpack$',
        ltrim(v_final, ', '), v_wrap,
        '$pgseries_unpack$');
END;
$$;

-- Regenerate a table's unpack function in place (heap-bound return
-- type, so OR REPLACE never hits the return-type-change error that
-- blocks upgrades). Needed when _unpack_ddl itself changes; normal
-- policy creation already generates it via _make_transparent.
CREATE OR REPLACE FUNCTION pgseries.rebuild_unpack(
    p_schema NAME, p_table NAME)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_ddl TEXT;
    pol   pgseries.compression_policy%ROWTYPE;
BEGIN
    PERFORM pgseries._series(p_schema, p_table);
    SELECT * INTO pol FROM pgseries.compression_policy
    WHERE series_schema = p_schema AND series_table = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pgseries: %.% has no compression policy',
            p_schema, p_table
            USING ERRCODE = 'undefined_object';
    END IF;
    FOR v_ddl IN SELECT * FROM pgseries._unpack_ddl(
                     p_schema, p_table, pol.codecs, pol.segmentby) LOOP
        EXECUTE v_ddl;
    END LOOP;
END;
$$;

-- Column inventory of a series table (one row per live source column).
-- consumed_by: 'segby' | 'time' | 'data'. gexpr set for generated cols.
-- Resolves the heap when the transparent view is installed (views
-- report no identity/generated flags, so the base name would
-- misclassify columns post-swap).
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
    WHERE a.attrelid = to_regclass(pgseries._write_parent(p_schema, p_table))
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
        || 'seg_row_count INTEGER NOT NULL, '
        || 'seg_space INTEGER NOT NULL DEFAULT 0';

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

-- parent_chunk appended last: CREATE OR REPLACE VIEW cannot change
-- middle column names on upgrade (existing installs).
CREATE OR REPLACE VIEW pgseries_information.chunks AS
SELECT c.series_schema, c.series_table,
       c.chunk_schema, c.chunk_name,
       c.range_lower, c.range_upper, c.space_index,
       l.state, c.created_at, c.parent_chunk
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

-- Per-chunk compression stats. Set-based: one UNION ALL probe per
-- side (a single EXECUTE each) plus one pg_class size join — no
-- per-chunk COUNT loop regardless of chunk count.
CREATE OR REPLACE FUNCTION pgseries.compressed_chunk_stats(
    p_schema NAME, p_table NAME)
RETURNS TABLE (chunk_name NAME, state TEXT, segments BIGINT,
               compressed_rows BIGINT, compressed_bytes BIGINT,
               heap_rows BIGINT)
LANGUAGE plpgsql VOLATILE
SECURITY DEFINER
SET search_path = pgseries, pg_catalog
AS $$
DECLARE
    v_sql TEXT;
BEGIN
    PERFORM pgseries._series(p_schema, p_table);
    IF to_regclass('pg_temp.pgseries_stats_heap') IS NULL THEN
        CREATE TEMP TABLE pgseries_stats_heap (cn NAME PRIMARY KEY, n BIGINT)
            ON COMMIT DROP;
    END IF;
    DELETE FROM pgseries_stats_heap;
    IF to_regclass('pg_temp.pgseries_stats_comp') IS NULL THEN
        CREATE TEMP TABLE pgseries_stats_comp (
            cn NAME PRIMARY KEY, segs BIGINT, crows BIGINT, cbytes BIGINT)
            ON COMMIT DROP;
    END IF;
    DELETE FROM pgseries_stats_comp;
    IF to_regclass('pg_temp.pgseries_stats_node') IS NULL THEN
        CREATE TEMP TABLE pgseries_stats_node (
            node NAME PRIMARY KEY, nrows BIGINT, nbytes BIGINT)
            ON COMMIT DROP;
    END IF;
    DELETE FROM pgseries_stats_node;

    SELECT string_agg(
               format('SELECT %L::NAME AS cn, count(*) AS n FROM %I.%I',
                      c.chunk_name, c.chunk_schema, c.chunk_name),
               ' UNION ALL ')
    INTO v_sql
    FROM pgseries.chunk c
    WHERE c.series_schema = p_schema AND c.series_table = p_table;
    IF v_sql IS NOT NULL THEN
        EXECUTE 'INSERT INTO pgseries_stats_heap ' || v_sql;
    END IF;

    -- Per-leaf slice of the shared node sibling (seg_space), one
    -- UNION ALL for all leaves. Bytes are node totals split
    -- proportionally to slice rows (estimate — TOAST is shared).
    SELECT string_agg(
               format('SELECT %L::NAME AS cn, count(*) AS segs, '
                      || 'COALESCE(sum(seg_row_count), 0) AS crows '
                      || 'FROM %I.%I WHERE seg_space = %s',
                      c.chunk_name, c.chunk_schema,
                      (COALESCE(c.parent_chunk, c.chunk_name)::TEXT
                       || '_compressed')::NAME,
                      c.space_index),
               ' UNION ALL ')
    INTO v_sql
    FROM pgseries.chunk c
    WHERE c.series_schema = p_schema AND c.series_table = p_table
      AND to_regclass(format('%I.%I', c.chunk_schema,
                             (COALESCE(c.parent_chunk, c.chunk_name)::TEXT
                              || '_compressed')::NAME))
          IS NOT NULL;
    IF v_sql IS NOT NULL THEN
        EXECUTE 'INSERT INTO pgseries_stats_comp (cn, segs, crows) ' || v_sql;
        INSERT INTO pgseries_stats_node (node, nrows, nbytes)
        SELECT COALESCE(c.parent_chunk, c.chunk_name),
               sum(s2.crows),
               pg_total_relation_size(to_regclass(format(
                   '%I.%I', c.chunk_schema,
                   (COALESCE(c.parent_chunk, c.chunk_name)::TEXT
                    || '_compressed')::NAME)))
        FROM pgseries.chunk c
        JOIN pgseries_stats_comp s2 ON s2.cn = c.chunk_name
        WHERE c.series_schema = p_schema AND c.series_table = p_table
        GROUP BY 1, c.chunk_schema;
        UPDATE pgseries_stats_comp s
        SET cbytes = (n.nbytes * s.crows / NULLIF(n.nrows, 0))::BIGINT
        FROM pgseries.chunk c
        JOIN pgseries_stats_node n
          ON n.node = COALESCE(c.parent_chunk, c.chunk_name)
        WHERE c.series_schema = p_schema AND c.series_table = p_table
          AND s.cn = c.chunk_name;
    END IF;

    RETURN QUERY
    SELECT c.chunk_name, l.state,
           COALESCE(s.segs, 0), COALESCE(s.crows, 0), COALESCE(s.cbytes, 0),
           COALESCE(h.n, 0)
    FROM pgseries.chunk c JOIN pgseries.chunk_lease l USING (chunk_id)
    LEFT JOIN pgseries_stats_heap h ON h.cn = c.chunk_name
    LEFT JOIN pgseries_stats_comp s ON s.cn = c.chunk_name
    WHERE c.series_schema = p_schema AND c.series_table = p_table
    ORDER BY c.range_lower NULLS LAST, c.chunk_name;
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
-- Drop-cascade hook (best effort: creating event triggers needs
-- superuser; restricted installs keep working, minus the cascade —
-- use drop_series_table() there).
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_event_trigger
               WHERE evtname = 'pgseries_series_drop') THEN
        EXECUTE 'DROP EVENT TRIGGER pgseries_series_drop';
    END IF;
    CREATE EVENT TRIGGER pgseries_series_drop ON sql_drop
    WHEN TAG IN ('DROP TABLE', 'DROP VIEW')
    EXECUTE FUNCTION pgseries._on_series_drop();
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'pgseries: skipping drop-cascade event trigger (needs superuser); use drop_series_table() to remove series tables cleanly';
END $$;

GRANT SELECT ON ALL TABLES IN SCHEMA pgseries_information TO pgseries_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA pgseries_information TO pgseries_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA pgseries_information TO pgseries_admin;

COMMIT;
