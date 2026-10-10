-- PgSeries v0.1/v0.2-base smoke test. Plain DO blocks, no extensions.
-- Any failed assertion RAISEs and (with ON_ERROR_STOP=1) aborts the run.

\pset footer off

DO $$ BEGIN RAISE NOTICE '--- 1. create_series_table ---'; END $$;

CREATE TABLE public.metrics (
    t TIMESTAMPTZ NOT NULL,
    device TEXT,
    v DOUBLE PRECISION
);
INSERT INTO public.metrics
SELECT now() - (s || ' hours')::INTERVAL, 'd' || (s % 3), s
FROM generate_series(1, 300) AS s;

SELECT pgseries.create_series_table('public', 'metrics', 't', '1 day');

DO $$
DECLARE
    v_ispart BOOLEAN;
    v_rows   BIGINT;
    v_def    BIGINT;
    v_brin   INTEGER;
BEGIN
    SELECT c.relispartition INTO v_ispart FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relname = 'metrics';
    IF NOT COALESCE(v_ispart, false) AND NOT EXISTS (
        SELECT 1 FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'metrics') THEN
        RAISE EXCEPTION 'metrics is not partitioned after create';
    END IF;
    SELECT count(*) INTO v_rows FROM public.metrics;
    IF v_rows <> 300 THEN RAISE EXCEPTION 'row loss: % rows, want 300', v_rows; END IF;
    SELECT count(*) INTO v_def FROM public.metrics_default;
    IF v_def <> 300 THEN RAISE EXCEPTION 'default should hold all 300 rows, has %', v_def; END IF;
    -- BRIN cascaded onto... parent has a BRIN partitioned index
    SELECT count(*) INTO v_brin FROM pg_indexes
    WHERE schemaname = 'public' AND tablename = 'metrics' AND indexdef ILIKE '%brin%';
    IF v_brin < 1 THEN RAISE EXCEPTION 'no BRIN index on parent'; END IF;
    IF (SELECT count(*) FROM pgseries.series_table) <> 1 THEN RAISE EXCEPTION 'catalog series_table wrong'; END IF;
    IF (SELECT count(*) FROM pgseries.dimension WHERE dim_type = 'time') <> 1 THEN RAISE EXCEPTION 'dimension time row missing'; END IF;
    IF (SELECT count(*) FROM pgseries.job) <> 2 THEN RAISE EXCEPTION 'expected 2 auto jobs, got %', (SELECT count(*) FROM pgseries.job); END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 2. precreate (idempotent) ---'; END $$;
SELECT pgseries.precreate_chunks('public', 'metrics', 7) AS created_first;
SELECT pgseries.precreate_chunks('public', 'metrics', 7) AS created_second_must_be_0;

DO $$
DECLARE
    a INTEGER; b INTEGER;
BEGIN
    SELECT pgseries.precreate_chunks('public', 'metrics', 7) INTO a;
    -- already created above, so a must be 0
    IF a <> 0 THEN RAISE EXCEPTION 'precreate not idempotent: %', a; END IF;
    SELECT count(*) INTO b FROM pgseries.chunk
    WHERE series_schema = 'public' AND series_table = 'metrics';
    IF b < 7 THEN RAISE EXCEPTION 'want >=7 chunks, got %', b; END IF;
    IF EXISTS (SELECT 1 FROM pgseries.chunk_lease l JOIN pgseries.chunk c USING (chunk_id)
               WHERE c.series_table = 'metrics' AND l.state <> 'ready') THEN
        RAISE EXCEPTION 'new chunks must be ready';
    END IF;
    -- every registered chunk must have a real partition behind it
    IF EXISTS (SELECT 1 FROM pgseries.chunk c WHERE c.series_table = 'metrics'
               AND to_regclass('"' || c.chunk_schema || '"."' || c.chunk_name || '"') IS NULL) THEN
        RAISE EXCEPTION 'orphan catalog chunk without a table';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 3. move_default (incl. multi-row INSERT..SELECT) ---'; END $$;

-- Rows for ranges with no partition land in DEFAULT (by design).
-- Capture the level, add 70 far-past rows, expect level+70.
CREATE TEMP TABLE IF NOT EXISTS pgseries_smoke_ctx (k TEXT PRIMARY KEY, n BIGINT);
DELETE FROM pgseries_smoke_ctx;
INSERT INTO pgseries_smoke_ctx
SELECT 'd0', count(*) FROM public.metrics_default;
INSERT INTO public.metrics
SELECT now() - '60 days'::INTERVAL - (s || ' minutes')::INTERVAL, 'old', s
FROM generate_series(1, 50) AS s;
INSERT INTO public.metrics
SELECT now() - '61 days'::INTERVAL, 'bulk-' || s, s FROM generate_series(1, 20) AS s;

DO $$
DECLARE v_def BIGINT; v_d0 BIGINT;
BEGIN
    -- precreate already pulled recent buckets out of DEFAULT via
    -- load-then-attach; only the 70 far-past rows must be new here.
    SELECT n INTO v_d0 FROM pgseries_smoke_ctx WHERE k = 'd0';
    SELECT count(*) INTO v_def FROM public.metrics_default;
    IF v_def <> v_d0 + 70 THEN
        RAISE EXCEPTION 'default holds %, want %', v_def, v_d0 + 70;
    END IF;
END $$;

DO $$
DECLARE v_m1 BIGINT;
BEGIN
    SELECT pgseries.move_default('public', 'metrics') INTO v_m1;
    IF v_m1 < 70 THEN RAISE EXCEPTION 'first move moved %, want >=70', v_m1; END IF;
    IF (SELECT pgseries.move_default('public', 'metrics')) <> 0 THEN
        RAISE EXCEPTION 'second move must be 0';
    END IF;
    IF (SELECT count(*) FROM public.metrics_default) <> 0 THEN
        RAISE EXCEPTION 'default not drained';
    END IF;
    IF (SELECT count(*) FROM public.metrics) <> 370 THEN
        RAISE EXCEPTION 'total row count changed after move';
    END IF;
    IF (SELECT count(*) FROM pgseries.show_chunks('public', 'metrics')) < 9 THEN
        RAISE EXCEPTION 'expected new chunks from mover';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 4. lease FSM invariants ---'; END $$;
DO $$
DECLARE v_cs NAME; v_cn NAME;
BEGIN
    SELECT chunk_schema, chunk_name INTO v_cs, v_cn
    FROM pgseries.show_chunks('public', 'metrics')
    WHERE range_lower > now() - INTERVAL '2 days'
    ORDER BY range_lower DESC LIMIT 1;

    PERFORM pgseries._lease_transition(v_cs, v_cn, 'compressing');
    BEGIN
        PERFORM pgseries._lease_transition(v_cs, v_cn, 'ready');
        RAISE EXCEPTION 'compressing->ready must be rejected';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    PERFORM pgseries._lease_transition(v_cs, v_cn, 'compressed');
    -- v0.6: compressed->ready is the decompress edge (must succeed)
    PERFORM pgseries._lease_transition(v_cs, v_cn, 'ready');
    BEGIN
        PERFORM pgseries._lease_transition(v_cs, v_cn, 'pending');
        RAISE EXCEPTION 'any->pending must be rejected';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    PERFORM pgseries._lease_transition(v_cs, v_cn, 'compressing');
    PERFORM pgseries._lease_transition(v_cs, v_cn, 'compressed');
    PERFORM pgseries._lease_transition(v_cs, v_cn, 'leased_for_drop');
    BEGIN
        PERFORM pgseries._lease_transition(v_cs, v_cn, 'ready');
        RAISE EXCEPTION 'leased_for_drop is terminal';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    RAISE NOTICE 'FSM ok on %.%', v_cs, v_cn;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 5. drop_chunks (dry-run + real + cagg-guard no-op) ---'; END $$;
SELECT * FROM pgseries.drop_chunks('public', 'metrics', '30 days', true) AS dry;
DO $$
BEGIN
    IF (SELECT count(*) FROM pgseries.drop_chunks('public', 'metrics', '30 days', true)) < 2 THEN
        RAISE EXCEPTION 'dry run should list the 60/61-day-old chunks';
    END IF;
    IF (SELECT count(*) FROM public.metrics) <> 370 THEN
        RAISE EXCEPTION 'dry run must not change data';
    END IF;
END $$;
SELECT * FROM pgseries.drop_chunks('public', 'metrics', '30 days') AS dropped;
DO $$
BEGIN
    IF (SELECT count(*) FROM public.metrics) <> 300 THEN
        RAISE EXCEPTION 'want 300 rows after retention drop, got %',
            (SELECT count(*) FROM public.metrics);
    END IF;
    IF (SELECT count(*) FROM pgseries.drop_chunks('public', 'metrics', '30 days')) <> 0 THEN
        RAISE EXCEPTION 'second drop must be empty';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 6. adopt_partitioned_table ---'; END $$;
CREATE TABLE public.logs (t TIMESTAMPTZ NOT NULL, msg TEXT) PARTITION BY RANGE (t);
CREATE TABLE public.logs_202601 PARTITION OF public.logs
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE public.logs_default PARTITION OF public.logs DEFAULT;
INSERT INTO public.logs VALUES ('2026-01-15 00:00+00', 'a'), (now(), 'b');
SELECT pgseries.adopt_partitioned_table('public', 'logs', 't', '1 day');
DO $$
BEGIN
    IF (SELECT count(*) FROM public.logs) <> 2 THEN RAISE EXCEPTION 'adopt moved data?!'; END IF;
    -- Bounds parsed (NULL = unparsed). Compare as an interval, not an
    -- instant: the tz-less DDL literal binds to the session timezone.
    IF NOT EXISTS (SELECT 1 FROM pgseries.chunk WHERE series_table = 'logs'
                   AND range_lower IS NOT NULL
                   AND age(range_upper, range_lower) = '1 month'::INTERVAL) THEN
        RAISE EXCEPTION 'adopted chunk bounds not parsed';
    END IF;
    IF (SELECT pgseries.precreate_chunks('public', 'logs', 3)) < 1 THEN
        RAISE EXCEPTION 'precreate on adopted table failed';
    END IF;
    -- load-then-attach places the now() row at creation time, so the
    -- mover is idle afterwards; both must agree on the total.
    IF (SELECT count(*) FROM public.logs_default) <> 0 THEN
        RAISE EXCEPTION 'precreate should have placed the now() row';
    END IF;
    IF (SELECT pgseries.move_default('public', 'logs')) <> 0 THEN
        RAISE EXCEPTION 'mover should be idle after precreate';
    END IF;
    IF (SELECT count(*) FROM public.logs) <> 2 THEN
        RAISE EXCEPTION 'adopted table lost rows';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 7. tune() print mode ---'; END $$;
SELECT * FROM pgseries.tune();
DO $$
BEGIN
    IF (SELECT count(*) FROM pgseries.tune()) <> 7 THEN
        RAISE EXCEPTION 'tune must return 7 rows';
    END IF;
    IF EXISTS (SELECT 1 FROM pgseries.tune() WHERE applied) THEN
        RAISE EXCEPTION 'print mode must not apply anything';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pgseries.tune(true)
                   WHERE note ILIKE 'run as superuser outside a transaction:%') THEN
        RAISE EXCEPTION 'apply mode must return runnable commands';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 8. roles ---'; END $$;
DO $$
BEGIN
    IF has_table_privilege('pgseries_writer', 'pgseries.chunk_lease', 'SELECT') THEN
        RAISE EXCEPTION 'writer must not read chunk_lease';
    END IF;
    IF has_table_privilege('pgseries_reader', 'pgseries.chunk', 'INSERT') THEN
        RAISE EXCEPTION 'reader must not mutate catalog';
    END IF;
    IF NOT has_table_privilege('pgseries_writer', 'public.metrics', 'INSERT') THEN
        RAISE EXCEPTION 'writer must write series tables';
    END IF;
    IF NOT has_table_privilege('pgseries_reader', 'public.metrics', 'SELECT') THEN
        RAISE EXCEPTION 'reader must read series tables';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 9. jobs: retention + control + run_job_now ---'; END $$;
SELECT pgseries.add_retention_policy('public', 'logs', '30 days') AS retention_job;
SELECT pgseries.pause_job(job_id) FROM pgseries.job WHERE job_type = 'retention' LIMIT 1;
DO $$ BEGIN
    IF (SELECT enabled FROM pgseries.job WHERE job_type = 'retention' LIMIT 1) THEN
        RAISE EXCEPTION 'pause failed';
    END IF;
END $$;
SELECT pgseries.resume_job(job_id) FROM pgseries.job WHERE job_type = 'retention' LIMIT 1;
SELECT pgseries.alter_job(job_id, NULL, '12 hours') FROM pgseries.job WHERE job_type = 'retention' LIMIT 1;
SELECT pgseries.run_job_now(job_id) AS precreate_out FROM pgseries.job WHERE job_type = 'precreate' AND target_table = 'metrics' LIMIT 1;
SELECT pgseries.run_job_now(job_id) AS retention_out FROM pgseries.job WHERE job_type = 'retention' LIMIT 1;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pgseries.job WHERE job_type = 'retention' AND last_run IS NULL) THEN
        RAISE EXCEPTION 'run_job_now must stamp last_run';
    END IF;
END $$;
SELECT pgseries.remove_retention_policy(job_id) FROM pgseries.job WHERE job_type = 'retention' LIMIT 1;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pgseries.job WHERE job_type = 'retention') THEN
        RAISE EXCEPTION 'remove_retention_policy failed';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 10. forward stubs raise 0A000 ---'; END $$;
DO $$
BEGIN
    BEGIN PERFORM pgseries.add_dimension('public', 'metrics', 'device');
        RAISE EXCEPTION 'add_dimension must raise';
    EXCEPTION WHEN feature_not_supported THEN NULL;
    END;
    BEGIN PERFORM pgseries.add_continuous_aggregate('c', 'select 1');
        RAISE EXCEPTION 'add_continuous_aggregate must raise';
    EXCEPTION WHEN feature_not_supported THEN NULL;
    END;
    BEGIN PERFORM pgseries_pgque.ticker();
        RAISE EXCEPTION 'ticker must raise';
    EXCEPTION WHEN feature_not_supported THEN NULL;
    END;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 11. info views ---'; END $$;
SELECT * FROM pgseries_information.series_tables;
SELECT count(*) AS n_chunks FROM pgseries_information.chunks;
SELECT * FROM pgseries_information.dimensions;
SELECT count(*) AS n_jobs FROM pgseries_information.jobs;

DO $$ BEGIN RAISE NOTICE '--- 12. date time-column path ---'; END $$;
CREATE TABLE public.events (d DATE NOT NULL, v INT);
INSERT INTO public.events SELECT CURRENT_DATE - s, s FROM generate_series(0, 5) AS s;
SELECT pgseries.create_series_table('public', 'events', 'd', '1 day');
DO $$
BEGIN
    IF (SELECT count(*) FROM public.events) <> 6 THEN RAISE EXCEPTION 'date table lost rows'; END IF;
    PERFORM pgseries.precreate_chunks('public', 'events', 3);
    INSERT INTO public.events VALUES (CURRENT_DATE - 40, 99);
    IF (SELECT pgseries.move_default('public', 'events')) < 1 THEN
        RAISE EXCEPTION 'date mover moved nothing';
    END IF;
    IF (SELECT count(*) FROM public.events) <> 7 THEN RAISE EXCEPTION 'date total wrong'; END IF;
    IF (SELECT count(*) FROM public.events_default) <> 0 THEN RAISE EXCEPTION 'date default not drained'; END IF;
    IF (SELECT count(*) FROM pgseries.drop_chunks('public', 'events', '30 days')) <> 1 THEN
        RAISE EXCEPTION 'date retention should drop the 40-day-old chunk';
    END IF;
    IF (SELECT count(*) FROM public.events) <> 6 THEN RAISE EXCEPTION 'date post-retention total wrong'; END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 13. timestamp (no tz) path ---'; END $$;
CREATE TABLE public.tsvals (t TIMESTAMP NOT NULL, v INT);
INSERT INTO public.tsvals SELECT now() - (s || ' hours')::INTERVAL, s FROM generate_series(1, 50) AS s;
SELECT pgseries.create_series_table('public', 'tsvals', 't', '1 day');
DO $$
BEGIN
    PERFORM pgseries.precreate_chunks('public', 'tsvals', 3);
    INSERT INTO public.tsvals VALUES (TIMESTAMP '2022-05-05 12:00', 1);
    IF (SELECT pgseries.move_default('public', 'tsvals')) < 1 THEN
        RAISE EXCEPTION 'timestamp mover moved nothing';
    END IF;
    IF (SELECT count(*) FROM public.tsvals) <> 51 THEN RAISE EXCEPTION 'timestamp total wrong'; END IF;
    IF (SELECT count(*) FROM public.tsvals_default) <> 0 THEN RAISE EXCEPTION 'timestamp default not drained'; END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 14. PK covering time key is carried ---'; END $$;
CREATE TABLE public.pkdemo (t TIMESTAMPTZ NOT NULL, d TEXT NOT NULL, v INT,
    day DATE GENERATED ALWAYS AS ((t AT TIME ZONE 'UTC')::date) STORED,
    seq BIGINT GENERATED ALWAYS AS IDENTITY,
    PRIMARY KEY (t, d));
INSERT INTO public.pkdemo (t, d, v) VALUES ('2026-03-03 12:00+00', 'a', 1), ('2026-03-03 12:00+00', 'b', 2);
SELECT pgseries.create_series_table('public', 'pkdemo', 't', '1 day');
DO $$
DECLARE v_seq BIGINT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
                   JOIN pg_namespace n ON n.oid = t.relnamespace
                   WHERE n.nspname = 'public' AND t.relname = 'pkdemo' AND c.contype = 'p') THEN
        RAISE EXCEPTION 'PK lost in conversion';
    END IF;
    BEGIN
        INSERT INTO public.pkdemo (t, d, v) VALUES ('2026-03-03 12:00+00', 'a', 99);
        RAISE EXCEPTION 'PK not enforced after conversion';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;
    -- generated recomputed, identity preserved + sequence advanced
    IF (SELECT day FROM public.pkdemo WHERE d = 'a') <> '2026-03-03' THEN
        RAISE EXCEPTION 'generated column wrong after conversion';
    END IF;
    IF (SELECT count(DISTINCT seq) FROM public.pkdemo) <> 2 THEN
        RAISE EXCEPTION 'identity values not preserved';
    END IF;
    INSERT INTO public.pkdemo (t, d, v) VALUES ('2026-03-04 12:00+00', 'c', 3)
    RETURNING seq INTO v_seq;
    IF v_seq <= (SELECT max(seq) FROM public.pkdemo WHERE d IN ('a','b')) THEN
        RAISE EXCEPTION 'identity sequence did not advance (got %)', v_seq;
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 15. compression policy + validation ---'; END $$;
DO $$
BEGIN
    BEGIN PERFORM pgseries.add_compression_policy('public', 'metrics', '{}', 't', false, 1000, '{"v": "delta"}');
        RAISE EXCEPTION 'delta on float must be rejected';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    BEGIN PERFORM pgseries.add_compression_policy('public', 'metrics', '{}', 't', false, 1000, '{"nope": "raw"}');
        RAISE EXCEPTION 'unknown column must be rejected';
    EXCEPTION WHEN undefined_column THEN NULL;
    END;
    BEGIN PERFORM pgseries.add_compression_policy('public', 'metrics', '{t}', 't');
        RAISE EXCEPTION 'time segmentby must be rejected';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    BEGIN PERFORM pgseries.add_compression_policy('public', 'metrics', '{}', 't', false, 100, '{"t": "delta2"}');
        RAISE EXCEPTION 'delta2 must be rejected (removed)';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
END $$;
-- auto resolution by measurement: monotonic t -> delta, hash-noise r -> for
CREATE TABLE public.autots (t TIMESTAMPTZ NOT NULL, r BIGINT);
INSERT INTO public.autots
SELECT '2026-01-01 00:00+00'::timestamptz + (s || ' minutes')::interval,
       abs(hashtextextended(s::text, 0) % 1000000000)
FROM generate_series(1, 20000) AS s;
SELECT pgseries.create_series_table('public', 'autots', 't', '1 day');
SELECT pgseries.add_compression_policy('public', 'autots', '{}', 't', false, 1000, '{}');
DO $$
DECLARE v_c JSONB;
BEGIN
    SELECT codecs INTO v_c FROM pgseries.compression_policy WHERE series_table = 'autots';
    IF v_c ->> 't' <> 'delta' THEN RAISE EXCEPTION 'auto failed for ts: %', v_c; END IF;
    IF v_c ->> 'r' <> 'for' THEN RAISE EXCEPTION 'auto failed for noise: %', v_c; END IF;
    IF v_c::TEXT LIKE '%auto%' THEN RAISE EXCEPTION 'unresolved auto stored: %', v_c; END IF;
END $$;
SELECT pgseries.add_compression_policy('public', 'metrics', '{device}', 't', false, 100, '{"t": "delta"}');
DO $$
BEGIN
    IF (SELECT count(*) FROM pgseries_information.compression_settings
        WHERE series_table = 'metrics') <> 1 THEN
        RAISE EXCEPTION 'policy not registered';
    END IF;
    IF to_regclass('public.metrics_compressed') IS NULL THEN
        RAISE EXCEPTION 'compressed template missing';
    END IF;
    IF to_regproc('pgseries.unpack_public_metrics') IS NULL THEN
        RAISE EXCEPTION 'unpack function missing';
    END IF;
    IF to_regclass('public.metrics_read') IS NULL THEN
        RAISE EXCEPTION 'read view missing';
    END IF;
    IF (SELECT count(*) FROM pg_indexes WHERE schemaname = 'public'
        AND tablename = 'metrics_compressed' AND indexdef ILIKE '%brin%') < 1 THEN
        RAISE EXCEPTION 'no BRIN on compressed template';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 16. compress roundtrip ---'; END $$;
CREATE TABLE public.metrics_pre AS SELECT * FROM public.metrics;
CREATE TEMP TABLE pgseries_victim AS
SELECT chunk_name AS victim FROM pgseries.show_chunks('public', 'metrics')
WHERE range_lower < now() - INTERVAL '2 days' LIMIT 1;
DO $$ BEGIN
    IF (SELECT count(*) FROM pgseries_victim) <> 1 THEN
        RAISE EXCEPTION 'no old chunk to compress';
    END IF;
END $$;
SELECT pgseries.compress_chunk('public', 'metrics', (SELECT victim FROM pgseries_victim)) AS compress_out;
DO $$
BEGIN
    IF (SELECT count(*) FROM (SELECT * FROM public.metrics_read EXCEPT SELECT * FROM public.metrics_pre) d) <> 0 THEN
        RAISE EXCEPTION 'read view diverged (read minus snapshot)';
    END IF;
    IF (SELECT count(*) FROM (SELECT * FROM public.metrics_pre EXCEPT SELECT * FROM public.metrics_read) d) <> 0 THEN
        RAISE EXCEPTION 'read view diverged (snapshot minus read)';
    END IF;
    IF (SELECT count(*) FROM public.metrics_read_range(now() - INTERVAL '3 days', now())) <>
       (SELECT count(*) FROM public.metrics_pre WHERE t >= now() - INTERVAL '3 days') THEN
        RAISE EXCEPTION 'read_range mismatch';
    END IF;
    -- exact equality (catches boundary-segment over-emission)
    IF (SELECT count(*) FROM (
            SELECT * FROM public.metrics_read_range(now() - INTERVAL '3 days', now())
            EXCEPT SELECT * FROM public.metrics_pre
                    WHERE t >= now() - INTERVAL '3 days' AND t <= now()) d) <> 0 THEN
        RAISE EXCEPTION 'read_range diverged (plus)';
    END IF;
    IF (SELECT count(*) FROM (
            SELECT * FROM public.metrics_pre
            WHERE t >= now() - INTERVAL '3 days' AND t <= now()
            EXCEPT SELECT * FROM public.metrics_read_range(now() - INTERVAL '3 days', now())) d) <> 0 THEN
        RAISE EXCEPTION 'read_range diverged (minus)';
    END IF;
    IF (SELECT compressed_rows FROM pgseries.compressed_chunk_stats('public', 'metrics')
        WHERE chunk_name = (SELECT victim FROM pgseries_victim)) < 1 THEN
        RAISE EXCEPTION 'stats show no compressed rows';
    END IF;
    -- read-only enforcement (fires for any role, incl. owner).
    -- A well-typed row must reach the trigger (not die on cast).
    BEGIN
        EXECUTE format('INSERT INTO %I (seg_no, seg_min_ts, seg_max_ts, '
                       || 'seg_row_count, device, c1_base, c1_arr, c1_nulls, '
                       || 'c3_arr, c3_nulls) VALUES (999, now(), now(), 1, '
                       || '''x'', 0, ''{}'', ''{}'', ''{1.0}'', ''{}'')',
            (SELECT victim FROM pgseries_victim) || '_compressed');
        RAISE EXCEPTION 'compressed write must be refused';
    EXCEPTION WHEN read_only_sql_transaction THEN NULL;
    END;
    BEGIN
        EXECUTE format('TRUNCATE %I', (SELECT victim FROM pgseries_victim) || '_compressed');
        RAISE EXCEPTION 'compressed truncate must be refused';
    EXCEPTION WHEN read_only_sql_transaction THEN NULL;
    END;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 17. decompress + recompress ---'; END $$;
SELECT pgseries.decompress_chunk('public', 'metrics', (SELECT victim FROM pgseries_victim)) AS decompress_out;
DO $$
BEGIN
    IF (SELECT state FROM pgseries.show_chunks('public', 'metrics')
        WHERE chunk_name = (SELECT victim FROM pgseries_victim)) <> 'ready' THEN
        RAISE EXCEPTION 'lease not back to ready';
    END IF;
    IF (SELECT count(*) FROM (SELECT * FROM public.metrics EXCEPT SELECT * FROM public.metrics_pre) d) <> 0 THEN
        RAISE EXCEPTION 'heap diverged after decompress';
    END IF;
END $$;
SELECT pgseries.compress_chunk('public', 'metrics', (SELECT victim FROM pgseries_victim)) AS recompress_out;
DO $$
BEGIN
    IF (SELECT count(*) FROM (SELECT * FROM public.metrics_read EXCEPT SELECT * FROM public.metrics_pre) d) <> 0 THEN
        RAISE EXCEPTION 'read view diverged after recompress';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 18b. suggest_compression ---'; END $$;
SELECT * FROM pgseries.suggest_compression('public', 'metrics', 370);
DO $$
DECLARE v_seg NAME[]; v_sql TEXT; v_ratio NUMERIC;
BEGIN
    SELECT segmentby, policy_sql, expected_ratio INTO v_seg, v_sql, v_ratio
    FROM pgseries.suggest_compression('public', 'metrics', 370);
    IF v_seg <> '{device}' THEN RAISE EXCEPTION 'suggest missed segmentby: %', v_seg; END IF;
    IF v_sql NOT LIKE '%add_compression_policy(%' THEN RAISE EXCEPTION 'no policy sql'; END IF;
    IF v_ratio IS NULL OR v_ratio < 1 THEN RAISE EXCEPTION 'bad ratio %', v_ratio; END IF;
END $$;
SELECT * FROM pgseries.bench_codecs('public', 'metrics', '{device}', 370);
DO $$
DECLARE v_raw BIGINT; v_d2 BIGINT; v_ratio NUMERIC;
BEGIN
    SELECT bytes INTO v_raw FROM pgseries.bench_codecs('public', 'metrics', '{device}', 370) WHERE name = 'raw';
    SELECT bytes, ratio_vs_heap INTO v_d2, v_ratio FROM pgseries.bench_codecs('public', 'metrics', '{device}', 370) WHERE name = 'delta';
    IF v_d2 > v_raw THEN
        RAISE EXCEPTION 'delta (%) bigger than raw (%) on regular data', v_d2, v_raw;
    END IF;
    IF v_ratio >= 1 THEN
        RAISE EXCEPTION 'delta not smaller than heap (ratio %)', v_ratio;
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 19. retention drops compressed sibling ---'; END $$;
SELECT pgseries.add_compression_policy('public', 'logs', '{msg}', 't', false, 100, '{"t": "delta"}');
INSERT INTO public.logs VALUES (now() - INTERVAL '60 days', 'oldmsg');
SELECT pgseries.move_default('public', 'logs') AS logs_moved;
CREATE TEMP TABLE pgseries_victim2 AS
SELECT chunk_name AS victim FROM pgseries.show_chunks('public', 'logs')
WHERE range_lower < now() - INTERVAL '30 days' LIMIT 1;
SELECT pgseries.compress_chunk('public', 'logs', (SELECT victim FROM pgseries_victim2)) AS logs_compress;
SELECT * FROM pgseries.drop_chunks('public', 'logs', '30 days') AS logs_dropped;
DO $$
BEGIN
    IF to_regclass(format('%I.%I', 'public',
            ((SELECT victim FROM pgseries_victim2) || '_compressed'))) IS NOT NULL THEN
        RAISE EXCEPTION 'compressed sibling survived retention';
    END IF;
    IF EXISTS (SELECT 1 FROM pgseries.chunk WHERE series_table = 'logs'
               AND chunk_name = (SELECT victim FROM pgseries_victim2)) THEN
        RAISE EXCEPTION 'catalog row survived retention';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 20. transparent base reads ---'; END $$;
DO $$
DECLARE v_base BIGINT; v_heap BIGINT; v_crows BIGINT;
BEGIN
    -- metrics has one compressed chunk (victim, recompressed in 17).
    SELECT count(*) INTO v_base FROM public.metrics;
    SELECT count(*) INTO v_heap FROM public.metrics_heap;
    SELECT COALESCE(sum(compressed_rows), 0) INTO v_crows
    FROM pgseries.compressed_chunk_stats('public', 'metrics');
    IF v_base <> v_heap + v_crows THEN
        RAISE EXCEPTION 'base % <> heap % + compressed %', v_base, v_heap, v_crows;
    END IF;
    IF v_crows < 1 THEN RAISE EXCEPTION 'victim must still be compressed'; END IF;
    IF to_regclass('public.metrics_heap') IS NULL THEN
        RAISE EXCEPTION 'heap parent missing after swap';
    END IF;
    -- _read alias kept working (covered in 16/17) and equals base.
    IF (SELECT count(*) FROM public.metrics_read) <> v_base THEN
        RAISE EXCEPTION '_read diverged from base';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 21. DML via transparent view ---'; END $$;
INSERT INTO public.metrics (t, device, v)
VALUES (now(), 'smoke21', 4242) RETURNING t, device, v;
DO $$
DECLARE v_lo TIMESTAMPTZ; v_hi TIMESTAMPTZ; v_n BIGINT;
BEGIN
    -- heap row round-trips through the view.
    UPDATE public.metrics SET v = 4243 WHERE device = 'smoke21';
    SELECT count(*) INTO v_n FROM public.metrics WHERE device = 'smoke21' AND v = 4243;
    IF v_n <> 1 THEN RAISE EXCEPTION 'view UPDATE missed heap row'; END IF;
    DELETE FROM public.metrics WHERE device = 'smoke21';
    IF EXISTS (SELECT 1 FROM public.metrics WHERE device = 'smoke21') THEN
        RAISE EXCEPTION 'view DELETE missed heap row';
    END IF;
    -- compressed rows are read-only through the view too.
    SELECT range_lower, range_upper INTO v_lo, v_hi
    FROM pgseries.show_chunks('public', 'metrics')
    WHERE chunk_name = (SELECT victim FROM pgseries_victim);
    BEGIN
        UPDATE public.metrics SET v = -1 WHERE t >= v_lo AND t < v_hi;
        RAISE EXCEPTION 'UPDATE of compressed rows must raise';
    EXCEPTION WHEN read_only_sql_transaction THEN NULL;
    END;
    BEGIN
        DELETE FROM public.metrics WHERE t >= v_lo AND t < v_hi;
        RAISE EXCEPTION 'DELETE of compressed rows must raise';
    EXCEPTION WHEN read_only_sql_transaction THEN NULL;
    END;
    -- writer keeps view privileges; heap-direct bypass works.
    IF NOT has_table_privilege('pgseries_writer', 'public.metrics', 'INSERT') THEN
        RAISE EXCEPTION 'writer must INSERT via base view';
    END IF;
    INSERT INTO public.metrics_heap (t, device, v)
    SELECT now(), 'heapdirect', s FROM generate_series(1, 10) s;
    IF (SELECT count(*) FROM public.metrics WHERE device = 'heapdirect') <> 10 THEN
        RAISE EXCEPTION 'heap-direct rows not visible via base';
    END IF;
    DELETE FROM public.metrics_heap WHERE device = 'heapdirect';
END $$;

DO $$ BEGIN RAISE NOTICE '--- 22. compress_table set-based ---'; END $$;
CREATE TABLE public.bulk (t TIMESTAMPTZ NOT NULL, device TEXT, v DOUBLE PRECISION);
INSERT INTO public.bulk
SELECT now() - '44 days'::INTERVAL - (s || ' minutes')::INTERVAL,
       'd' || (s % 2), s FROM generate_series(1, 5000) AS s;
SELECT pgseries.create_series_table('public', 'bulk', 't', '1 day');
SELECT pgseries.precreate_chunks('public', 'bulk', 3);
SELECT pgseries.move_default('public', 'bulk') AS bulk_moved;
SELECT pgseries.add_compression_policy('public', 'bulk', '{device}', 't', false, 1000, '{"t": "delta"}');
DO $$
BEGIN
    IF (SELECT count(*) FROM public.bulk) <> 5000 THEN
        RAISE EXCEPTION 'base must read 5000 pre-compress, got %',
            (SELECT count(*) FROM public.bulk);
    END IF;
END $$;
SELECT pgseries.compress_table('public', 'bulk') AS compress_all;
DO $$
DECLARE v_comp BIGINT; v_ready BIGINT; v_base BIGINT; v_h BIGINT; v_txt TEXT;
BEGIN
    SELECT count(*) INTO v_comp FROM pgseries.show_chunks('public', 'bulk')
    WHERE state = 'compressed';
    SELECT count(*) INTO v_ready FROM pgseries.show_chunks('public', 'bulk')
    WHERE state = 'ready';
    SELECT count(*) INTO v_base FROM public.bulk;
    SELECT count(*) INTO v_h FROM public.bulk_heap;
    IF v_comp < 3 THEN RAISE EXCEPTION 'want >=3 compressed, got %', v_comp; END IF;
    IF v_base <> 5000 THEN RAISE EXCEPTION 'base lost rows: %', v_base; END IF;
    IF v_h <> 0 THEN RAISE EXCEPTION 'heaps not drained: %', v_h; END IF;
    IF v_ready < 3 THEN RAISE EXCEPTION 'precreated hot chunks must stay ready, got %', v_ready; END IF;
    -- hot chunk excluded by default: fresh row + rerun compresses nothing.
    INSERT INTO public.bulk (t, device, v) VALUES (now(), 'hot', 1);
    IF (SELECT pgseries.compress_table('public', 'bulk')) <> 'nothing eligible to compress' THEN
        RAISE EXCEPTION 'hot-only rerun must be a no-op';
    END IF;
    SELECT pgseries.compress_table('public', 'bulk', NULL, true) INTO v_txt;
    IF (SELECT count(*) FROM public.bulk) <> 5001 THEN
        RAISE EXCEPTION 'hot include lost rows';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 23. stats set-based totals ---'; END $$;
DO $$
DECLARE v_sum BIGINT;
BEGIN
    SELECT COALESCE(sum(compressed_rows), 0) + COALESCE(sum(heap_rows), 0)
    INTO v_sum FROM pgseries.compressed_chunk_stats('public', 'bulk');
    IF v_sum <> 5001 THEN RAISE EXCEPTION 'stats total %, want 5001', v_sum; END IF;
    IF NOT EXISTS (SELECT 1 FROM pgseries.compressed_chunk_stats('public', 'bulk')
                   WHERE segments > 0 AND compressed_bytes > 0) THEN
        RAISE EXCEPTION 'stats show no compressed segments';
    END IF;
END $$;

DO $$ BEGIN RAISE NOTICE '--- 24. drop policy restores plain table ---'; END $$;
DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT chunk_name FROM pgseries.show_chunks('public', 'bulk')
             WHERE state = 'compressed' LOOP
        PERFORM pgseries.decompress_chunk('public', 'bulk', r.chunk_name);
    END LOOP;
END $$;
SELECT pgseries.drop_compression_policy('public', 'bulk');
DO $$
DECLARE v_kind CHAR;
BEGIN
    SELECT relkind INTO v_kind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relname = 'bulk';
    IF v_kind <> 'p' THEN RAISE EXCEPTION 'base must be a partitioned table again, got %', v_kind; END IF;
    IF to_regclass('public.bulk_heap') IS NOT NULL THEN RAISE EXCEPTION 'heap leftover'; END IF;
    IF to_regclass('public.bulk_read') IS NOT NULL THEN RAISE EXCEPTION '_read leftover'; END IF;
    IF (SELECT count(*) FROM public.bulk) <> 5001 THEN
        RAISE EXCEPTION 'teardown lost rows';
    END IF;
END $$;

SELECT 'SMOKE OK' AS result;