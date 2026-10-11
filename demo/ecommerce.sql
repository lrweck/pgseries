-- PgSeries demo: e-commerce orders (non-IoT), end to end.
-- Installs nothing (assumes pgseries.sql already loaded), builds a
-- 200K-row business table and walks: series table -> mover ->
-- space dimension -> compression policy -> set-based compress ->
-- transparent reads. ~1 minute on local NVMe.
--
-- Usage: psql -v ON_ERROR_STOP=1 -f demo/ecommerce.sql
\timing on
\pset footer off

DO $$ BEGIN RAISE NOTICE '--- 1. 1M pedidos, 180 dias, 11 colunas ---'; END $$;
DROP VIEW IF EXISTS public.pedidos_demo CASCADE;
DROP TABLE IF EXISTS public.pedidos_demo CASCADE;
CREATE TABLE public.pedidos_demo (
  ts        TIMESTAMPTZ NOT NULL,
  pedido    BIGINT NOT NULL,
  loja      TEXT NOT NULL,
  categoria TEXT NOT NULL,
  cliente   INT NOT NULL,
  valor     NUMERIC(10,2) NOT NULL,
  desconto  REAL,
  pago      BOOLEAN NOT NULL DEFAULT false,
  obs       TEXT,
  PRIMARY KEY (ts, loja)
);
INSERT INTO public.pedidos_demo
SELECT now() - '180 days'::INTERVAL + (s || ' seconds')::INTERVAL * 15.552,
       7000000 + s,
       'loja-' || (1 + s % 8),
       (ARRAY['eletro','moda','casa','esporte','livros','toys',
              'beleza','doces','moveis','jardim','auto','pet'])[1 + s % 12],
       1 + (abs(hashtextextended(s::text, 1)) % 30000),
       ((abs(hashtextextended(s::text, 2)) % 200000) / 100.0)::NUMERIC(10,2),
       (s % 4) * 2.5,
       (s % 5) <> 0,
       'entrega-' || s || '-' || md5(s::text)
FROM generate_series(1, 1000000) s;
SELECT count(*) AS linhas FROM public.pedidos_demo;

DO $$ BEGIN RAISE NOTICE '--- 2. series table + mover (tudo cai no DEFAULT) ---'; END $$;
SELECT pgseries.create_series_table('public', 'pedidos_demo', 'ts', '7 days');
SELECT pgseries.move_default('public', 'pedidos_demo') AS movidas;
SELECT count(*) AS chunks,
       count(*) FILTER (WHERE space_index <> 0) AS com_espaco
FROM pgseries.show_chunks('public', 'pedidos_demo');
SELECT pg_size_pretty(sum(pg_total_relation_size(oid))) AS heap_antes
FROM pg_class WHERE relname LIKE 'pedidos_demo_p%' AND relkind IN ('r','p');

DO $$ BEGIN RAISE NOTICE '--- 3. add_dimension: 8 lojas, fan-out in place ---'; END $$;
SELECT pgseries.add_dimension('public', 'pedidos_demo', 'loja', 'space_hash', 8);
SELECT count(*) AS folhas FROM pgseries.show_chunks('public', 'pedidos_demo');
SELECT count(*) AS total FROM public.pedidos_demo;
SELECT chunk_name, space_index, state
FROM pgseries.show_chunks('public', 'pedidos_demo') LIMIT 4;

DO $$ BEGIN RAISE NOTICE '--- 4. policy (auto + guard escolhem sozinhos) ---'; END $$;
SELECT pgseries.add_compression_policy(
  'public', 'pedidos_demo', '{categoria}', 'ts', false, 1000, '{}');
SELECT codecs FROM pgseries.compression_policy
WHERE series_table = 'pedidos_demo';

DO $$ BEGIN RAISE NOTICE '--- 5. compress_table: tudo num statement ---'; END $$;
SELECT pgseries.compress_table('public', 'pedidos_demo', NULL, true);
-- O guard de mínimo pulou 1 chunk minúsculo (<1000 linhas): fica no
-- heap, visível na leitura do mesmo jeito. Total fecha em 200k.
SELECT count(*) AS via_base FROM public.pedidos_demo;
SELECT pg_size_pretty(sum(pg_total_relation_size(oid))) AS comprimido
FROM pg_class WHERE relname LIKE '%pedidos_demo%compressed%' AND relkind IN ('r','p');

DO $$ BEGIN RAISE NOTICE '--- 6. leitura transparente + poda por segmento ---'; END $$;
SELECT count(*) AS via_base FROM public.pedidos_demo;
SELECT categoria, count(*) AS n
FROM public.pedidos_demo
WHERE ts >= now() - '30 days'::INTERVAL
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
SELECT count(*) AS ultimos_7d
FROM public.pedidos_demo_read_range(now() - '7 days'::INTERVAL, now());

DO $$ BEGIN RAISE NOTICE '--- 7. resumo ---'; END $$;
SELECT state, count(*) AS folhas,
       pg_size_pretty(sum(compressed_bytes)) AS bytes_comp,
       sum(heap_rows) AS linhas_heap
FROM pgseries.compressed_chunk_stats('public', 'pedidos_demo')
GROUP BY 1 ORDER BY 1;
SELECT 'DEMO OK' AS result;
