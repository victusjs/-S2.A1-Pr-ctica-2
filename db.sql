\set ON_ERROR_STOP 1
\pset pager off

-- === 0) Ruta del CSV (EDITA esta línea) ===
\set csv 'C:/Users/joshr/Desktop/main/FilmTV_USAMoviesClean.csv'

-- (Opcional) Comprobar que psql ve el archivo:
-- \! dir ":csv"

-- === 1) Crear BD etl si no existe (desde 'postgres') ===
SELECT format('CREATE DATABASE %I', 'etl')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='etl')\gexec

\connect etl

-- === 2) Crear tabla con CHECKs ===
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.film_tv_usa_movies_clean (
  "color" TEXT,
  "director_name" TEXT,
  "num_critic_for_reviews" DOUBLE PRECISION,
  "duration" DOUBLE PRECISION,
  "director_facebook_likes" DOUBLE PRECISION,
  "actor_3_facebook_likes" DOUBLE PRECISION,
  "actor_2_name" TEXT,
  "actor_1_facebook_likes" DOUBLE PRECISION,
  "gross" DOUBLE PRECISION,
  "genres" TEXT,
  "actor_1_name" TEXT,
  "movie_title" TEXT,
  "num_voted_users" BIGINT,
  "cast_total_facebook_likes" BIGINT,
  "actor_3_name" TEXT,
  "facenumber_in_poster" BIGINT,
  "plot_keywords" TEXT,
  "movie_imdb_link" TEXT,
  "num_user_for_reviews" DOUBLE PRECISION,
  "language" TEXT,
  "country" TEXT,
  "content_rating" TEXT,
  "budget" DOUBLE PRECISION,
  "title_year" BIGINT,
  "actor_2_facebook_likes" DOUBLE PRECISION,
  "imdb_score" DOUBLE PRECISION,
  "aspect_ratio" DOUBLE PRECISION,
  "movie_facebook_likes" BIGINT,
  "TittleCode" TEXT,
  CHECK (facenumber_in_poster >= 0),
  CHECK (title_year >= 0),
  CHECK ("TittleCode" IS NULL OR "TittleCode" ~ '^tt[0-9]+$')
);

-- === 3) Cargar CSV (usa \copy: lee desde tu PC) ===
TRUNCATE TABLE public.film_tv_usa_movies_clean;

-- Si el orden de columnas en el CSV es el mismo que la tabla, podemos omitir la lista:
\copy public.film_tv_usa_movies_clean FROM :'csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- (Opcional) si quieres forzar columnas exactamente:
-- \set cols '"color","director_name","num_critic_for_reviews","duration","director_facebook_likes","actor_3_facebook_likes","actor_2_name","actor_1_facebook_likes","gross","genres","actor_1_name","movie_title","num_voted_users","cast_total_facebook_likes","actor_3_name","facenumber_in_poster","plot_keywords","movie_imdb_link","num_user_for_reviews","language","country","content_rating","budget","title_year","actor_2_facebook_likes","imdb_score","aspect_ratio","movie_facebook_likes","TittleCode"'
-- \copy public.film_tv_usa_movies_clean (:cols) FROM :'csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- === 4) Índices útiles ===
CREATE INDEX IF NOT EXISTS ix_film_tv_usa_movies_clean_tittlecode ON public.film_tv_usa_movies_clean ("TittleCode");
CREATE INDEX IF NOT EXISTS ix_film_tv_usa_movies_clean_country    ON public.film_tv_usa_movies_clean ("country");

-- === 5) Verificaciones rápidas (deben verse con datos reales) ===
\echo 'Filas cargadas:'
SELECT COUNT(*) FROM public.film_tv_usa_movies_clean;

\echo 'Países y conteo (debe salir solo USA):'
SELECT country, COUNT(*) FROM public.film_tv_usa_movies_clean GROUP BY country;

\echo 'Reglas (esperado = 0,0; min >= 0; regex válido):'
-- gross sin nulos
SELECT COUNT(*) AS n_nulls_gross
FROM public.film_tv_usa_movies_clean
WHERE gross IS NULL;

-- facenumber_in_poster sin nulos y sin negativos
SELECT
  SUM((facenumber_in_poster IS NULL)::int) AS n_nulls_faces,
  MIN(facenumber_in_poster)                AS min_faces,
  SUM((facenumber_in_poster < 0)::int)     AS n_negativos
FROM public.film_tv_usa_movies_clean;

-- TittleCode válido
SELECT COUNT(*) AS n_invalid_tittlecode
FROM public.film_tv_usa_movies_clean
WHERE "TittleCode" IS NULL OR "TittleCode" !~ '^tt[0-9]+$';

-- title_year sin nulos y >= 0
SELECT
  SUM((title_year IS NULL)::int) AS n_nulls_year,
  MIN(title_year)                AS min_year
FROM public.film_tv_usa_movies_clean;
