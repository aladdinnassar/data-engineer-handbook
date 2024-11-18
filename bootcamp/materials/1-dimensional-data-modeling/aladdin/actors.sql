/*
---------------------------------------------------------------------------------
DROP TYPE public.quality_class;
CREATE TYPE public.quality_class_enum AS ENUM
    ('star', 'good', 'average', 'bad');
ALTER TYPE public.quality_class_enum
    OWNER TO postgres;
---------------------------------------------------------------------------------
DROP TYPE public.film_struct;
CREATE TYPE public.film_struct AS
(
	film text,
	year integer,
	votes integer,
	rating real,
	filmid text
);
ALTER TYPE public.film_struct
    OWNER TO postgres;
---------------------------------------------------------------------------------
DROP TABLE public.actors;
CREATE TABLE public.actors
(
    actor text COLLATE pg_catalog."default",
    actorid text COLLATE pg_catalog."default" NOT NULL,
	asofyear integer,
    films film_struct[],
    nfilms bigint,
    sum_rating real,
	is_active boolean,
    CONSTRAINT actors_pkey PRIMARY KEY (actorid, asofyear)
)

ALTER TABLE public.actors
    OWNER to postgres;
---------------------------------------------------------------------------------
DROP FUNCTION ARRAY_SORT
CREATE OR REPLACE FUNCTION ARRAY_SORT(ANYARRAY)
RETURNS ANYARRAY
LANGUAGE SQL
AS $$
SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$$;
---------------------------------------------------------------------------------
-- MIN(year) = 1970, MAX(year) = 2021
SELECT
	MIN(year),
	MAX(year)
FROM public.actor_films
---------------------------------------------------------------------------------
-- SELECT
-- 	MAX(actor) AS actor,
-- 	actorid,
-- 	ARRAY_AGG(
-- 		ROW(film, year, votes, rating, filmid)::film_struct
-- 		ORDER BY year
-- 	) AS films,
-- 	AVG(rating) AS avg_rating,
-- 	CASE
--  		WHEN AVG(rating) > 8 THEN 'star'
--  		WHEN AVG(rating) > 7 THEN 'good'
--  		WHEN AVG(rating) > 6 THEN 'average'
--  		ELSE 'bad'
--  	END::quality_class_enum AS quality_class,
-- 	COUNT(1) AS films,
-- 	MIN(year) AS min_year,
-- 	MAX(year) AS max_year,
-- 	(MAX(year) = DATE_PART('year', CURRENT_DATE)) AS is_active
-- FROM public.actor_films
-- GROUP BY actorid
-- ORDER BY MAX(year) DESC, MIN(year); 
---------------------------------------------------------------------------------
DROP FUNCTION generate_yearly_actors;
CREATE OR REPLACE FUNCTION generate_yearly_actors (
  select_year integer
)
RETURNS TABLE (
	actor text,
	actorid text,
	asofyear integer,
	films film_struct[],
	nfilms bigint,
	sum_rating real,
	is_active boolean
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
	return query
		SELECT
			MAX(actor) AS actor,
			actorid,
			select_year AS asofyear,
			ARRAY_AGG(
				ROW(film, year, votes, rating, filmid)::film_struct
				ORDER BY year
			) AS films,
			COUNT(1) AS nfilms,
			SUM(rating) AS sum_rating,
			true AS is_active
		FROM public.actor_films
		WHERE year = select_year
		GROUP BY actorid;
END;
$$;
---------------------------------------------------------------------------------
SELECT * FROM generate_yearly_actors(1970);
---------------------------------------------------------------------------------
-- DELETE FROM actors;
-- INSERT INTO actors (
-- 	actor,
-- 	actorid,
-- 	asofyear,
-- 	films,
-- 	nfilms,
-- 	sum_rating,
-- 	is_active
-- )
-- SELECT
-- 	*
-- FROM generate_yearly_actors(1970);
---------------------------------------------------------------------------------
SELECT * FROM actors
ORDER BY 1
---------------------------------------------------------------------------------
DROP FUNCTION generate_cumul_actors;
CREATE OR REPLACE FUNCTION generate_cumul_actors (
  select_year integer
)
RETURNS TABLE (
	actor text,
	actorid text,
	asofyear integer,
	films film_struct[],
	nfilms bigint,
	sum_rating real,
	is_active boolean
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
	return query
	SELECT
		COALESCE(y.actor, t.actor) AS actor,
		COALESCE(y.actorid, t.actorid) AS actorid,
		select_year AS asofyear,
		CASE
			WHEN y.actorid IS NULL THEN t.films
			WHEN t.actorid IS NULL THEN y.films
			ELSE ARRAY_SORT(y.films || t.films)
		END::film_struct[] AS films,
		COALESCE(y.nfilms, 0) + COALESCE(t.nfilms, 0) AS nfilms,
		COALESCE(y.sum_rating, 0) + COALESCE(t.sum_rating, 0) AS sum_rating,
		COALESCE(t.is_active, false) AS is_active
	FROM (
		SELECT
			*
		FROM actors
		WHERE asofyear = select_year - 1
	) y
	FULL OUTER JOIN (
		SELECT * FROM generate_yearly_actors(select_year)
	) t
	ON y.actorid = t.actorid;	
END;
$$;
---------------------------------------------------------------------------------
-- DROP PROCEDURE insert_cumul_actors;
CREATE PROCEDURE insert_cumul_actors(select_year integer)
LANGUAGE SQL
AS $$
	DELETE FROM actors where asofyear = select_year;
	INSERT INTO actors (
		actor,
		actorid,
		asofyear,
		films,
		nfilms,
		sum_rating,
		is_active
	) SELECT * FROM generate_cumul_actors(select_year);
$$;
---------------------------------------------------------------------------------
DROP PROCEDURE loop_cumul_actors;
CREATE PROCEDURE loop_cumul_actors()
LANGUAGE SQL
AS $$
$$;

DO $$
BEGIN
  DELETE FROM actors;  

  FOR year IN 1970..2021 LOOP
  	CALL insert_cumul_actors(year);
  	RAISE NOTICE 'year = %', year;
  END LOOP;
  
  -- SELECT * FROM actors ORDER BY actor, asofyear;
END;
$$;
---------------------------------------------------------------------------------
-- SELECT * FROM generate_cumul_actors(1970);
-- SELECT * FROM actors;
---------------------------------------------------------------------------------
-- CALL loop_cumul_actors();
-- SELECT * FROM actors ORDER BY actor, asofyear;

-- DELETE FROM films;
-- CALL insert_cumul_actors(1970);
-- CALL insert_cumul_actors(1971);
-- CALL insert_cumul_actors(1972);
-- SELECT * FROM actors ORDER BY actor, asofyear;
---------------------------------------------------------------------------------
*/

DO $$
BEGIN
	DELETE FROM actors;  

	FOR year IN 1970..2021 LOOP
		CALL insert_cumul_actors(year);
		RAISE NOTICE 'year = %', year;
	END LOOP;
END;
$$;

SELECT * FROM actors ORDER BY actor, asofyear;
  



