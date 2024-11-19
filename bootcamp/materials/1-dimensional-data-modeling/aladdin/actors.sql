/*
---------------------------------------------------------------------------------
DROP TYPE public.quality_class_enum;
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
    films film_struct[],				-- cumulative since 1970
    nfilms bigint,						-- cumulative since 1970
	avg_rating real,					-- for asofyear only
	quality_class quality_class_enum, 	-- for asofyear only
	is_active boolean, 					-- for asofyear only
    CONSTRAINT actors_pkey PRIMARY KEY (actorid, asofyear)
);
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
DROP FUNCTION calc_quality_class;
CREATE FUNCTION calc_quality_class(real) RETURNS quality_class_enum
	AS
	$BODY$
	SELECT
		CASE
			WHEN $1 > 8 THEN 'star'
			WHEN $1 > 7 THEN 'good'
			WHEN $1 > 6 THEN 'average'
			ELSE 'bad'
		END::quality_class_enum;
	$BODY$
	LANGUAGE SQL;
	
SELECT calc_quality_class(5.0);
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
	avg_rating real,
	quality_class quality_class_enum,
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
			CAST(AVG(rating) AS real) AS avg_rating,
			calc_quality_class(CAST(AVG(rating) AS real)) AS quality_class,
			true AS is_active
		FROM public.actor_films
		WHERE year = select_year
		GROUP BY actorid;
END;
$$;
---------------------------------------------------------------------------------
SELECT * FROM generate_yearly_actors(1970);
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
	avg_rating real,
	quality_class quality_class_enum,
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
		END::film_struct[] AS films,								-- Sorted list of films since 1970
		COALESCE(y.nfilms, 0) + COALESCE(t.nfilms, 0) AS nfilms,	-- Cumulative count of films since 1970
		t.avg_rating AS avg_rating,									-- NULL if not active
		t.quality_class,											-- NULL if not active
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
	ON y.actorid = t.actorid
	;	
END;
$$;
---------------------------------------------------------------------------------
DROP PROCEDURE insert_cumul_actors;
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
		avg_rating,
		quality_class,
		is_active
	) SELECT * FROM generate_cumul_actors(select_year);
$$;
---------------------------------------------------------------------------------
SELECT * FROM generate_cumul_actors(1970);
CALL insert_cumul_actors(1970);
CALL insert_cumul_actors(1971);
CALL insert_cumul_actors(1972);
SELECT * FROM actors ORDER BY actor, asofyear;
---------------------------------------------------------------------------------
-- DROP PROCEDURE loop_cumul_actors;
-- CREATE PROCEDURE loop_cumul_actors()
-- LANGUAGE SQL
-- AS $$
-- $$;

-- DO $$
-- BEGIN
--   DELETE FROM actors;  

--   FOR year IN 1970..2021 LOOP
--   	CALL insert_cumul_actors(year);
--   	RAISE NOTICE 'year = %', year;
--   END LOOP;
  
--   -- SELECT * FROM actors ORDER BY actor, asofyear;
-- END;
-- $$;
---------------------------------------------------------------------------------
-- SELECT * FROM generate_cumul_actors(1970);
-- SELECT * FROM actors;
---------------------------------------------------------------------------------
-- CALL loop_cumul_actors();
-- SELECT * FROM actors ORDER BY actor, asofyear;

-- DELETE FROM actors;
-- CALL insert_cumul_actors(1970);
-- CALL insert_cumul_actors(1971);
-- CALL insert_cumul_actors(1972);
-- SELECT * FROM actors ORDER BY actor, asofyear;
---------------------------------------------------------------------------------
DO $$
BEGIN
	DELETE FROM actors;  

	FOR year IN 1970..2021 LOOP
		CALL insert_cumul_actors(year);
		RAISE NOTICE 'year = %', year;
	END LOOP;
END;
$$;

SELECT * FROM actors ORDER BY actor, asofyear;  -- results = 249082 rows
---------------------------------------------------------------------------------
DROP TABLE public.actors_history_scd;
CREATE TABLE public.actors_history_scd
(
    actor text COLLATE pg_catalog."default",
    actorid text COLLATE pg_catalog."default" NOT NULL,
	asofyear integer,
	streak integer,
    start_date integer, 
	end_date integer,
	quality_class quality_class_enum,
	is_active boolean,
    CONSTRAINT actors_history_scd_pkey PRIMARY KEY (actorid, asofyear, start_date)
);
ALTER TABLE public.actors_history_scd
    OWNER to postgres;
---------------------------------------------------------------------------------
SELECT * FROM actors LIMIT 30;
SELECT * FROM actor_films ORDER BY actor, year LIMIT 30;
---------------------------------------------------------------------------------
DROP FUNCTION generate_cumul_actors_scd;
CREATE OR REPLACE FUNCTION generate_cumul_actors_scd (
  thru_year integer
)
RETURNS TABLE (
	actor text,
	actorid text,
	asofyear integer,
	streak bigint,
	start_date integer,
	end_date integer,
	quality_class quality_class_enum,
	is_active boolean
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
	return query
	
	WITH xview AS (
		SELECT
			*,
			CASE
				WHEN z.prev != z.curr THEN 1 
				ELSE 0	
			END AS change
		FROM (
			SELECT
				*,
				CONCAT(
					CASE
						WHEN x.prev_is_active IS NULL THEN 'F'
						WHEN x.prev_is_active THEN 'T'
						ELSE 'F'
					END,
					':',
					COALESCE(CAST(x.prev_qc AS TEXT), 'none')
				) AS prev,

				CONCAT(
					CASE
						WHEN x.is_active IS NULL THEN 'F'
						WHEN x.is_active THEN 'T'
						ELSE 'F'
					END,
					':',
					COALESCE(CAST(x.qc AS TEXT), 'none')
				) AS curr
			FROM (
				SELECT
					actor,
					actorid,
					asofyear,

					LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY asofyear) AS prev_qc,
					LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY asofyear) AS prev_is_active,

					quality_class AS qc,
					is_active
				FROM actors
				WHERE asofyear <= thru_year
			) x
		) z
	),
	streaks AS (
		SELECT
			actor,
			actorid,
			asofyear,
			is_active,
			qc,
			prev,
			curr,
			change,
			SUM(change) OVER (PARTITION BY actorid ORDER BY asofyear) AS streak
		FROM xview
	),
	changes AS (
		-- Grain = actor, streak
		SELECT
			actorid,
			streak,
			qc,
			is_active
		FROM streaks
		WHERE change = 1
	)

	SELECT
		s.*,
		c.qc AS quality_class,
		c.is_active
	FROM (
		SELECT
			MAX(actor) AS actor,
			actorid,
			thru_year AS asofyear,

			streak,
			MIN(asofyear) AS start_date,
			MAX(asofyear) AS end_date
		FROM streaks s
		GROUP BY actorid, streak
	) s
	JOIN changes c
	ON s.actorid = c.actorid
		AND s.streak = c.streak
	ORDER BY actor, streak;
	END;
$$;
---------------------------------------------------------------------------------
SELECT * FROM generate_cumul_actors_scd(2021);
---------------------------------------------------------------------------------
DROP PROCEDURE insert_cumul_actors_scd;
CREATE PROCEDURE insert_cumul_actors_scd(thru_year integer)
LANGUAGE SQL
AS $$
	DELETE FROM actors_history_scd where asofyear = thru_year;
	INSERT INTO actors_history_scd (
		actor,
		actorid,
		asofyear,
		streak,
		start_date,
		end_date,
		quality_class,
		is_active
	) SELECT * FROM generate_cumul_actors_scd(thru_year);
$$;
---------------------------------------------------------------------------------
*/
CALL insert_cumul_actors_scd(2021);
SELECT * FROM actors_history_scd
ORDER BY actor, asofyear, streak;
---------------------------------------------------------------------------------

  



