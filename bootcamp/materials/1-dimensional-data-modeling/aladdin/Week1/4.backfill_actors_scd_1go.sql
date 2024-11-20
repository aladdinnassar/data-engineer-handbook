---------------------------------------------------------------------------------
-- DROP TABLE public.actors_history_scd;
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
---------------------------------------------------------------------------------
-- DROP FUNCTION calc_token;
CREATE FUNCTION calc_token(boolean, quality_class_enum) RETURNS text
	AS
	$BODY$
	SELECT
		CONCAT(
			CASE
				WHEN $1 IS NULL THEN 'F'
				WHEN $1 THEN 'T'
				ELSE 'F'
			END,
			':',
			COALESCE(CAST($2 AS TEXT), 'none')
		)
	$BODY$
	LANGUAGE SQL;
	
SELECT calc_token(false, NULL);
SELECT calc_token(true, 'bad'::quality_class_enum);
---------------------------------------------------------------------------------
-- DROP TYPE public.scd_type;
CREATE TYPE public.scd_type AS
(
	streak integer,
	token text,
	is_active boolean,
	quality_class quality_class_enum,
	start_date integer,
	end_date integer
);
---------------------------------------------------------------------------------
-- DROP FUNCTION generate_actors_scd_1go;
CREATE OR REPLACE FUNCTION generate_actors_scd_1go (
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
				calc_token(x.prev_is_active, x.prev_qc) AS prev,
				calc_token(x.is_active, x.qc) AS curr
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
-- DROP PROCEDURE backfill_actors_scd_1go;
CREATE PROCEDURE backfill_actors_scd_1go(select_year integer)
LANGUAGE SQL
AS $$
	DELETE FROM actors_history_scd where asofyear = select_year;
	INSERT INTO actors_history_scd (
		actor,
		actorid,
		asofyear,
		streak,
		start_date,
		end_date,
		quality_class,
		is_active
	) SELECT * FROM generate_actors_scd_1go(select_year);
$$;
---------------------------------------------------------------------------------
CALL backfill_actors_scd_1go(2021); -- Output = 119,337 rows for asofyear = 2021
---------------------------------------------------------------------------------
DO $$
BEGIN
	DELETE FROM actors_history_scd;  

	FOR year IN 1970..2021 LOOP
		CALL backfill_actors_scd_1go(year);
		RAISE NOTICE 'year = %', year;
	END LOOP;
END;
$$;

SELECT * FROM actors_history_scd; -- Output = 2,238,682 rows for all years
---------------------------------------------------------------------------------
SELECT
	*
FROM actors_history_scd
WHERE actor = 'Charlie Murphy'
	AND asofyear = 2021
ORDER BY actor, start_date;
---------------------------------------------------------------------------------
-- There are 2 x actors with the same name = Charlie Murphy
SELECT
	*
FROM actor_films
WHERE actor = 'Charlie Murphy'
---------------------------------------------------------------------------------






  



