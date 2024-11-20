---------------------------------------------------------------------------------
-- DROP TABLE public.actors_history_scd_inc;
CREATE TABLE public.actors_history_scd_inc
(
    actor text COLLATE pg_catalog."default",
    actorid text COLLATE pg_catalog."default" NOT NULL,
	asofyear integer,
	streak integer,
	source text COLLATE pg_catalog."default",
    start_date integer, 
	end_date integer,
	quality_class quality_class_enum,
	is_active boolean,
    CONSTRAINT actors_history_scd_inc_pkey PRIMARY KEY (actorid, asofyear, start_date)
);
---------------------------------------------------------------------------------
-- DROP PROCEDURE prime_actors_scd_inc;
CREATE PROCEDURE prime_actors_scd_inc(select_year integer)
LANGUAGE SQL
AS $$
	DELETE FROM actors_history_scd_inc where asofyear = select_year;
	INSERT INTO actors_history_scd_inc (
		actor,
		actorid,
		asofyear,
		streak,
		source,
		start_date,
		end_date,
		quality_class,
		is_active
	) SELECT
		actor,
		actorid,
		asofyear,
		streak,
		'prime' AS source,
		start_date,
		end_date,
		quality_class,
		is_active
	FROM generate_actors_scd_1go(select_year);
$$;
---------------------------------------------------------------------------------
-- DELETE FROM actors_history_scd_inc;
-- CALL prime_actors_scd_inc(1970);
-- CALL prime_actors_scd_inc(1971);
-- CALL prime_actors_scd_inc(1972);

DO $$
BEGIN
	DELETE FROM actors_history_scd_inc;  

	FOR year IN 1970..1980 LOOP
		CALL prime_actors_scd_inc(year);
		RAISE NOTICE 'year = %', year;
	END LOOP;
END;
$$;

SELECT * FROM actors_history_scd_inc
WHERE asofyear = 1980
ORDER BY actor, asofyear, streak;
---------------------------------------------------------------------------------
-- DROP FUNCTION increment_actors_scd;
CREATE OR REPLACE FUNCTION increment_actors_scd (
  select_year integer
)
RETURNS TABLE (
	actor text,
	actorid text,
	asofyear integer,
	streak integer,
	source text,
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
	
	WITH last_year_scd AS (
		SELECT
			*,
			calc_token(is_active, quality_class) AS token
		FROM actors_history_scd_inc
		WHERE asofyear = select_year - 1
			AND end_date = select_year - 1
	),
	historical AS (
		SELECT
			*,
			calc_token(is_active, quality_class) AS token
		FROM actors_history_scd_inc
		WHERE asofyear = select_year - 1
			AND end_date < select_year - 1
	),
	this_year AS (
		SELECT
			*,
			calc_token(is_active, quality_class) AS token
		FROM actors
		WHERE asofyear = select_year
	),
	extended AS (
		SELECT
			ly.actor,
			ly.actorid,
			select_year AS asofyear,
			ly.streak,
			ly.start_date,
			select_year AS end_date,
			ly.quality_class,
			ly.is_active
		FROM this_year ty
		JOIN last_year_scd ly
		ON ty.actorid = ly.actorid
		WHERE ty.token = ly.token
	),
	new_rows AS (
		SELECT
			ty.actor,
			ty.actorid,
			select_year AS asofyear,
			1 AS streak,
			select_year AS start_date,
			select_year AS end_date,
			ty.quality_class,
			ty.is_active
		FROM this_year ty
		LEFT JOIN last_year_scd ly
		ON ty.actorid = ly.actorid
		WHERE ly.actorid IS NULL
	),
	changed AS (
		SELECT
			ly.actor,
			ly.actorid,
			ly.streak,
			UNNEST(
				ARRAY[
					ROW(
						ly.streak,
						ly.token,
						ly.is_active,
						ly.quality_class,
						ly.start_date,
						ly.end_date
					)::scd_type,
					ROW(
						ly.streak+1,
						ty.token,
						ty.is_active,
						ty.quality_class,
						select_year,
						select_year
					)::scd_type
				]
			) AS x
		FROM this_year ty
		JOIN last_year_scd ly
		ON ty.actorid = ly.actorid
		WHERE ty.token != ly.token
	)

	SELECT
		actor,
		actorid,
		select_year AS asofyear,
		(x::scd_type).streak,
		'changed' AS source,
		-- (x::scd_type).token,
		(x::scd_type).start_date,
		(x::scd_type).end_date,
		(x::scd_type).quality_class,
		(x::scd_type).is_active
	FROM changed
	
	UNION ALL
	
	SELECT		
		actor,
		actorid,
		select_year AS asofyear,
		streak,
		'new' AS source,
		start_date,
		end_date,
		quality_class,
		is_active
	FROM new_rows
	
	UNION ALL
	
	SELECT
		actor,
		actorid,
		select_year AS asofyear,
		streak,
		'extended' AS source,
		start_date,
		end_date,
		quality_class,
		is_active
	FROM extended
	
	UNION ALL
	
	SELECT
		actor,
		actorid,
		select_year AS asofyear,
		streak,
		'historical' AS source,
		start_date,
		end_date,
		quality_class,
		is_active
	FROM historical;

	END;
$$;
---------------------------------------------------------------------------------
-- DROP PROCEDURE insert_actors_scd_inc;
CREATE PROCEDURE insert_actors_scd_inc(select_year integer)
LANGUAGE SQL
AS $$
	DELETE FROM actors_history_scd_inc where asofyear = select_year;
	INSERT INTO actors_history_scd_inc (
		actor,
		actorid,
		asofyear,
		streak,
		source,
		start_date,
		end_date,
		quality_class,
		is_active
	) SELECT
		actor,
		actorid,
		asofyear,
		streak,
		source,
		start_date,
		end_date,
		quality_class,
		is_active
	FROM increment_actors_scd(select_year);
$$;
---------------------------------------------------------------------------------
DO $$
BEGIN
	-- DELETE FROM actors_history_scd_inc;  

	FOR year IN 1981..2021 LOOP
		CALL insert_actors_scd_inc(year);
		RAISE NOTICE 'year = %', year;
	END LOOP;
END;
$$;
---------------------------------------------------------------------------------
WITH cte AS (
	SELECT * FROM increment_actors_scd(2021)
	WHERE asofyear = 2021
)

SELECT * FROM actors_history_scd_inc
WHERE asofyear = 2021
	AND actor = 'Charlie Murphy'
	AND actorid = 'nm0614151'
ORDER BY actor, streak;

-- SELECT * FROM cte ORDER BY actor, streak;

SELECT
	*,
	CARDINALITY(x.sources) AS nsources
FROM (
	SELECT
		actorid,
		MAX(actor) AS actor,
		COUNT(1) AS records,
		ARRAY_SORT(ARRAY_AGG(DISTINCT source)) AS sources
	FROM cte
	GROUP BY 1
) x
ORDER BY 5 DESC
---------------------------------------------------------------------------------
-- One Go
SELECT * FROM actors_history_scd
WHERE asofyear = 2021
	AND actor = 'Charlie Murphy'
	AND actorid = 'nm0614151'
ORDER BY actor, streak;
---------------------------------------------------------------------------------
-- Compare the 2 x versions for any given asofyear to make sure they are identical
SELECT
	COALESCE(v1.actorid, v2.actorid) AS actorid,
	COALESCE(v1.actor, v2.actor) AS actor,
	v1.token AS onego_token,
	v2.token AS inc_token
FROM (
	SELECT
		*,
		calc_token(is_active,quality_class) AS token
	FROM actors_history_scd
	WHERE asofyear = 2000
) v1
FULL OUTER JOIN (
	SELECT
		*,
		calc_token(is_active,quality_class) AS token
	FROM actors_history_scd_inc
	WHERE asofyear = 2000
) v2
ON v1.asofyear = v2.asofyear
	AND v1.actorid = v2.actorid
	AND v1.streak = v2.streak
WHERE v1.actorid IS NULL
	OR v2.actorid IS NULL
	OR v1.token != v2.token
---------------------------------------------------------------------------------








  



