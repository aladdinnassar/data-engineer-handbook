---------------------------------------------------------------------------------
-- DROP FUNCTION calc_quality_class;
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
---------------------------------------------------------------------------------
SELECT calc_quality_class(5.0);
---------------------------------------------------------------------------------
-- DROP FUNCTION generate_yearly_actors;
CREATE OR REPLACE FUNCTION generate_yearly_actors (
  select_year integer
)
RETURNS TABLE (
	actor text,
	actorid text,
	asofyear integer,
	films film_type[],
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
				ROW(film, year, votes, rating, filmid)::film_type
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
-- DROP FUNCTION ARRAY_SORT
CREATE OR REPLACE FUNCTION ARRAY_SORT(ANYARRAY)
RETURNS ANYARRAY
LANGUAGE SQL
AS $$
SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$$;
---------------------------------------------------------------------------------
DROP FUNCTION increment_actors;
CREATE OR REPLACE FUNCTION increment_actors (
  select_year integer
)
RETURNS TABLE (
	actor text,
	actorid text,
	asofyear integer,
	films film_type[],							-- Sorted list of films since 1970
	nfilms bigint,								-- Cumulative count of films since 1970
	avg_rating real,							-- NULL if not active
	quality_class quality_class_enum,			-- NULL if not active
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
		END::film_type[] AS films,									-- Sorted list of films since 1970
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
-- DROP PROCEDURE insert_actors;
CREATE PROCEDURE insert_actors(select_year integer)
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
	) SELECT * FROM increment_actors(select_year);
$$;
---------------------------------------------------------------------------------
-- MIN(year) = 1970, MAX(year) = 2021
SELECT
	MIN(year),
	MAX(year)
FROM public.actor_films
---------------------------------------------------------------------------------
DO $$
BEGIN
	DELETE FROM actors;  

	FOR year IN 1970..2021 LOOP
		CALL insert_actors(year);
		RAISE NOTICE 'year = %', year;
	END LOOP;
END;
$$;

SELECT * FROM actors ORDER BY actor, asofyear LIMIT 100;  -- results = 249082 rows
---------------------------------------------------------------------------------









  



