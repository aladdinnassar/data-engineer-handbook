-- Generate films table
SELECT
	MAX(actor) AS actor,
	actorid,
	ARRAY_AGG(ROW(film, year, votes, rating, filmid) ORDER BY year) AS films,  	-- remove year
	AVG(rating) AS avg_rating,
	CASE
 		WHEN AVG(rating) > 8 THEN 'star'
 		WHEN AVG(rating) > 7 THEN 'good'
 		WHEN AVG(rating) > 6 THEN 'average'
 		ELSE 'bad'
 	END AS quality_class,
	COUNT(1) AS films, 															-- remove
	MIN(year) AS min_year,	 													-- remove
	MAX(year) AS max_year, 														-- remove
	(MAX(year) = DATE_PART('year', CURRENT_DATE)) AS is_active
FROM public.actor_films
GROUP BY actorid
ORDER BY MAX(year) DESC, MIN(year); 