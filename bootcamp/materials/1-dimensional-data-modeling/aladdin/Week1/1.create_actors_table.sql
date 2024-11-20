-- DROP TYPE public.film_type;
CREATE TYPE public.film_type AS
(
	film text,
	year integer,
	votes integer,
	rating real,
	filmid text
);


-- DROP TYPE public.quality_class_enum;
CREATE TYPE public.quality_class_enum AS ENUM
    ('star', 'good', 'average', 'bad');


-- DROP TABLE public.actors;
CREATE TABLE public.actors
(
    actor text COLLATE pg_catalog."default",
    actorid text COLLATE pg_catalog."default" NOT NULL,
	asofyear integer,
    films film_type[],					-- cumulative since 1970
    nfilms bigint,						-- cumulative since 1970
	avg_rating real,					-- for asofyear only
	quality_class quality_class_enum, 	-- for asofyear only
	is_active boolean, 					-- for asofyear only
    CONSTRAINT actors_pkey PRIMARY KEY (actorid, asofyear)
);









  



