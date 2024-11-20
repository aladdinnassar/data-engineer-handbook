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