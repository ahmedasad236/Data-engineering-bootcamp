create type film as (
	film text,
	votes integer,
	rating real,
	filmid text
);

create type quality_class as enum('star', 'good', 'average', 'bad');

create table actors (
	actor text,
	films film[],
	quality_class quality_class,
	is_active boolean,
	current_year integer,
	primary key(actor, current_year)
);
-------------------------- Cumulative table generation query -----------------
insert into actors
with ly as (
	select * from actors
	where current_year = 1972
),
cy as (
	select actor, 
		year, 
		array_agg(row(film, votes, rating, filmid)::film) as films,
		avg(rating) as avg_rating 
	from actor_films group by actor, year
	having year = 1973

)
select coalesce(cy.actor, ly.actor) as actor,
		coalesce(ly.films, array[]::film[]) || 
		case 
			when cy.year is not null then
				cy.films
   			else array[]::film[]
		end as films,
		
	   case
	   	when cy.year is not null then
	   		case when cy.avg_rating > 8 then 'star'
	   			when cy.avg_rating > 7 then 'good'
	   			when cy.avg_rating > 6 then 'average'
	   			else 'bad'
   			end::quality_class
		else ly.quality_class
	 	end as quality_class,
	   case
	   	when cy.year is not null
	   		then true
	   		else false
	   end as is_active,	   
	   coalesce(cy.year, ly.current_year + 1) as current_year
from cy
full outer join ly
on cy.actor = ly.actor

-------------------------------------------- actors_history_scd -----------------------------



CREATE TABLE actors_scd_table (
	actor text,
	quality_class quality_class,
	is_active boolean,
	start_year integer,
	end_year integer,
	current_year integer,
	PRIMARY KEY(actor, start_year)
);



insert into actors_scd_table 
WITH streak_started AS (
    SELECT actor,
           current_year,
           quality_class,
           is_active,
           LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY current_year) <> quality_class
               OR LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY current_year) IS NULL
           	   or LAG(is_active, 1) OVER
               (PARTITION BY actor ORDER BY current_year) <> is_active
               OR LAG(is_active, 1) OVER
               (PARTITION BY actor ORDER BY current_year) IS NULL
               AS did_change
    FROM actors
    where current_year <= 1970
),
     streak_identified AS (
         SELECT
            actor,
                quality_class,
                is_active,
                current_year,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY actor ORDER BY current_year) as streak_identifier

         FROM streak_started
     ),
     aggregated AS (
         SELECT
            actor,
            quality_class,
            is_active,
            streak_identifier,
            MIN(current_year) AS start_year,
            MAX(current_year) AS end_year,
            1970 as current_year
         FROM streak_identified
         GROUP BY 1,2,3,4
     )

     SELECT actor, quality_class, is_active, start_year, end_year, current_year
     FROM aggregated
     
 ------------------------------------- Incremental query for actors_scd ----------------------------
CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active boolean,
    start_year INTEGER,
    end_year INTEGER
);

WITH last_year_scd AS (
    SELECT * FROM actors_scd_table
    WHERE current_year = 1971
    AND end_year = 1971
),
     historical_scd AS (
        SELECT
            actor,
               quality_class,
               is_active,
               start_year,
               end_year
        FROM actors_scd_table
        WHERE current_year= 1971
        AND end_year < 1971
     ),
     this_year_data AS (
         SELECT * FROM actors
         WHERE current_year = 1972
     ),
     unchanged_records AS (
         SELECT
                ts.actor,
                ts.quality_class,
                ts.is_active,
                ls.start_year,
                ts.current_year as end_year
        FROM this_year_data ts
        JOIN last_year_scd ls
        ON ls.actor = ts.actor
         WHERE ts.quality_class = ls.quality_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.actor,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_year,
                        ls.end_year
                        )::scd_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.current_year,
                        ts.current_year
                        )::scd_type
                ]) as records
        FROM this_year_data ts
        LEFT JOIN last_year_scd ls
        ON ls.actor = ts.actor
         WHERE (ts.quality_class <> ls.quality_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT actor,
                (records::scd_type).quality_class,
                (records::scd_type).is_active,
                (records::scd_type).start_year,
                (records::scd_type).end_year
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.actor,
                ts.quality_class,
                ts.is_active,
                ts.current_year AS start_year,
                ts.current_year AS end_year
         FROM this_year_data ts
         LEFT JOIN last_year_scd ls
             ON ts.actor = ls.actor
         WHERE ls.actor IS NULL

     )


SELECT *, 1972 AS current_year FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a
