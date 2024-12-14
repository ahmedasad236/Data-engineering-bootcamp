create table hosts_cumulated (
	host text,
	host_activity_datelist date[],
	date date,
	primary key(host, date)
);


WITH yesterday AS (
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2022-01-31')
),
    today AS (
          SELECT host,
                 date(cast(event_time as timestamp)) AS today_date,
                 COUNT(1) AS num_events FROM events
            WHERE date(cast(event_time as timestamp)) = DATE('2023-02-01')
            AND host IS NOT NULL
         GROUP BY 1, 2
    )
INSERT INTO hosts_cumulated
SELECT
       COALESCE(t.host, y.host),
       COALESCE(y.host_activity_datelist,
           ARRAY[]::DATE[])
            || CASE WHEN
                t.host IS NOT NULL
                THEN ARRAY[t.today_date]
                ELSE ARRAY[]::DATE[]
                END AS date_list,
       COALESCE(t.today_date, y.date + Interval '1 day') as date
FROm yesterday y
    FULL OUTER JOIN
    today t ON t.host = y.host;
    
   -------------------------------------------------------------
   
   WITH host_starter AS (
    SELECT hc.host_activity_datelist @> ARRAY [DATE(d.valid_date)]  AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
            hc.host
    FROM hosts_cumulated hc
             CROSS JOIN
         (SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-01-31')
),
     host_bits AS (
         SELECT host,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,
                DATE('2023-01-31') as date
         FROM host_starter
         GROUP BY host
     )

     --INSERT INTO host_datelist_int
     SELECT * FROM host_bits