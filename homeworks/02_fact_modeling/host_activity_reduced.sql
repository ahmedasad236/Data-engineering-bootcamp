CREATE TABLE host_activity_reduced(
 	host text,
 	MONTH date,
 	hit_array integer[],
 	unique_visitors_array integer[],
 	PRIMARY KEY (host, month)
 	);
 
 -- An incremental query that loads `host_activity_reduced`
 insert into host_activity_reduced
 WITH daily_aggregate AS (
 	SELECT host,
 			DATE(event_time) as current_date,
 			count(1) AS num_hits,
 			count(distinct user_id) as unique_visitors
 	FROM events
 	WHERE  DATE(event_time) = date('2023-01-01')
 	GROUP BY 1, 2
 ),
 yesterday_array as (
 	select *
 	from host_activity_reduced
 	where month = date('2023-01-01')
 )
 
 select 
 	coalesce(da.host, ya.host) as host,
 	coalesce(ya.month, date_trunc('month', da.current_date)) as month,
 	case
 		when ya.hit_array is not null
 		then ya.hit_array || array[coalesce(da.num_hits, 0)]
 	 		
 		when ya.hit_array is null
 		then array_fill(0, array[coalesce(da.current_date - date(date_trunc('month', month)), 0)]) || array[coalesce(da.num_hits, 0)] 
 		
 	end as hit_array,
 	
 	case
 		when ya.unique_visitors_array is not null
 		then ya.unique_visitors_array || array[coalesce(da.unique_visitors, 0)]
 	 		
 		when ya.unique_visitors_array is null
 		then array_fill(0, array[coalesce(da.current_date - date(date_trunc('month', month)), 0)]) || array[coalesce(da.unique_visitors, 0)] 
 		
 	end as unique_visitors_array
 	
 from daily_aggregate da
 full outer join yesterday_array ya
 on da.host = ya.host

 ON CONFLICT (host, month) 
DO UPDATE SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array;