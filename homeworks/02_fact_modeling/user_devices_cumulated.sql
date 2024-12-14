/*
A DDL for an user_devices_cumulated table that has:

    a device_activity_datelist which tracks a users active days by browser_type
    data type here should look similar to MAP<STRING, ARRAY[DATE]>

    or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
*/

create table user_devices_cumulated (
	user_id text,
	device_id text,
	browser_type text,
	device_activity_datelist date[],
	date date,
	primary key(user_id, device_id, browser_type, date)
);


INSERT INTO user_devices_cumulated (
    user_id,
    device_id,
    browser_type,
    device_activity_datelist,
    date
)
WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-04')
),
today AS (
    SELECT 
        cast(user_id as text) AS user_id,
        date(cast(event_time as timestamp)) AS today_date,
        cast(device_id as text) AS device_id,
        COUNT(1) AS num_events
    FROM events
    WHERE date(cast(event_time as timestamp)) = DATE('2023-01-05')
      AND user_id IS NOT NULL
      AND device_id IS NOT NULL
    GROUP BY 1, 2, 3
),
merged_data AS (
    SELECT
        COALESCE(t.user_id, y.user_id) AS user_id,
        COALESCE(t.device_id, y.device_id) AS device_id,
        COALESCE(d.browser_type, y.browser_type) AS browser_type,
        COALESCE(
            y.device_activity_datelist,
            ARRAY[]::DATE[]
        ) || 
        CASE 
            WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
            ELSE ARRAY[]::DATE[]
        END AS device_activity_datelist,
        COALESCE(t.today_date, date(cast(y.date + INTERVAL '1 day' AS timestamp))) AS date
    FROM yesterday y
    FULL OUTER JOIN today t
        ON t.user_id = y.user_id AND t.device_id = y.device_id
    JOIN devices d
        ON cast(d.device_id AS text) = t.device_id
)
SELECT DISTINCT ON (user_id, device_id, browser_type, date)
    user_id,
    device_id,
    browser_type,
    device_activity_datelist,
    date
FROM merged_data;
