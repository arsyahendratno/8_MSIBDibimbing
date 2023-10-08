-- 1a.
create table dim_user(
	user_id int primary key,
	name varchar,
	email varchar,
	gender varchar,
	age int,
	ads_source varchar
);
insert into dim_user(user_id,name,email,gender,age,ads_source)
select 
	u.id,
	concat(first_name,' ',last_name) as nama,
	email,
	gender,
	extract (year from register_date) - extract (year from dob) as umur,
	concat(fa.ads_id, ia.ads_id) as ads
from "user".users u
left join social_media.facebook_ads fa on u.client_id = fa.id 
left join social_media.instagram_ads ia on u.client_id = ia.id 


-- 1b.
CREATE TABLE dim_ads AS
SELECT
    ads_id,
    device_type,
    device_id,
    timestamp
FROM
    social_media.facebook_ads
UNION ALL
SELECT
    ads_id,
    device_type,
    device_id,
    timestamp
FROM
    social_media.instagram_ads;

    
-- 2. Fact Table Creation
-- a. Fact table named 'fact_user_performance'
CREATE TABLE fact_user_performance (
    user_id INT PRIMARY KEY,
    last_login TIMESTAMP,
    last_activity TIMESTAMP,
    total_transactions INT,
    total_ads_clicks INT,
    total_events INT,
    engagement_score DECIMAL(10, 2),
);
-- Create a link between the fact table and the dimension table through a foreign key.
ALTER TABLE fact_user_performance
    ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id);
-- Populating the table with data.
INSERT INTO fact_user_performance (user_id, last_login, last_activity, total_transactions, total_ads_clicks, total_events, engagement_score)
SELECT
    u.id AS user_id,
    MAX(
        CASE
            WHEN ue.event_type = 'login' THEN ue.timestamp -- Use the timestamp of the 'login' event.
            ELSE u.register_date -- Use the registration date as fallback.
        END
    ) AS last_login,
    -- The last_activity is determined from the maximum timestamp from different event sources, namely registration, transactions, ad clicks, and user events.
    MAX(
        GREATEST(
            COALESCE(
            	u.register_date,
                ut.transaction_date,
                fa.timestamp,
                ia.timestamp,
                ue.timestamp
            ),
            CASE
                WHEN ue.event_type = 'NULL' THEN u.register_date
                ELSE ue.timestamp
            END
        )
    ) AS last_activity,
    COALESCE(SUM(ut.total_transactions), 0) AS total_transactions,
    COALESCE(SUM(fa.total_clicks), 0) + COALESCE(SUM(ia.total_clicks), 0) AS total_ads_clicks,
    --  The engagement score is determined by calculating the mean of the user's activities.
    -- total_transactions + total_ads_clicks + total_events / 3
    COALESCE(SUM(ue.total_events), 0) AS total_events,
    (
        (
            COALESCE(SUM(ut.total_transactions), 0) +
            COALESCE(SUM(fa.total_clicks), 0) +
            COALESCE(SUM(ia.total_clicks), 0) +
            COALESCE(SUM(ue.total_events), 0)
        ) / 3.0
    ) AS engagement_score
FROM "user".users u
LEFT JOIN (
    SELECT user_id, transaction_date, COUNT(*) AS total_transactions 
    FROM "user".user_transactions ut
    GROUP BY user_id, transaction_date
) ut ON u.id = ut.user_id
LEFT JOIN (
    SELECT user_id, timestamp, COUNT(*) AS total_clicks
    FROM (
        SELECT user_id, "timestamp" FROM social_media.facebook_ads fa
        JOIN "user".users u ON fa.id = u.client_id
        JOIN "user".user_transactions ut ON u.id = ut.user_id
        UNION ALL
        SELECT user_id, "timestamp" FROM social_media.instagram_ads ia
        JOIN "user".users u ON ia.id = u.client_id
        JOIN "user".user_transactions ut ON u.id = ut.user_id
    ) AS combined_ads
    GROUP BY user_id, timestamp
) AS fa ON u.client_id = fa.user_id
LEFT JOIN (
    SELECT user_id, MAX("timestamp") AS "timestamp", COUNT(*) AS total_clicks
    FROM (
        SELECT user_id, "timestamp" FROM social_media.facebook_ads fa
        JOIN "user".users u ON fa.id = u.client_id
        JOIN "user".user_transactions ut ON u.id = ut.user_id
        UNION ALL
        SELECT user_id, "timestamp" FROM social_media.instagram_ads ia
        JOIN "user".users u ON ia.id = u.client_id
        JOIN "user".user_transactions ut ON u.id = ut.user_id
        UNION ALL
        SELECT user_id, "timestamp" FROM "event"."User Event" -- Reference 'User Event' directly from the 'event' schema.
    ) AS combined_events
    GROUP BY user_id
) AS ia ON u.client_id = ia.user_id
LEFT JOIN (
    SELECT user_id, event_type, timestamp, COUNT(*) AS total_events
    FROM "event"."User Event" -- Reference 'User Event' directly from the 'event' schema.
    GROUP BY user_id, event_type, timestamp
) AS ue ON u.id = ue.user_id
GROUP BY u.id;

-- 2b.                                                                                                                                                CREATE TABLE fakta_iklan_kinerja AS
SELECT
    a.ads_id,
    COUNT(DISTINCT fb.device_id) AS total_klik,
    COUNT(DISTINCT CASE WHEN fb.device_type = 'Mobile' THEN fb.device_id ELSE NULL END) AS total_konversi_mobile,
    COUNT(DISTINCT CASE WHEN fb.device_type = 'Desktop' THEN fb.device_id ELSE NULL END) AS total_konversi_desktop
FROM facebook_ads AS fb
GROUP BY a.ads_id;

INSERT INTO fakta_iklan_kinerja (ads_id, total_klik, total_konversi_mobile, total_konversi_desktop)
SELECT
    a.ads_id,
    COUNT(DISTINCT ig.device_id) AS total_klik,
    COUNT(DISTINCT CASE WHEN ig.device_type = 'Mobile' THEN ig.device_id ELSE NULL END) AS total_konversi_mobile,
    COUNT(DISTINCT CASE WHEN ig.device_type = 'Desktop' THEN ig.device_id ELSE NULL END) AS total_konversi_desktop
FROM instagram_ads AS ig
GROUP BY a.ads_id;

-- 3. Data Mart
-- 3a. fact_daily_event_performance
CREATE TABLE fact_daily_event_performance (
    event_date DATE PRIMARY KEY,
    total_events INT,
    total_users INT
);
-- Populate the table with data.
INSERT INTO fact_daily_event_performance (event_date, total_events, total_users)
SELECT
    DATE(timestamp) AS event_date,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS total_users
FROM "event"."User Event"
WHERE timestamp >= 'start_date'::date AND timestamp <= 'end_date'::date
GROUP BY event_date;
