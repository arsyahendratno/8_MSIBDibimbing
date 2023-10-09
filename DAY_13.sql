--no 1 a
create table dim_user(
	id serial primary key,
	user_id int,
	name varchar,
	email varchar,
	gender varchar,
	age int,
	ads_source varchar,
	event_type varchar,
	"timestamp" timestamp,
	event_data json
);

insert into dim_user(user_id,name,email,gender,age,ads_source,event_type,"timestamp",event_data)
select
	u.id,
	concat(first_name,' ',last_name) as nama,
	email,
	gender,
	extract (year from register_date) - extract (year from dob) as umur,
	concat(fa.ads_id, ia.ads_id) as ads,
	ue.event_type,
	ue."timestamp",
	ue.event_data 
from "user".users u 
left join "event"."User Event" ue on ue.user_id = u.id 
left join "user".user_transactions ut on ut.user_id = u.id
left join social_media.facebook_ads fa on u.client_id = fa.id 
left join social_media.instagram_ads ia on u.client_id = ia.id

--no 1b
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
    
--no 2a
CREATE TABLE fact_user_performance (
    user_id INT PRIMARY KEY,
    last_login TIMESTAMP,
    last_activity TIMESTAMP,
    total_transactions INT,
    total_ads_clicks INT,
    total_events INT,
    engagement_score DECIMAL(10, 2)
);
-- Create a link between the fact table and the dimension table through a foreign key.
ALTER TABLE fact_user_performance
    ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES "user".users(id);
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
    SELECT combined_ads.id,combined_ads.timestamp, COUNT(*) AS total_clicks
    FROM (
        SELECT fa.id,fa.timestamp
        FROM social_media.facebook_ads fa
        JOIN "user".users u ON fa.id = u.client_id
        UNION ALL
        SELECT ia.id,ia.timestamp
        FROM social_media.instagram_ads ia
        JOIN "user".users u ON ia.id = u.client_id
    ) combined_ads
    GROUP BY combined_ads.id, combined_ads.timestamp
) AS fa ON u.client_id = fa.id
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

--no 2b
CREATE TABLE fact_ads_performance (
    ad_id VARCHAR(50) PRIMARY KEY,
    total_clicks INT,
    total_converted INT,
    total_impressions INT,
    click_through_rate DECIMAL(10, 2),
    conversion_rate DECIMAL(10, 2)
);

-- Populate the table with data.
INSERT INTO fact_ads_performance (ad_id, total_clicks, total_converted, total_impressions, click_through_rate, conversion_rate)
SELECT
    fa.ads_id AS ad_id,
    COALESCE(fa.total_clicks, 0) AS total_clicks,
    COALESCE(uc.total_converted_android, 0) + COALESCE(uc.total_converted_ios, 0) + COALESCE(uc.total_converted_desktop, 0) AS total_converted,
    COALESCE(fa.total_impressions, 0) AS total_impressions,
    CASE
        WHEN COALESCE(fa.total_impressions, 0) > 0 THEN (COALESCE(fa.total_clicks, 0) / COALESCE(fa.total_impressions, 0)) * 100
        ELSE 0
    END AS click_through_rate,
    CASE
        WHEN COALESCE(fa.total_clicks, 0) > 0 THEN ((COALESCE(uc.total_converted_android, 0) + COALESCE(uc.total_converted_ios, 0) + COALESCE(uc.total_converted_desktop, 0)) / COALESCE(fa.total_clicks, 0)) * 100
        ELSE 0
    END AS conversion_rate
FROM (
    -- Combine data from Facebook Ads and Instagram Ads for total_clicks and total_impressions.
    SELECT ads_id, SUM(total_clicks) AS total_clicks, SUM(total_impressions) AS total_impressions
    FROM (
        SELECT ads_id, SUM(total_clicks) AS total_clicks, SUM(total_impressions) AS total_impressions
        FROM (
            SELECT ads_id, SUM(1) AS total_clicks, 0 AS total_impressions
            FROM social_media.facebook_ads
            GROUP BY ads_id
            UNION ALL
            SELECT ads_id, 0 AS total_clicks, SUM(1) AS total_impressions
            FROM social_media.instagram_ads
            GROUP BY ads_id
        ) AS combined_ads
        GROUP BY ads_id
    ) AS aggregated_ads
    GROUP BY ads_id
) AS fa
LEFT JOIN (
    -- Calculate total conversions for Android, IOS, and Desktop separately.
    SELECT ads_id, 
           SUM(CASE WHEN device_type = 'Android' THEN 1 ELSE 0 END) AS total_converted_android,
           SUM(CASE WHEN device_type = 'IOS' THEN 1 ELSE 0 END) AS total_converted_ios,
           SUM(CASE WHEN device_type = 'Desktop' THEN 1 ELSE 0 END) AS total_converted_desktop
    FROM social_media.facebook_ads
    GROUP BY ads_id
) AS uc ON fa.ads_id = uc.ads_id::VARCHAR(50);

--no 31
create table fact_daily_event_performance(
	id serial primary key,
	event_date date,
	event_type varchar,
	total_events int,
	event_data json
)
insert into fact_daily_event_performance(event_date,event_type,total_events,event_data)
SELECT
    DATE(du.timestamp) AS event_date,
    du.event_type,
    COUNT(du.event_type) AS total_events,	
    json_agg(du.event_data) as event_data
FROM public.fact_user_performance fup 
LEFT JOIN dim_user du ON du.user_id = fup.user_id
WHERE event_data IS NOT NULL AND event_type IS NOT NULL
GROUP BY event_date, du.event_type
order by event_date asc
