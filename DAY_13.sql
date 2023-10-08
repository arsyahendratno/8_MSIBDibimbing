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
    -- Presuming that the register_date is automatically updated whenever the user attempts to log in, hence the last_login is calculated as the maximum registration date from the 'users' table.
    MAX(u.register_date) AS last_login,
    -- The last_activity is derived from the maximum timestamp from different event sources, namely registration, transactions, ad clicks, and user events.
    MAX(COALESCE(u.register_date, ut.transaction_date, fa.timestamp, ia.timestamp, ue.timestamp)) AS last_activity,
    COALESCE(ut.total_transactions, 0) AS total_transactions,
    COALESCE(fa.total_clicks, 0) + COALESCE(ia.total_clicks, 0) AS total_ads_clicks,
    COALESCE(ue.total_events, 0) AS total_events,
    -- The engagement score is determined by calculating the mean of the user's activities.
    -- total_transactions + total_ads_clicks + total_events) / 3
    ((COALESCE(ut.total_transactions, 0) + COALESCE(fa.total_clicks, 0) + COALESCE(ia.total_clicks, 0) + COALESCE(ue.total_events, 0)) / 3.0) AS engagement_score
FROM users u
JOIN (
    SELECT user_id, COUNT(*) AS total_transactions
    FROM user_transactions
    GROUP BY user_id
) ut ON u.id = ut.user_id
JOIN (
    SELECT user_id, COUNT(*) AS total_clicks
    FROM facebook_ads
    GROUP BY user_id
    UNION ALL
    SELECT user_id, COUNT(*) AS total_clicks
    FROM instagram_ads
    GROUP BY user_id
) AS fa ON u.id = fa.user_id
JOIN (
    SELECT user_id, COUNT(*) AS total_events
    FROM user_event
    GROUP BY user_id
) AS ue ON u.id = ue.user_id
JOIN (
    SELECT user_id, MAX(timestamp) AS timestamp
    FROM (
        SELECT user_id, timestamp FROM facebook_ads
        UNION ALL
        SELECT user_id, timestamp FROM instagram_ads
        UNION ALL
        SELECT user_id, timestamp FROM user_event
    ) AS combined_events
    GROUP BY user_id
) AS ia ON u.id = ia.user_id
GROUP BY u.id;
