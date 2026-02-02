CREATE DATABASE customers_db;
USE customers_db;

CREATE TABLE users (
    user_id VARCHAR(10) PRIMARY KEY,
    signup_date DATE,
    country VARCHAR(50),
    device_type VARCHAR(20)
);


CREATE TABLE sessions (
    session_id VARCHAR(15) PRIMARY KEY,
    user_id VARCHAR(10),
    session_start TIMESTAMP,
    session_end TIMESTAMP NULL,
    pages_visited INT,
    source_channel VARCHAR(20),

    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE feature_usage (
    user_id VARCHAR(10),
    feature_name VARCHAR(50),
    usage_timestamp TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE subscriptions (
    user_id VARCHAR(10),
    plan_type VARCHAR(20),
    start_date DATE,
    end_date DATE NULL,
    is_active INT,

    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE cancellations (
    user_id VARCHAR(10),
    cancel_date DATE,
    cancel_reason VARCHAR(100),

    FOREIGN KEY (user_id) REFERENCES users(user_id)
);



SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM cancellations;
SELECT COUNT(*) FROM feature_usage;
SELECT COUNT(*) FROM sessions;
SELECT COUNT(*) FROM subscriptions;

CREATE OR REPLACE VIEW retention_base AS
SELECT
    u.user_id,
    DATE_FORMAT(u.signup_date, '%Y-%m') AS signup_month,

    -- last time the user used the app
    (
        SELECT MAX(session_start)
        FROM sessions s
        WHERE s.user_id = u.user_id
    ) AS last_active_date,

    -- number of total sessions
    (
        SELECT COUNT(*)
        FROM sessions s
        WHERE s.user_id = u.user_id
    ) AS total_sessions,

    -- number of active months
    (
        SELECT COUNT(DISTINCT DATE_FORMAT(session_start, '%Y-%m'))
        FROM sessions s
        WHERE s.user_id = u.user_id
    ) AS active_months,

    -- churn flag from subscriptions
    CASE
        WHEN sub.is_active = 0 THEN 1
        ELSE 0
    END AS churned,

    sub.end_date AS churn_date

FROM users u
LEFT JOIN subscriptions sub
    ON u.user_id = sub.user_id;
    
SELECT * FROM retention_base;

CREATE OR REPLACE VIEW session_analytics AS
SELECT
    s.session_id,
    s.user_id,

    DATE(s.session_start) AS session_date,

    s.session_start,
    s.session_end,

    -- raw session duration in minutes (may be NULL or extreme → Python will handle)
    TIMESTAMPDIFF(
        MINUTE,
        s.session_start,
        s.session_end
    ) AS session_duration_minutes,

    s.pages_visited,
    s.source_channel,

    -- weekday vs weekend flag (for hypothesis testing)
    CASE
        WHEN DAYOFWEEK(s.session_start) IN (1,7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type

FROM sessions s;
SELECT * FROM session_analytics;

CREATE OR REPLACE VIEW feature_usage_summary AS
SELECT
    fu.user_id,

    -- total feature events per user
    COUNT(*) AS total_feature_events,

    -- number of distinct features used
    COUNT(DISTINCT fu.feature_name) AS unique_features_used,

    -- most used feature for each user
    (
        SELECT f2.feature_name
        FROM feature_usage f2
        WHERE f2.user_id = fu.user_id
        GROUP BY f2.feature_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) AS top_feature_used,

    -- whether user used any feature in last 7 days (from last session)
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM feature_usage f3
            WHERE f3.user_id = fu.user_id
            AND f3.usage_timestamp >= DATE_SUB(
                (SELECT MAX(session_start)
                 FROM sessions s
                 WHERE s.user_id = fu.user_id),
                INTERVAL 7 DAY
            )
        ) THEN 1
        ELSE 0
    END AS used_in_last_7_days

FROM feature_usage fu
GROUP BY fu.user_id;
SELECT * FROM feature_usage_summary;

CREATE OR REPLACE VIEW churn_analysis AS
SELECT
    c.user_id,
    c.cancel_date,
    c.cancel_reason,

    -- plan type from subscriptions
    sub.plan_type,
    sub.start_date AS subscription_start_date,

    -- date of last activity before churn
    (
        SELECT MAX(s.session_start)
        FROM sessions s
        WHERE s.user_id = c.user_id
    ) AS last_active_date,

    -- how many sessions user had before churn
    (
        SELECT COUNT(*)
        FROM sessions s
        WHERE s.user_id = c.user_id
        AND s.session_start <= c.cancel_date
    ) AS total_sessions_before_churn,

    -- feature usage count before churn
    (
        SELECT COUNT(*)
        FROM feature_usage f
        WHERE f.user_id = c.user_id
        AND f.usage_timestamp <= c.cancel_date
    ) AS feature_usage_before_churn

FROM cancellations c
LEFT JOIN subscriptions sub
    ON c.user_id = sub.user_id;
    
    SELECT * FROM churn_analysis;
