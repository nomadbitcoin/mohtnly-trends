CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.influencers` (
    id STRING,
    name STRING,
    twitter_handle STRING,
    instagram_handle STRING,
    youtube_handle STRING,
    tiktok_handle STRING,
    facebook_handle STRING,
    last_twitter_updated TIMESTAMP,
    last_instagram_updated TIMESTAMP,
    last_youtube_updated TIMESTAMP,
    last_tiktok_updated TIMESTAMP,
    last_facebook_updated TIMESTAMP,
    active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.twitter_metrics` (
    id STRING,
    influencer_id STRING,
    followers INT64,
    following INT64,
    tweets INT64,
    engagement_rate FLOAT64,
    timestamp TIMESTAMP,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.youtube_metrics` (
    id STRING,
    influencer_id STRING,
    subscribers INT64,
    total_views INT64,
    videos INT64,
    engagement_rate FLOAT64,
    timestamp TIMESTAMP,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.instagram_metrics` (
    id STRING,
    influencer_id STRING,
    followers INT64,
    following INT64,
    posts INT64,
    engagement_rate FLOAT64,
    timestamp TIMESTAMP,
    created_at TIMESTAMP
); 