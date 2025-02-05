CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.influencers` (
    id STRING NOT NULL,
    name STRING NOT NULL,
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
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.twitter_metrics` (
    id STRING NOT NULL,
    influencer_id STRING NOT NULL,
    followers INT64 DEFAULT 0,
    following INT64 DEFAULT 0,
    tweets INT64 DEFAULT 0,
    engagement_rate FLOAT64 DEFAULT 0.0,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.youtube_metrics` (
    id STRING NOT NULL,
    influencer_id STRING NOT NULL,
    subscribers INT64 DEFAULT 0,
    total_views INT64 DEFAULT 0,
    videos INT64 DEFAULT 0,
    engagement_rate FLOAT64 DEFAULT 0.0,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.instagram_metrics` (
    id STRING NOT NULL,
    influencer_id STRING NOT NULL,
    followers INT64 DEFAULT 0,
    following INT64 DEFAULT 0,
    posts INT64 DEFAULT 0,
    engagement_rate FLOAT64 DEFAULT 0.0,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.tiktok_metrics` (
    id STRING NOT NULL,
    influencer_id STRING NOT NULL,
    followers INT64 DEFAULT 0,
    following INT64 DEFAULT 0,
    likes INT64 DEFAULT 0,
    videos INT64 DEFAULT 0,
    engagement_rate FLOAT64 DEFAULT 0.0,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL
); 