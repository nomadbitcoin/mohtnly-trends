# Social Media Influencer Data Fetcher

This tool fetches and stores social media metrics for influencers from various platforms using the SocialBlade API.

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up environment variables:
   ```bash
   cp .env.example .env
   ```

Required environment variables:

- `SOCIALBLADE_API_KEY`: Your SocialBlade API key
- `BIGQUERY_PROJECT_ID`: Your Google Cloud project ID
- `BIGQUERY_DATASET`: BigQuery dataset name
- `DEV_MODE`: Set to 'true' for development (saves to CSV) or 'false' for production

4. Set up BigQuery tables by running the SQL scripts in `schema/create_tables.sql`

## Usage

### Adding a New Influencer

To add a new influencer and fetch their historical data (last 12 months):

```bash
python main.py --add_user
```

This will:

1. Prompt for the influencer's name
2. Ask for handles for each platform (Twitter, YouTube, Instagram, TikTok, Facebook)
3. Save the influencer to the database
4. Fetch and store one year of historical data for each platform

### Monthly Data Update

To update data for all active influencers (runs automatically via Cloud Run):

```bash
python main.py
```

This will:

1. Fetch all active influencers from the database
2. For each platform, fetch the last 30 days of metrics
3. Skip influencers that were updated less than 30 days ago
4. Store the new metrics in BigQuery

### Development Mode

To run in development mode (storing data in CSV files):

```bash
python main.py --dev_mode
```

## Error Handling

- All errors are logged to both console and `app.log`
- The script continues processing other platforms if one fails
- Failed API requests are retried with exponential backoff

## Development

Run tests:

```bash
pytest
```

The tests use VCR.py to mock API responses, so they can run without actual API calls.
