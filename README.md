# Google Ads Customer Data Upload Tool

This Node.js tool automates the upload of customer data to Google Ads for Customer Match audiences, supporting both full and incremental (delta) uploads with brand-specific targeting capabilities.

## Features

- Brand-specific targeting capabilities
- Automatic dependency management
- Retry logic for handling concurrent modification errors
- Rate limiting to prevent API rate limits
- Support for email, phone, and address-based matching

## Setup

1. **Install Dependencies**:
   ```
   npm install
   ```

2. **Configure Environment Variables**:
   - Copy `.env.example` to `.env`
   - Fill in your database credentials and Google Ads API information
   - Add brand-specific user list IDs as needed

   ```
   # Brand-specific example
   BRANDNAME_USER_LIST_ID=1234567890
   ```

## Usage

### Basic Usage

Run a delta upload (only new customers):
```
node googleAdsUploader.js
```

Run a full upload (all customers):
```
node googleAdsUploader.js --mode full
```

### Brand-Specific Uploads

Upload only customers for a specific brand:
```
node googleAdsUploader.js --brand brandname
```

Upload for all brands at once (processes each brand sequentially):
```
node googleAdsUploader.js --all-brands
```

### Listing Available Configurations

List configured brands:
```
node googleAdsUploader.js --list-brands
```

List available Google Ads user lists:
```
node googleAdsUploader.js --list-user-lists
```

### Creating Brand-Specific Lists

Create a new user list for a brand:
```
node googleAdsUploader.js --create-brand-list brandname
```

## Automatic Uploads

This tool provides two methods for automating uploads:

### 1. Built-in Scheduler

Use the included scheduler script for flexible automated uploads:

```
node schedule-upload.js
```

The scheduler runs in the background and executes uploads based on your configured schedules. Edit `schedule-upload.js` to customize:
- Default schedule (runs at 2 AM daily for all brands)
- Brand-specific schedules (run different brands at different times)
- Upload modes (delta or full)

### 2. System Cron Jobs

Alternatively, use system cron jobs for direct scheduling:

Example crontab entry (run daily at 2 AM):
```
0 2 * * * cd /path/to/tool && node googleAdsUploader.js --all-brands --silent >> uploads.log 2>&1
```

Example for specific brands:
```
0 1 * * * cd /path/to/tool && node googleAdsUploader.js --brand brandname --silent >> brand_uploads.log 2>&1
```

## Database Requirements

The tool expects the following stored procedures to be available in your database:

- `dbo.GetNewCustomersForGoogleAds`: For regular customer data retrieval
- `dbo.GetNewCustomersForGoogleAdsByBrand`: For brand-specific customer data retrieval

## Logging

Logs are saved to `googleAdsUploader.log` and also output to the console.