# Setup Instructions

## Installation

1. Download and extract the project files
2. Install dependencies:
   ```bash
   npm install
   ```

## Configuration

1. Copy `.env.example` to `.env`
2. Fill in your database and Google Ads API details
3. Create 18 audience lists in Google Ads (6 per brand account)
4. Add the list IDs to your `.env` file
5. Update your database with the provided stored procedure

## Database Setup

Run the stored procedure update from the attached file:
`GetNewCustomersForGoogleAdsWithBrandInfo`

## Testing

Test with a dry run first:
```bash
node segmented-googleAdsUploader.js --dry-run
```

## Production Usage

Initial setup:
```bash
node segmented-googleAdsUploader.js --mode full
```

Ongoing updates:
```bash
node segmented-googleAdsUploader.js --mode delta
```