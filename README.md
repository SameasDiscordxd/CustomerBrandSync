# Google Ads Customer Segmentation Uploader

Automated tool for uploading customer data to Google Ads audience lists with advanced segmentation.

## Features

- **18 Audience Lists**: 6 segments across 3 brands (BBT, ATD, TW)
- **Multi-Account Support**: Uploads to separate brand Google Ads accounts
- **Advanced Segmentation**: Tire, Service, Lapsed, Repeat, Non-Customer segments
- **Full & Delta Uploads**: Initial setup and ongoing maintenance modes
- **Automated Tracking**: Database logging for all uploads

## Customer Segments

**ALL** - All customers for the brand
**TIRE** - Customers who purchased tires (PartTypeId = 13688)
**SERVICE** - Customers who used services (LAB parts or non-tire items)
**LAPSED** - Customers with no purchases in 15+ months
**NON_CUSTOMER** - Email subscribers who haven't made purchases
**REPEAT** - Customers with multiple visits

## Setup

1. Copy `.env.example` to `.env`
2. Add your Google Ads API credentials
3. Add Customer IDs for each brand account
4. Create 18 audience lists in Google Ads and add their IDs
5. Update the stored procedure in your database

## Usage

**Initial Setup** (populate all lists):
```bash
node segmented-googleAdsUploader.js --mode full
```

**Daily/Monthly Updates** (only new/changed customers):
```bash
node segmented-googleAdsUploader.js --mode delta
```

**Test Run** (process data without uploading):
```bash
node segmented-googleAdsUploader.js --dry-run
```

**Brand-Specific Upload**:
```bash
node segmented-googleAdsUploader.js --brand BBT
```

## Requirements

- Node.js
- Google Ads API access
- SQL Server database with required stored procedure
- Customer lists created in Google Ads

## Database

Uses stored procedure: `GetNewCustomersForGoogleAdsWithBrandInfo`
Updates tracking in: `GoogleAdsUploadTracking` table

## Brand Coverage

- **Big Brand Tire (BBT)**: ~968K customers
- **American Tire Depot (ATD)**: ~881K customers  
- **Tire World (TW)**: ~69K customers