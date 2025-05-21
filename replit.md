# Google Ads Customer Data Upload Tool

## Overview
This repository contains a tool designed to automate the upload of customer data to Google Ads for Customer Match audiences. It supports both full and incremental (delta) uploads with virtual environment management, all packaged in a single file application.

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture
The application follows a single-file design pattern, with the main functionality contained in `Workspace_customer_data.py`. It is built with a self-contained approach that:

1. Creates and manages its own virtual environment (`venv_gads`)
2. Automatically installs required dependencies
3. Provides retry logic and rate limiting to handle Google Ads API constraints
4. Manages brand-specific targeting capabilities

The system is designed to run as a Python command-line tool, with automatic environment setup and dependency management to simplify deployment and usage.

## Key Components

### Core Components
1. **Virtual Environment Management**: The script automatically creates and manages a virtual environment to isolate dependencies (`venv_gads`). It checks if it's already running in the virtual environment, and if not, recreates execution within that environment.

2. **Command Line Interface**: The script uses the `argparse` module to provide a command-line interface for users to control upload behavior.

3. **Google Ads API Integration**: The tool interfaces with the Google Ads API for Customer Match audiences, handling authentication and data upload.

4. **Error Handling**: Includes retry logic for handling concurrent modification errors and rate limiting to prevent API rate limits.

### Supporting Files
- `pyproject.toml` - Defines Python package metadata and dependencies
- `requirements.txt` - Lists required package dependencies
- `.replit` - Configures the Replit environment and execution workflow

## Data Flow
1. User invokes the tool with appropriate command-line arguments
2. The script checks if it's running in its virtual environment
   - If not, it sets up the environment and re-executes itself
3. Command line arguments are parsed to determine execution mode
4. Customer data is prepared and formatted according to Google Ads specifications
5. The data is uploaded to Google Ads using the API, with retry logic and rate limiting
6. Results and logs are provided to the user

## External Dependencies
The application has the following key dependencies:
1. `google-ads` - For interacting with the Google Ads API
2. `phonenumbers` - Likely used for phone number validation or formatting
3. `pyodbc` - Suggests the tool can connect to ODBC data sources
4. `pyyaml` - Used for configuration and settings

## Deployment Strategy
The application is designed to be run directly from the command line. In the Replit environment, it's configured to run with a button press using the workflow defined in `.replit`.

The script supports both ad-hoc usage and could be scheduled for regular data synchronization. The virtual environment management makes it suitable for distribution as a standalone tool that manages its own dependencies.

## Usage Patterns
While the full command-line interface isn't visible in the code snippet provided, the tool appears to support different upload modes:
1. Full uploads of customer data
2. Incremental (delta) uploads for efficiency
3. Brand-specific targeting options

The help command (`--help`) will display all available options and usage patterns.