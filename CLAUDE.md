# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Buff Backend is a FastAPI-based REST API for Amazon FBA advertising optimization. It manages Amazon Ads connections, generates advertising reports, and provides bid optimization capabilities.

## Common Development Commands

```bash
# Install dependencies
poetry install

# Run development server
poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload

# Run with Docker
docker build -t buff-backend .
docker run -p 8000:8000 buff-backend
```

## Architecture

The codebase follows a layered architecture:

- **API Layer** (`src/api/routes/`): REST endpoints organized by feature
  - `connections.py`: Amazon Ads OAuth2 flow and profile management
  - `reports.py`: Advertising report generation and download
  - `bid_optimizer.py`: Bid optimization logic
  - `metadatas.py`: Amazon Ads metadata endpoints

- **Service Layer** (`src/services/`): Business logic implementation
  - `amazon_ads.py`: Amazon Ads API client wrapper
  - `report_processor.py`: Report processing and storage logic

- **Core Layer** (`src/core/`):
  - `config.py`: Environment configuration using Pydantic Settings
  - `security.py`: Token encryption/decryption utilities
  - `supabase.py`: Database client initialization

- **Models** (`src/models/`): Data models and Pydantic schemas

## Key Technical Details

- **Async/Await**: All API endpoints and service methods use async/await for optimal performance
- **Environment Variables**: Required variables include SUPABASE_URL, SUPABASE_ANON_KEY, AMAZON_ADS_CLIENT_ID, AMAZON_ADS_CLIENT_SECRET, and ENCRYPTION_KEY
- **Amazon Ads API**: Uses V3 API for report generation with async report processing
- **Database**: Supabase for storing connections and report data
- **Authentication**: OAuth2 flow for Amazon Ads integration

## Development Notes

- FastAPI auto-generates API documentation at `/docs`
- All Amazon Ads tokens are encrypted before storage using Fernet encryption
- Report processing includes retry logic for failed reports
- CORS is configured for frontend integration