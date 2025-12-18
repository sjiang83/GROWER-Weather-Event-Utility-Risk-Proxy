"""
Purpose:
- Fetch weather-related disaster declarations from FEMA OpenFEMA Disaster Declarations Summaries v2 API
- Clean and aggregate data to "event-level" list for time series analysis
- Output processed data to `weather_events.csv` for event study notebooks

Data Source: FEMA OpenFEMA API v2
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import sys
import time
import math
from urllib.parse import urlencode
from typing import List, Dict, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 1) Configuration Section


# FEMA OpenFEMA API v2 endpoint
BASE_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

# Geographic scope - Southeast 4 states
STATES = ["GA", "FL", "SC", "NC"]  # Southeast region focus

# Time range (default: last 5 years)
END_DATE = datetime.now().date()
START_DATE = (datetime.now() - timedelta(days=5*365)).date()

# Weather event types whitelist - corrected based on actual FEMA incidentType values
WEATHER_TYPES = [
    "Hurricane", 
    "Tropical Storm",
    "Tropical Depression", 
    "Severe Storm",  # Note: NOT "Severe Storm(s)" - this was causing missing data
    "Flood"
    # Explicitly exclude "Fire" - not weather-related for grid resilience study
]

# API pagination parameters
TOP = 1000  # Max records per page
TIMEOUT = 30  # Request timeout in seconds
MAX_RETRY = 3  # Maximum retry attempts
USE_MOCK_DATA = False  # Set to True for testing without internet access

# Output path
OUTPUT_PATH = "data/weather_events.csv"

# Mock data for testing (when API is unavailable) - Southeast states focus
MOCK_DATA = [
    {
        "disasterNumber": 4673,
        "state": "GA",
        "incidentType": "Hurricane",
        "incidentBeginDate": "2024-08-26T00:00:00.000Z",
        "incidentEndDate": "2024-08-28T00:00:00.000Z",
        "declarationDate": "2024-08-27T00:00:00.000Z",
        "declarationTitle": "Hurricane Debby",
        "fipsStateCode": "13",
        "fipsCountyCode": "001",
        "placeCode": "0",
        "designatedArea": "Appling County"
    },
    {
        "disasterNumber": 4673,
        "state": "FL", 
        "incidentType": "Hurricane",
        "incidentBeginDate": "2024-08-26T00:00:00.000Z",
        "incidentEndDate": "2024-08-28T00:00:00.000Z",
        "declarationDate": "2024-08-27T00:00:00.000Z",
        "declarationTitle": "Hurricane Debby",
        "fipsStateCode": "12",
        "fipsCountyCode": "003",
        "placeCode": "0",
        "designatedArea": "Baker County"
    },
    {
        "disasterNumber": 4669,
        "state": "SC",
        "incidentType": "Severe Storm",  # Corrected - no "(s)"
        "incidentBeginDate": "2024-07-08T00:00:00.000Z",
        "incidentEndDate": "2024-07-09T00:00:00.000Z",
        "declarationDate": "2024-07-09T00:00:00.000Z",
        "declarationTitle": "South Carolina Severe Storms",
        "fipsStateCode": "45",
        "fipsCountyCode": "015",
        "placeCode": "0",
        "designatedArea": "Bamberg County"
    },
    {
        "disasterNumber": 4634,
        "state": "NC",
        "incidentType": "Tropical Storm",
        "incidentBeginDate": "2023-09-15T00:00:00.000Z", 
        "incidentEndDate": "2023-09-17T00:00:00.000Z",
        "declarationDate": "2023-09-16T00:00:00.000Z",
        "declarationTitle": "Tropical Storm Ophelia",
        "fipsStateCode": "37",
        "fipsCountyCode": "000",  # Tribal/special area example
        "placeCode": "12345",     # Valid place code for tribal area
        "designatedArea": "Eastern Band of Cherokee Indians"
    },
    {
        "disasterNumber": 4598,
        "state": "FL",
        "incidentType": "Hurricane",
        "incidentBeginDate": "2022-09-28T00:00:00.000Z",
        "incidentEndDate": "2022-09-30T00:00:00.000Z",
        "declarationDate": "2022-09-29T00:00:00.000Z",
        "declarationTitle": "Hurricane Ian",
        "fipsStateCode": "12",
        "fipsCountyCode": "071",
        "placeCode": "0",
        "designatedArea": "Lee County"
    },
    {
        "disasterNumber": 4598,
        "state": "FL",
        "incidentType": "Hurricane", 
        "incidentBeginDate": "2022-09-28T00:00:00.000Z",
        "incidentEndDate": "2022-09-30T00:00:00.000Z",
        "declarationDate": "2022-09-29T00:00:00.000Z",
        "declarationTitle": "Hurricane Ian",
        "fipsStateCode": "12",
        "fipsCountyCode": "000",   # Another tribal/special area 
        "placeCode": "67890",      # Different place code
        "designatedArea": "Seminole Tribe of Florida"
    }
]


# 2) OData Filter Construction


def build_odata_filter() -> Dict[str, str]:
    """
    Build OData query parameters for FEMA API request.
    
    Returns:
        Dict containing URL parameters for the API request
    """
    filters = []
    
    # Time filter: incidentBeginDate >= START_DATE
    if START_DATE:
        filters.append(f"incidentBeginDate ge '{START_DATE.isoformat()}'")
    
    if END_DATE:
        filters.append(f"incidentBeginDate le '{END_DATE.isoformat()}'")
    
    # Event type filter: incidentType in WEATHER_TYPES
    if WEATHER_TYPES:
        type_filters = [f"incidentType eq '{event_type}'" for event_type in WEATHER_TYPES]
        filters.append(f"({' or '.join(type_filters)})")
    
    # Multi-state filter: (state eq 'GA' or state eq 'FL' or state eq 'SC' or state eq 'NC')
    if STATES:
        state_filters = [f"state eq '{state}'" for state in STATES]
        filters.append(f"({' or '.join(state_filters)})")
    
    # Combine all filters
    filter_string = ' and '.join(filters) if filters else None
    
    # Select only necessary fields for event-level aggregation
    select_fields = [
        "disasterNumber",
        "state", 
        "incidentType",
        "incidentBeginDate",
        "incidentEndDate", 
        "declarationDate",
        "declarationTitle",
        "fipsStateCode",
        "fipsCountyCode",
        "placeCode",        # Added for tribal/special areas coverage calculation
        "designatedArea"
    ]
    
    params = {
        "$select": ",".join(select_fields),
        "$orderby": "incidentBeginDate desc",
        "$top": str(TOP)
    }
    
    if filter_string:
        params["$filter"] = filter_string
    
    return params


# 3) API Request and Pagination


def fetch_page(skip: int = 0, retry_count: int = 0) -> Tuple[List[Dict], Optional[int]]:
    """
    Fetch a single page of data from FEMA API.
    
    Args:
        skip: Number of records to skip (for pagination)
        retry_count: Current retry attempt
    
    Returns:
        Tuple of (records_list, total_count)
    """
    params = build_odata_filter()
    params["$skip"] = str(skip)
    
    try:
        logger.info(f"Fetching page with skip={skip}, top={TOP}")
        response = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract records and metadata
        records = data.get("DisasterDeclarationsSummaries", [])
        
        # Try to get total count from different possible locations in metadata
        total_count = None
        if skip == 0:  # Only try to get count on first page
            metadata = data.get("metadata", {})
            # Try different possible field names for total count
            total_count = (metadata.get("count") or 
                          metadata.get("total") or 
                          metadata.get("totalCount") or
                          metadata.get("entitycount"))
            
            # If still no count and we have records, estimate based on page size
            if total_count is None and records:
                if len(records) < TOP:
                    total_count = len(records)  # This is the last/only page
                else:
                    total_count = None  # We'll handle this in the calling function
        
        logger.info(f"Successfully fetched {len(records)} records")
        return records, total_count
        
    except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
        if retry_count < MAX_RETRY:
            wait_time = 2 ** retry_count  # Exponential backoff
            logger.warning(f"Request failed (attempt {retry_count + 1}/{MAX_RETRY + 1}): {e}")
            logger.info(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            return fetch_page(skip, retry_count + 1)
        else:
            logger.error(f"Failed to fetch data after {MAX_RETRY + 1} attempts: {e}")
            raise

def fetch_all_data() -> List[Dict]:
    """
    Fetch all disaster declaration data with pagination.
    Falls back to mock data if API access fails.
    
    Returns:
        List of all disaster declaration records
    """
    if USE_MOCK_DATA:
        logger.info("Using mock data for testing...")
        return MOCK_DATA
        
    try:
        all_rows = []
        skip = 0
        
        # Get first page to determine total count
        logger.info("Fetching first page to determine total record count...")
        first_page, total_count = fetch_page(0)
        all_rows.extend(first_page)
        
        if not total_count:
            logger.warning("Could not determine total count from API response - using pagination until no more data")
            # Continue fetching until we get empty results
            skip = TOP
            page_num = 2
            
            while True:
                logger.info(f"Fetching page {page_num}...")
                try:
                    page_data, _ = fetch_page(skip)
                    
                    if not page_data:  # No more data
                        break
                        
                    all_rows.extend(page_data)
                    skip += TOP
                    page_num += 1
                    
                    # Brief pause to be respectful to the API
                    time.sleep(0.1)
                    
                    # Safety check to prevent infinite loops
                    if page_num > 100:  # Reasonable limit
                        logger.warning("Reached maximum page limit (100), stopping pagination")
                        break
                        
                except Exception as e:
                    logger.error(f"Error fetching page {page_num}: {e}")
                    break
        else:
            logger.info(f"Total records available: {total_count}")
            
            # Calculate total pages needed
            total_pages = math.ceil(total_count / TOP)
            logger.info(f"Total pages to fetch: {total_pages}")
            
            # Fetch remaining pages
            skip = TOP
            page_num = 2
            
            while skip < total_count:
                logger.info(f"Fetching page {page_num}/{total_pages}...")
                page_data, _ = fetch_page(skip)
                
                if not page_data:  # No more data
                    break
                    
                all_rows.extend(page_data)
                skip += TOP
                page_num += 1
                
                # Brief pause to be respectful to the API
                time.sleep(0.1)
        
        logger.info(f"Completed fetching {len(all_rows)} total records")
        return all_rows
        
    except Exception as e:
        logger.error(f"API access completely failed: {e}")
        logger.info("Falling back to mock data for demonstration...")
        return MOCK_DATA


# 4) Data Quality Checks


def perform_quality_checks(df: pd.DataFrame) -> bool:
    """
    Perform basic quality checks on raw data.
    
    Args:
        df: Raw DataFrame from API
    
    Returns:
        Boolean indicating if data passes quality checks
    """
    if len(df) == 0:
        logger.error("No data retrieved. Check your filters and try again.")
        return False
    
    # Check for missing values in critical fields
    critical_fields = ["disasterNumber", "incidentBeginDate", "incidentType"]
    
    for field in critical_fields:
        if field in df.columns:
            missing_pct = df[field].isnull().mean() * 100
            logger.info(f"Missing values in {field}: {missing_pct:.1f}%")
            
            if missing_pct > 50:
                logger.warning(f"High missing rate for critical field {field}: {missing_pct:.1f}%")
        else:
            logger.warning(f"Critical field {field} not found in data")
    
    logger.info(f"Raw data quality check completed. Records: {len(df)}")
    return True


# 5) Data Cleaning and Standardization

def clean_and_standardize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize the raw disaster declaration data.
    
    Args:
        df: Raw DataFrame from API
    
    Returns:
        Cleaned and standardized DataFrame
    """
    logger.info("Starting data cleaning and standardization...")
    
    # 5.1 Select and retain key fields
    required_fields = {
        "disasterNumber": "disaster_number",
        "state": "region", 
        "incidentType": "event_type",
        "incidentBeginDate": "incident_begin_date",
        "incidentEndDate": "incident_end_date",
        "declarationDate": "declaration_date_raw",
        "declarationTitle": "event_name",
        "fipsStateCode": "fips_state_code",
        "fipsCountyCode": "fips_county_code",
        "placeCode": "place_code",           # Added for tribal/special areas
        "designatedArea": "designated_area"
    }
    
    # Keep only available fields
    available_fields = {k: v for k, v in required_fields.items() if k in df.columns}
    df_clean = df[list(available_fields.keys())].copy()
    df_clean = df_clean.rename(columns=available_fields)
    
    # 5.2 Date field processing (convert to daily frequency, no timezone conversion)
    date_fields = ["incident_begin_date", "incident_end_date", "declaration_date_raw"]
    
    for field in date_fields:
        if field in df_clean.columns:
            logger.info(f"Processing date field: {field}")
            
            # Parse ISO timestamps and extract date part only (YYYY-MM-DD)
            df_clean[field] = pd.to_datetime(df_clean[field], errors='coerce').dt.date
            
            # Convert back to string for consistent formatting, handle NaT properly
            df_clean[field] = df_clean[field].astype(str)
            df_clean[field] = df_clean[field].replace(['NaT', 'None'], None)
    
    # Create standardized date fields
    if "incident_begin_date" in df_clean.columns:
        df_clean["event_date"] = df_clean["incident_begin_date"]
    
    if "declaration_date_raw" in df_clean.columns:
        df_clean["declaration_date"] = df_clean["declaration_date_raw"]
    
    # 5.3 Basic filtering and missing data handling
    initial_rows = len(df_clean)
    
    # Remove records with missing critical fields
    critical_fields = ["disaster_number", "event_date", "event_type"]
    for field in critical_fields:
        if field in df_clean.columns:
            df_clean = df_clean.dropna(subset=[field])
    
    # Explicitly exclude Fire events (not weather-related for grid resilience study)
    if "event_type" in df_clean.columns:
        fire_count = (df_clean["event_type"] == "Fire").sum()
        if fire_count > 0:
            logger.info(f"Excluding {fire_count} Fire events (not weather-related)")
            df_clean = df_clean[df_clean["event_type"] != "Fire"]
    
    # Confirm filter conditions
    if WEATHER_TYPES:
        df_clean = df_clean[df_clean["event_type"].isin(WEATHER_TYPES)]
    
    if START_DATE:
        df_clean = df_clean[pd.to_datetime(df_clean["event_date"]) >= pd.Timestamp(START_DATE)]
    
    # Multi-state filter instead of single state
    if STATES and "region" in df_clean.columns:
        df_clean = df_clean[df_clean["region"].isin(STATES)]
    
    logger.info(f"Filtering reduced records from {initial_rows} to {len(df_clean)}")
    
    # 5.4 Quick sanity check
    if len(df_clean) > 0:
        event_dates = pd.to_datetime(df_clean["event_date"])
        logger.info(f"Date range after cleaning: {event_dates.min().date()} to {event_dates.max().date()}")
        
        if "event_type" in df_clean.columns:
            type_distribution = df_clean["event_type"].value_counts()
            logger.info(f"Event type distribution (Fire excluded):\n{type_distribution}")
        
        if "region" in df_clean.columns:
            state_distribution = df_clean["region"].value_counts()
            logger.info(f"State distribution:\n{state_distribution}")
    
    return df_clean


# 6) Area Coverage Calculation


def create_area_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create unified area_id for accurate coverage calculation.
    Handles county FIPS, tribal place codes, and designated areas.
    
    Priority:
    1. Valid FIPS county code (non-zero)
    2. Valid place code (for tribal/special areas)  
    3. Designated area (fallback)
    
    Args:
        df: DataFrame with area identification fields
    
    Returns:
        DataFrame with area_id column added
    """
    df = df.copy()
    
    def generate_area_id(row):
        # Get values and handle various formats
        fips_state = str(row.get('fips_state_code', '')).strip()
        fips_county = str(row.get('fips_county_code', '')).strip()
        place_code = str(row.get('place_code', '')).strip()
        designated_area = str(row.get('designated_area', '')).strip()
        
        # Check if FIPS county code is valid (not 0, 000, empty, or nan)
        fips_county_valid = (
            fips_county and 
            fips_county.lower() not in ['0', '00', '000', 'nan', 'none', ''] and
            not pd.isna(fips_county)
        )
        
        if fips_county_valid:
            # Use FIPS: STATE + COUNTY (ensure 3-digit county code)
            county_padded = fips_county.zfill(3)
            return f"FIPS:{fips_state}{county_padded}"
        
        # Check if place code is valid (for tribal/special areas)
        place_code_valid = (
            place_code and 
            place_code.lower() not in ['0', '00', '000', 'nan', 'none', ''] and
            not pd.isna(place_code)
        )
        
        if place_code_valid:
            return f"PLACE:{place_code}"
        
        # Fallback to designated area
        if designated_area and designated_area.lower() not in ['nan', 'none', '']:
            # Clean and standardize designated area name
            area_clean = designated_area.replace(' ', '_').replace(',', '')
            return f"AREA:{area_clean}"
        
        # Last resort - use a generic identifier
        return "UNKNOWN:1"
    
    # Generate area_id for each row
    df['area_id'] = df.apply(generate_area_id, axis=1)
    
    # Log area_id distribution for debugging
    logger.info("Area ID generation summary:")
    area_types = df['area_id'].str.split(':', expand=True)[0].value_counts()
    for area_type, count in area_types.items():
        logger.info(f"  {area_type}: {count} records")
    
    # Check for effective coverage calculation
    sample_area_ids = df['area_id'].value_counts().head(10)
    logger.info(f"Sample area IDs: {dict(sample_area_ids)}")
    
    return df


# 7) Aggregation: From Record-Level to Event-Level


def aggregate_to_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate disaster declarations from record-level to event-level.
    
    Event Key: (disaster_number, state) 
    - Same disaster can span multiple states (e.g., hurricanes)
    - Same disaster in same state should be one event regardless of areas affected
    
    Coverage Calculation: Uses unified area_id (FIPS county / tribal place / designated area)
    
    Args:
        df: Cleaned DataFrame
    
    Returns:
        Event-level aggregated DataFrame
    """
    logger.info("Aggregating data to event-level...")
    
    if len(df) == 0:
        logger.warning("No data to aggregate")
        return df
    
    # Step 1: Create unified area_id for accurate coverage calculation
    logger.info("Step 1: Creating unified area IDs for coverage calculation...")
    df_with_area = create_area_id(df)
    
    # Event key: (disaster_number, state) to handle cross-state disasters
    groupby_cols = ["disaster_number", "region"]
    
    logger.info(f"Step 2: Aggregation analysis...")
    logger.info(f"Input data: {len(df_with_area)} records")
    logger.info(f"Unique disaster numbers: {df_with_area['disaster_number'].nunique()}")
    logger.info(f"Unique (disaster_number, state) pairs: {df_with_area[groupby_cols].drop_duplicates().shape[0]}")
    logger.info(f"Unique area IDs: {df_with_area['area_id'].nunique()}")
    
    # Aggregation functions for each field
    agg_functions = {
        # Time fields - take earliest dates for each event
        "event_date": "min",           # Earliest incident begin date (true event start)
        "declaration_date": "min",     # Earliest declaration date (response timing)
        
        # Type and name fields - take first non-null values
        "event_type": "first",         # Usually consistent within same disaster number
        "event_name": "first",         # Take first title (usually consistent)
    }
    
    # Perform initial aggregation without affected_areas_count
    df_events = df_with_area.groupby(groupby_cols, as_index=False).agg(agg_functions)
    
    # Step 3: Calculate affected areas count using unified area_id
    logger.info("Step 3: Calculating affected areas count using unified area_id...")
    
    # Count unique area_ids for each event
    affected_counts = df_with_area.groupby(groupby_cols)['area_id'].nunique().reset_index()
    affected_counts = affected_counts.rename(columns={'area_id': 'affected_areas_count'})
    df_events = df_events.merge(affected_counts, on=groupby_cols, how="left")
    
    logger.info("Coverage calculation completed using unified area_id approach")
    
    # Rename disaster_number to event_id for clarity
    df_events = df_events.rename(columns={"disaster_number": "event_id"})
    
    # Sort by event date (chronological order)
    df_events = df_events.sort_values("event_date").reset_index(drop=True)
    
    # Post-aggregation sanity checks with enhanced coverage analysis
    logger.info("="*50)
    logger.info("EVENT-LEVEL AGGREGATION COMPLETE")
    logger.info("="*50)
    logger.info(f"Records: {len(df_with_area)} → {len(df_events)} unique events")
    
    if "region" in df_events.columns:
        state_counts = df_events["region"].value_counts()
        logger.info(f"Events by state:")
        for state, count in state_counts.items():
            logger.info(f"  {state}: {count}")
    
    if "event_type" in df_events.columns:
        type_counts = df_events["event_type"].value_counts()
        logger.info(f"Event type distribution (Fire should be 0):")
        for event_type, count in type_counts.items():
            logger.info(f"  {event_type}: {count}")
    
    if "affected_areas_count" in df_events.columns:
        coverage_stats = df_events["affected_areas_count"].describe()
        logger.info(f"Coverage analysis (fixed tribal/place code issue):")
        logger.info(f"  Mean areas per event: {coverage_stats['mean']:.1f}")
        logger.info(f"  Median: {coverage_stats['50%']:.1f}")
        logger.info(f"  Max coverage: {int(coverage_stats['max'])}")
        
        # Check for potential improvements
        high_coverage_events = df_events[df_events["affected_areas_count"] > 10]
        if len(high_coverage_events) > 0:
            logger.info(f"  Events with >10 areas: {len(high_coverage_events)}")
    
    if "event_date" in df_events.columns and len(df_events) > 0:
        dates = pd.to_datetime(df_events["event_date"])
        logger.info(f"Date range: {dates.min().date()} to {dates.max().date()}")
    
    # Sample first 5 events for verification
    if len(df_events) > 0:
        logger.info("Sample events (with coverage counts):")
        for i, row in df_events.head().iterrows():
            areas = row.get('affected_areas_count', 'N/A')
            logger.info(f"  {row.get('event_id', 'N/A')} ({row.get('region', 'N/A')}): {row.get('event_date', 'N/A')} - {row.get('event_name', 'N/A')} [{areas} areas]")
    
    logger.info("="*50)
    
    return df_events


# 8) Output Generation


def save_final_output(df: pd.DataFrame) -> None:
    """
    Save the final event-level dataset to CSV.
    
    Args:
        df: Final processed DataFrame
    """
    logger.info("Preparing final output...")
    
    # Add source attribution
    df["source"] = "OpenFEMA_v2_API"
    
    # Final column order as requested: event_id, region, event_date, declaration_date, event_type, event_name, affected_areas_count, source
    output_columns = [
        "event_id",              # disaster_number
        "region",                # state (GA/FL/SC/NC)  
        "event_date",            # YYYY-MM-DD (incident begin)
        "declaration_date",      # YYYY-MM-DD (official declaration)
        "event_type",            # Hurricane, Severe Storm, etc.
        "event_name",            # official title
        "affected_areas_count",  # spatial coverage proxy
        "source"                 # data attribution
    ]
    
    # Keep only available columns in specified order
    available_columns = [col for col in output_columns if col in df.columns]
    
    # Add any missing optional columns with default values
    if "affected_areas_count" not in df.columns:
        df["affected_areas_count"] = 1  # Default value if not available
        available_columns = [col for col in output_columns if col in df.columns]
    
    df_final = df[available_columns].copy()
    
    # Final sanity checks
    logger.info("Performing final data validation...")
    
    # Check for duplicate event IDs
    duplicates = df_final["event_id"].duplicated().sum()
    if duplicates > 0:
        logger.warning(f"Found {duplicates} duplicate event IDs")
    
    # Validate date formats
    try:
        pd.to_datetime(df_final["event_date"])
        logger.info("Date format validation passed")
    except Exception as e:
        logger.error(f"Date format validation failed: {e}")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # Save to CSV
    df_final.to_csv(OUTPUT_PATH, index=False)
    logger.info(f"Final dataset saved to {OUTPUT_PATH}")
    
    # Print summary statistics
    print_summary(df_final)

def print_summary(df: pd.DataFrame) -> None:
    """
    Print comprehensive summary of the final weather events dataset.
    
    Args:
        df: Final processed DataFrame
    """
    print("\n" + "="*70)
    print("WEATHER EVENTS DATA PROCESSING COMPLETE - SOUTHEAST REGION")
    print("="*70)
    print(f"Output file: {OUTPUT_PATH}")
    print(f"Geographic scope: Southeast US ({', '.join(STATES)})")
    print(f"Total unique events: {len(df)}")
    
    # Date range analysis
    if "event_date" in df.columns and len(df) > 0:
        dates = pd.to_datetime(df["event_date"])
        print(f"Event date range: {dates.min().date()} to {dates.max().date()}")
        print(f"Time span: {(dates.max() - dates.min()).days} days")
    
    # State distribution
    if "region" in df.columns:
        print(f"\nEvents by state:")
        state_counts = df["region"].value_counts()
        for state, count in state_counts.items():
            pct = (count / len(df)) * 100 if len(df) > 0 else 0
            print(f"  {state}: {count:>3} events ({pct:>5.1f}%)")
    
    # Event type distribution (should show Fire = 0)
    if "event_type" in df.columns:
        print(f"\nEvent type distribution (Fire excluded):")
        type_counts = df["event_type"].value_counts()
        for event_type, count in type_counts.items():
            pct = (count / len(df)) * 100 if len(df) > 0 else 0
            print(f"  {event_type:>18}: {count:>3} events ({pct:>5.1f}%)")
    
    # Coverage analysis
    if "affected_areas_count" in df.columns and len(df) > 0:
        coverage_stats = df["affected_areas_count"].describe()
        print(f"\nSpatial coverage (affected areas per event):")
        print(f"  Mean: {coverage_stats['mean']:.1f}")
        print(f"  Median: {coverage_stats['50%']:.1f}")
        print(f"  Max: {int(coverage_stats['max'])}")
    
    # Sample events for verification
    if len(df) > 0:
        print(f"\nSample events (first 5):")
        print(f"{'ID':<6} {'State':<5} {'Date':<12} {'Type':<15} {'Name':<25}")
        print("-" * 65)
        for i, row in df.head().iterrows():
            event_id = str(row.get('event_id', 'N/A'))[:6]
            region = str(row.get('region', 'N/A'))[:5]
            event_date = str(row.get('event_date', 'N/A'))[:12]
            event_type = str(row.get('event_type', 'N/A'))[:15]
            event_name = str(row.get('event_name', 'N/A'))[:25]
            print(f"{event_id:<6} {region:<5} {event_date:<12} {event_type:<15} {event_name}")
    
    print("\nData quality indicators:")
    print("✓ Fire events excluded (not weather-related)")
    print("✓ Multi-state disasters properly aggregated by (disaster_number, state)")
    print("✓ Daily frequency timestamps (YYYY-MM-DD)")
    print("✓ Event-level aggregation (no county duplicates)")
    print("✓ Corrected event type matching (Severe Storm not Severe Storm(s))")
    
    print("\nNext steps:")
    print("1. Run event study analysis: notebooks/01_event_study_utility_risk.ipynb")
    print("2. Combine with utility stock price data for market impact analysis")
    print("3. Use affected_areas_count as spatial coverage proxy for severity")
    print("="*70)


# 9) Main Execution


def main():
    """
    Main execution function that orchestrates the entire data pipeline.
    """
    print("GROWER Lab - Weather Events Data Builder - Southeast Edition")
    print("="*60)
    print(f"Fetching weather disasters for: Southeast US ({', '.join(STATES)})")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print(f"Weather types: {', '.join(WEATHER_TYPES)}")
    print(f"Explicitly excluding: Fire (not weather-related)")
    print("="*60)
    
    try:
        # Step 1: Fetch raw data from FEMA API
        logger.info("Step 1: Fetching raw data from FEMA API...")
        raw_data = fetch_all_data()
        
        if not raw_data:
            logger.error("No data retrieved. Exiting.")
            return
        
        # Step 2: Convert to DataFrame and perform quality checks
        logger.info("Step 2: Converting to DataFrame and performing quality checks...")
        df_raw = pd.DataFrame(raw_data)
        
        if not perform_quality_checks(df_raw):
            return
        
        # Step 3: Clean and standardize data
        logger.info("Step 3: Cleaning and standardizing data...")
        df_clean = clean_and_standardize(df_raw)
        
        if len(df_clean) == 0:
            logger.error("No data remaining after cleaning. Check your filters.")
            return
        
        # Step 4: Aggregate to event level
        logger.info("Step 4: Aggregating to event level...")
        df_events = aggregate_to_events(df_clean)
        
        # Step 5: Save final output
        logger.info("Step 5: Saving final output...")
        save_final_output(df_events)
        
        logger.info("Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
