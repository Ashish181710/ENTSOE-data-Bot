# ENTSO-E Streamlit App

Streamlit app to fetch ENTSO-E generation, load, and energy price datasets, preview them in the browser, and download the results as CSV, Parquet, ZIP, or Excel files.

## Features

- Fetch generation datasets from ENTSO-E
- Fetch load datasets from ENTSO-E
- Fetch day-ahead energy prices from ENTSO-E
- Export data in original, 15-minute, 60-minute, or both granularities
- Fast mode for very large datasets to keep the app responsive
- Fetch country totals and bidding-zone data where available
- Download each dataset as CSV
- Download each dataset as Parquet
- Download all results together as a ZIP file
- Download all fetched results as an Excel workbook
- Keep personal defaults and API token outside the main application code

## Project Files

- `entsoe_generation_streamlit_app.py`: Main Streamlit application
- `requirements.txt`: Python dependencies
- `user_settings.toml.example`: Example personal settings file
- `.streamlit/secrets.toml.example`: Example file for your ENTSO-E API token
- `app_config.py`: Loads personal settings from a separate file

## Requirements

- Python 3.10 or newer recommended
- ENTSO-E Transparency Platform API token

## Setup

### 1. Install dependencies

```bash
python -m pip install -r requirements.txt
```

### 2. Add your ENTSO-E API token

Create this file:

```text
.streamlit/secrets.toml
```

Add:

```toml
ENTSOE_API_TOKEN = "your_entsoe_token_here"
```

You can copy the example file:

```bash
copy .streamlit\secrets.toml.example .streamlit\secrets.toml
```

Then replace the placeholder token with your real token.

### 3. Add your personal defaults

Create this file:

```text
user_settings.toml
```

You can copy the example file:

```bash
copy user_settings.toml.example user_settings.toml
```

Example:

```toml
default_country = "Austria"
default_psr_label = "All production types"
default_granularity = "Both (15M and 60M)"
default_lookback_days = 7
default_theme = "Light"

default_categories = [
  "actual_total_load",
  "load_forecast_day_ahead",
  "actual_generation_per_type",
]
```

## Run the App

```bash
python -m streamlit run entsoe_generation_streamlit_app.py
```

After starting, Streamlit will show a local URL such as:

```text
http://localhost:8501
```

Open that in your browser.

## How It Works

1. Choose country, production type, date range, datasets, and output granularity.
2. Optionally enable Fast mode for large results and adjust the preview threshold.
3. Choose whether to fetch all zones, selected zones, and/or the country total.
4. Click `Fetch ENTSO-E data`.
5. Preview the returned datasets in the app.
6. Download individual CSV or Parquet files, one ZIP, or one Excel workbook containing all outputs.

## Notes

- `user_settings.toml` is for personal preferences only.
- `.streamlit/secrets.toml` is for your private API token.
- Do not commit your real token file to a shared repository.
- Some ENTSO-E endpoints may return large datasets or may have time-range limits.
- The app automatically chunks heavy or one-year-limited endpoints across longer date ranges where possible.
- Fast mode shows only a preview table for very large datasets, while downloads still contain the full data.
- Output timestamps are normalized to UTC in the exported data.
- The app includes both Light and Dark display modes.

## Troubleshooting

### `Missing API token`

Make sure `.streamlit/secrets.toml` exists and contains:

```toml
ENTSOE_API_TOKEN = "your_entsoe_token_here"
```

### `streamlit is not recognized`

Install dependencies first:

```bash
python -m pip install -r requirements.txt
```

### No data returned

- Check whether the selected country and dataset are available for the chosen dates
- Try a shorter date range
- Try `Original only` granularity first if you are debugging output shape

## Quick Start

```bash
python -m pip install -r requirements.txt
copy .streamlit\secrets.toml.example .streamlit\secrets.toml
copy user_settings.toml.example user_settings.toml
python -m streamlit run entsoe_generation_streamlit_app.py
```
