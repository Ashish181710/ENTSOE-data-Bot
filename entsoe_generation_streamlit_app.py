import io
import os
import zipfile
from datetime import date, timedelta
from xml.etree import ElementTree as ET
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import streamlit as st
from entsoe import EntsoePandasClient
from entsoe.mappings import lookup_area
from app_config import load_app_settings
from streamlit.errors import StreamlitSecretNotFoundError

st.set_page_config(page_title="ENTSO-E Generation Data Bot", page_icon="⚡", layout="wide")


ENTSOE_TIMEZONE = "Europe/Brussels"
UTC_TIMEZONE = "UTC"

COUNTRY_OPTIONS: Dict[str, str] = {
    "Austria": "AT",
    "Belgium": "BE",
    "Bulgaria": "BG",
    "Croatia": "HR",
    "Czechia": "CZ",
    "Denmark": "DK",
    "Estonia": "EE",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE_LU",
    "Greece": "GR",
    "Hungary": "HU",
    "Ireland": "IE_SEM",
    "Italy": "IT",
    "Latvia": "LV",
    "Lithuania": "LT",
    "Luxembourg": "LU",
    "Netherlands": "NL",
    "North Macedonia": "MK",
    "Norway": "NO_1",
    "Poland": "PL",
    "Portugal": "PT",
    "Romania": "RO",
    "Serbia": "RS",
    "Slovakia": "SK",
    "Slovenia": "SI",
    "Spain": "ES",
    "Sweden": "SE_1",
    "Switzerland": "CH",
    "United Kingdom": "GB",
}

COUNTRY_DOMAIN_CONFIG: Dict[str, Dict[str, List[str]]] = {
    "Austria": {"total": ["AT"], "zones": ["AT"]},
    "Belgium": {"total": ["BE"], "zones": ["BE"]},
    "Bulgaria": {"total": ["BG"], "zones": ["BG"]},
    "Croatia": {"total": ["HR"], "zones": ["HR"]},
    "Czechia": {"total": ["CZ"], "zones": ["CZ"]},
    "Denmark": {"total": ["DK"], "zones": ["DK_1", "DK_2"]},
    "Estonia": {"total": ["EE"], "zones": ["EE"]},
    "Finland": {"total": ["FI"], "zones": ["FI"]},
    "France": {"total": ["FR"], "zones": ["FR"]},
    "Germany": {"total": ["DE_LU"], "zones": ["DE_LU"]},
    "Greece": {"total": ["GR"], "zones": ["GR"]},
    "Hungary": {"total": ["HU"], "zones": ["HU"]},
    "Ireland": {"total": ["IE_SEM"], "zones": ["IE_SEM"]},
    "Italy": {
        "total": ["IT"],
        "zones": [
            "10Y1001A1001A70O",
            "10Y1001A1001A71M",
            "10Y1001A1001A788",
            "10Y1001C--00096J",
            "10Y1001A1001A74G",
            "10Y1001A1001A73I",
            "10Y1001A1001A75E",
        ],
    },
    "Latvia": {"total": ["LV"], "zones": ["LV"]},
    "Lithuania": {"total": ["LT"], "zones": ["LT"]},
    "Luxembourg": {"total": ["LU"], "zones": ["LU"]},
    "Netherlands": {"total": ["NL"], "zones": ["NL"]},
    "North Macedonia": {"total": ["MK"], "zones": ["MK"]},
    "Norway": {"total": ["NO"], "zones": ["NO_1", "NO_2", "NO_3", "NO_4", "NO_5"]},
    "Poland": {"total": ["PL"], "zones": ["PL"]},
    "Portugal": {"total": ["PT"], "zones": ["PT"]},
    "Romania": {"total": ["RO"], "zones": ["RO"]},
    "Serbia": {"total": ["RS"], "zones": ["RS"]},
    "Slovakia": {"total": ["SK"], "zones": ["SK"]},
    "Slovenia": {"total": ["SI"], "zones": ["SI"]},
    "Spain": {"total": ["ES"], "zones": ["ES"]},
    "Sweden": {"total": ["SE"], "zones": ["SE_1", "SE_2", "SE_3", "SE_4"]},
    "Switzerland": {"total": ["CH"], "zones": ["CH"]},
    "United Kingdom": {"total": ["GB"], "zones": ["GB"]},
}

ZONE_LABELS: Dict[str, str] = {
    "10Y1001A1001A70O": "IT_CNOR",
    "10Y1001A1001A71M": "IT_CSUD",
    "10Y1001A1001A788": "IT_SUD",
    "10Y1001C--00096J": "IT_CALA",
    "10Y1001A1001A74G": "IT_SARD",
    "10Y1001A1001A73I": "IT_NORD",
    "10Y1001A1001A75E": "IT_SICI",
    "SE_1": "SE1",
    "SE_2": "SE2",
    "SE_3": "SE3",
    "SE_4": "SE4",
    "NO_1": "NO1",
    "NO_2": "NO2",
    "NO_3": "NO3",
    "NO_4": "NO4",
    "NO_5": "NO5",
    "DK_1": "DK1",
    "DK_2": "DK2",
    "IT": "Italy Total",
    "SE": "Sweden Total",
    "NO": "Norway Total",
    "DK": "Denmark Total",
    "FR": "France Total",
    "DE_LU": "Germany Total",
    "ES": "Spain Total",
    "PT": "Portugal Total",
    "NL": "Netherlands Total",
    "BE": "Belgium Total",
    "AT": "Austria Total",
    "PL": "Poland Total",
    "CZ": "Czechia Total",
    "SK": "Slovakia Total",
    "SI": "Slovenia Total",
    "HU": "Hungary Total",
    "GR": "Greece Total",
    "RO": "Romania Total",
    "BG": "Bulgaria Total",
    "HR": "Croatia Total",
    "FI": "Finland Total",
    "EE": "Estonia Total",
    "LV": "Latvia Total",
    "LT": "Lithuania Total",
    "CH": "Switzerland Total",
    "IE_SEM": "Ireland Total",
    "GB": "United Kingdom Total",
    "LU": "Luxembourg Total",
    "MK": "North Macedonia Total",
    "RS": "Serbia Total",
}

PSR_TYPE_OPTIONS: Dict[str, Optional[str]] = {
    "All production types": None,
    "Biomass": "B01",
    "Fossil Brown coal/Lignite": "B02",
    "Fossil Coal-derived gas": "B03",
    "Fossil Gas": "B04",
    "Fossil Hard coal": "B05",
    "Fossil Oil": "B06",
    "Fossil Oil shale": "B07",
    "Fossil Peat": "B08",
    "Geothermal": "B09",
    "Hydro Pumped Storage": "B10",
    "Hydro Run-of-river and poundage": "B11",
    "Hydro Water Reservoir": "B12",
    "Marine": "B13",
    "Nuclear": "B14",
    "Other renewable": "B15",
    "Solar": "B16",
    "Waste": "B17",
    "Wind Offshore": "B18",
    "Wind Onshore": "B19",
    "Other": "B20",
    "AC Link": "B21",
    "DC Link": "B22",
    "Substation": "B23",
    "Transformer": "B24",
}

CATEGORY_LABELS: Dict[str, str] = {
    "installed_capacity_per_type": "14.1.A Installed Capacity per Production Type",
    "water_reservoirs_hydro": "16.1.D Water Reservoirs and Hydro Storage Plants",
    "actual_generation_per_type": "16.1.B&C Actual Generation per Production Type",
    "actual_generation_per_unit": "16.1.A Actual Generation per Generation Unit",
    "day_ahead_prices": "12.1.D Day-ahead Prices",
    "generation_forecast_day_ahead": "14.1.C Generation Forecast - Day ahead",
    "wind_solar_forecast": "14.1.D Generation Forecasts for Wind and Solar",
    "installed_capacity_per_unit": "14.1.B Installed Capacity Per Production Unit",
    "actual_total_load": "6.1.A Actual Total Load",
    "load_forecast_day_ahead": "6.1.B Day-ahead Total Load Forecast",
    "load_forecast_week_ahead": "6.1.C Week-ahead Total Load Forecast",
    "load_forecast_month_ahead": "6.1.D Month-ahead Total Load Forecast",
    "load_forecast_year_ahead": "6.1.E Year-ahead Total Load Forecast",
    "forecast_margin_year_ahead": "8.1 Year-ahead Forecast Margin",
}

DEFAULT_CATEGORIES = list(CATEGORY_LABELS.keys())
GRANULARITY_OPTIONS = {
    "15M + 60M": ["15min", "60min"],
    "15M": ["15min"],
    "60M": ["60min"],
    "Original": [],
}
THEME_OPTIONS = ["Light", "Dark"]
FAST_MODE_PREVIEW_ROWS = 200
FAST_MODE_ROW_THRESHOLD = 50000


def flatten_columns(columns: pd.Index) -> List[str]:
    flattened: List[str] = []
    for col in columns:
        if isinstance(col, tuple):
            parts = [str(part) for part in col if part not in ("", None)]
            flattened.append(" | ".join(parts))
        else:
            flattened.append(str(col))
    return flattened


def unique_in_order(values: List[str]) -> List[str]:
    return list(dict.fromkeys(values))


def get_default_multiselect_values(options: List[str], selected_value: str) -> List[str]:
    if selected_value in options:
        return [selected_value]
    if options:
        return [options[0]]
    return []


def make_utc_day_start(value: date) -> pd.Timestamp:
    return pd.Timestamp(value).tz_localize(UTC_TIMEZONE)


def make_entsoe_query_timestamp(value: pd.Timestamp) -> pd.Timestamp:
    return value.tz_convert(ENTSOE_TIMEZONE)


def split_query_period(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    chunk_days: int,
) -> List[tuple[pd.Timestamp, pd.Timestamp]]:
    periods: List[tuple[pd.Timestamp, pd.Timestamp]] = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + pd.Timedelta(days=chunk_days), end_date)
        periods.append((current_start, current_end))
        current_start = current_end

    return periods


def is_chunk_retryable_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "503" in message or "service unavailable" in message or "504" in message


def combine_chunk_frames(frames: List[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        return combined

    duplicate_mask = combined.duplicated()
    if duplicate_mask.any():
        combined = combined.loc[~duplicate_mask].reset_index(drop=True)
    else:
        combined = combined.reset_index(drop=True)

    return combined


def fetch_dataframe_in_chunks(
    fetch_chunk: Callable[[pd.Timestamp, pd.Timestamp], Any],
    normalize_chunk: Callable[[Any], pd.DataFrame],
    *,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    utc_start: pd.Timestamp,
    utc_end_exclusive: pd.Timestamp,
    initial_chunk_days: int,
    min_chunk_days: int = 1,
) -> pd.DataFrame:
    pending_periods = split_query_period(start_date, end_date, chunk_days=initial_chunk_days)
    frames: List[pd.DataFrame] = []

    while pending_periods:
        chunk_start, chunk_end = pending_periods.pop(0)
        try:
            chunk_result = fetch_chunk(chunk_start, chunk_end)
        except Exception as exc:
            chunk_length_days = max(
                1,
                int((chunk_end - chunk_start) / pd.Timedelta(days=1)),
            )
            if chunk_length_days <= min_chunk_days or not is_chunk_retryable_error(exc):
                raise

            split_days = max(min_chunk_days, chunk_length_days // 2)
            split_point = min(chunk_end, chunk_start + pd.Timedelta(days=split_days))
            if split_point <= chunk_start or split_point >= chunk_end:
                raise

            pending_periods = [
                (chunk_start, split_point),
                (split_point, chunk_end),
                *pending_periods,
            ]
            continue

        normalized_chunk = normalize_chunk(chunk_result)
        filtered_chunk = filter_dataframe_to_utc_window(
            normalized_chunk,
            utc_start,
            utc_end_exclusive,
        )
        if not filtered_chunk.empty:
            frames.append(filtered_chunk)

    return combine_chunk_frames(frames)


def granularity_label(freq: str) -> str:
    return "15M" if freq == "15min" else "60M"


def get_selectbox_index(options: List[str], selected_value: str, fallback: int = 0) -> int:
    try:
        return options.index(selected_value)
    except ValueError:
        return fallback


def local_name(tag: str) -> str:
    return tag.split("}", 1)[-1]


def zone_label(zone_code: str) -> str:
    return ZONE_LABELS.get(zone_code, zone_code)


def sanitize_filename_part(value: str) -> str:
    cleaned = value.lower().replace(" ", "_").replace(".", "").replace("/", "_")
    cleaned = cleaned.replace("(", "").replace(")", "").replace("-", "_")
    return cleaned


def make_safe_sheet_name(value: str) -> str:
    cleaned = value.replace("/", "_").replace("\\", "_").replace(":", " ")
    cleaned = cleaned.replace("*", "").replace("?", "").replace("[", "(").replace("]", ")")
    return cleaned[:31]


def get_theme_palette(theme_mode: str) -> Dict[str, str]:
    if theme_mode == "Dark":
        return {
            "bg": "#08111f",
            "bg_soft": "#0f1b2d",
            "panel": "rgba(11, 24, 42, 0.88)",
            "panel_border": "rgba(125, 211, 252, 0.28)",
            "text": "#e5eefb",
            "muted": "#9fb3cc",
            "accent": "#7dd3fc",
            "accent_strong": "#38bdf8",
            "grid": "rgba(159, 179, 204, 0.16)",
            "input_bg": "#0f172a",
            "chip_bg": "#1e293b",
            "chip_text": "#f8fafc",
            "button_text": "#062033",
            "menu_bg": "#0f172a",
            "menu_text": "#e5eefb",
            "csv_button_bg": "#153b33",
            "csv_button_text": "#b8f3df",
            "csv_button_border": "#2e7d68",
            "csv_button_hover_bg": "#1d5145",
            "csv_button_hover_text": "#d9fff2",
            "csv_button_hover_border": "#46a388",
            "zip_button_bg": "#162f5f",
            "zip_button_text": "#c6dbff",
            "zip_button_border": "#4068b8",
            "zip_button_hover_bg": "#204282",
            "zip_button_hover_text": "#e1edff",
            "zip_button_hover_border": "#5b88e5",
            "excel_button_bg": "#4b3214",
            "excel_button_text": "#ffd8a8",
            "excel_button_border": "#9e6a2b",
            "excel_button_hover_bg": "#65441a",
            "excel_button_hover_text": "#ffe8c7",
            "excel_button_hover_border": "#c58a42",
            "parquet_button_bg": "#31204f",
            "parquet_button_text": "#dfd2ff",
            "parquet_button_border": "#7a5cc4",
            "parquet_button_hover_bg": "#402b67",
            "parquet_button_hover_text": "#efe7ff",
            "parquet_button_hover_border": "#9a7be3",
        }
    return {
        "bg": "#f4f7fb",
        "bg_soft": "#e8f1fb",
        "panel": "rgba(255, 255, 255, 0.92)",
        "panel_border": "rgba(37, 99, 235, 0.26)",
        "text": "#142033",
        "muted": "#5f6f86",
        "accent": "#1d4ed8",
        "accent_strong": "#0f766e",
        "grid": "rgba(95, 111, 134, 0.14)",
        "input_bg": "#ffffff",
        "chip_bg": "#dbeafe",
        "chip_text": "#142033",
        "button_text": "#ffffff",
        "menu_bg": "#ffffff",
        "menu_text": "#142033",
        "csv_button_bg": "#dff3e8",
        "csv_button_text": "#175c45",
        "csv_button_border": "#8fd1b4",
        "csv_button_hover_bg": "#cdebdc",
        "csv_button_hover_text": "#124a37",
        "csv_button_hover_border": "#73b99b",
        "zip_button_bg": "#dbe8ff",
        "zip_button_text": "#1f4db8",
        "zip_button_border": "#9fbcff",
        "zip_button_hover_bg": "#cddfff",
        "zip_button_hover_text": "#183f98",
        "zip_button_hover_border": "#84a8ff",
        "excel_button_bg": "#ffe7c7",
        "excel_button_text": "#995200",
        "excel_button_border": "#f4c27f",
        "excel_button_hover_bg": "#ffddb2",
        "excel_button_hover_text": "#7d4300",
        "excel_button_hover_border": "#eca952",
        "parquet_button_bg": "#efe7ff",
        "parquet_button_text": "#5b3db4",
        "parquet_button_border": "#c5b2ff",
        "parquet_button_hover_bg": "#e3d7ff",
        "parquet_button_hover_text": "#4a2f98",
        "parquet_button_hover_border": "#af96ff",
    }


def apply_theme(theme_mode: str) -> Dict[str, str]:
    palette = get_theme_palette(theme_mode)
    st.markdown(
        f"""
        <style>
        [data-testid="stAppViewContainer"] {{
            background:
                radial-gradient(circle at top left, {palette["bg_soft"]} 0%, transparent 32%),
                linear-gradient(180deg, {palette["bg"]} 0%, {palette["bg_soft"]} 100%);
            color: {palette["text"]};
        }}
        [data-testid="stHeader"] {{
            background: transparent;
        }}
        .block-container {{
            max-width: 1500px;
            padding-top: 0.35rem;
            padding-bottom: 2rem;
        }}
        [data-testid="stMetric"], div[data-testid="stVerticalBlockBorderWrapper"] > div {{
            background: {palette["panel"]};
            border: 1.5px solid {palette["panel_border"]};
            border-radius: 18px;
            box-shadow: 0 16px 35px rgba(15, 23, 42, 0.08);
        }}
        [data-baseweb="select"] > div,
        [data-testid="stDateInput"] > div > div,
        [data-testid="stMultiSelect"] > div > div,
        [data-testid="stTextInput"] > div > div,
        [data-testid="stNumberInput"] > div > div {{
            background: {palette["input_bg"]} !important;
            border: 1.5px solid {palette["panel_border"]} !important;
            border-radius: 14px !important;
            color: {palette["text"]} !important;
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
        }}
        [data-testid="stNumberInput"] [data-baseweb="input"] {{
            background: {palette["input_bg"]} !important;
            color: {palette["text"]} !important;
        }}
        [data-testid="stNumberInput"] input {{
            color: {palette["text"]} !important;
            -webkit-text-fill-color: {palette["text"]} !important;
            opacity: 1 !important;
            caret-color: {palette["text"]} !important;
            background: {palette["input_bg"]} !important;
        }}
        [data-testid="stNumberInput"] input::placeholder {{
            color: {palette["text"]} !important;
            -webkit-text-fill-color: {palette["text"]} !important;
            opacity: 0.75 !important;
        }}
        [data-testid="stNumberInput"] [data-baseweb="input"] input {{
            color: {palette["text"]} !important;
            -webkit-text-fill-color: {palette["text"]} !important;
            opacity: 1 !important;
        }}
        [data-testid="stNumberInput"] button {{
            color: {palette["text"]} !important;
            background: {palette["input_bg"]} !important;
        }}
        [data-testid="stNumberInput"] button svg {{
            fill: {palette["text"]} !important;
        }}
        [data-baseweb="tag"] {{
            background: {palette["chip_bg"]} !important;
            border: 1px solid {palette["panel_border"]} !important;
            color: {palette["chip_text"]} !important;
            max-width: 26rem !important;
        }}
        [data-baseweb="tag"] span,
        [data-baseweb="tag"] svg {{
            color: {palette["chip_text"]} !important;
            fill: {palette["chip_text"]} !important;
        }}
        [data-baseweb="tag"] span {{
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }}
        input, textarea {{
            color: {palette["text"]} !important;
        }}
        [data-testid="stAppViewContainer"] div[data-testid="stMarkdownContainer"],
        [data-testid="stAppViewContainer"] label,
        [data-testid="stAppViewContainer"] p,
        [data-testid="stAppViewContainer"] span,
        [data-testid="stAppViewContainer"] h1,
        [data-testid="stAppViewContainer"] h2,
        [data-testid="stAppViewContainer"] h3 {{
            color: {palette["text"]};
        }}
        div[role="menu"],
        div[role="menu"] *,
        [data-testid="stToolbar"] [role="dialog"],
        [data-testid="stToolbar"] [role="dialog"] * {{
            color: {palette["menu_text"]} !important;
        }}
        div[role="menu"],
        [data-testid="stToolbar"] [role="dialog"] {{
            background: {palette["menu_bg"]} !important;
        }}
        [data-baseweb="tab-list"] {{
            gap: 0.4rem;
        }}
        [data-baseweb="tab"] {{
            background: {palette["panel"]};
            border: 1px solid {palette["panel_border"]};
            border-radius: 999px;
            color: {palette["text"]};
            padding: 0.45rem 0.9rem;
        }}
        [data-baseweb="tab"][aria-selected="true"] {{
            background: {palette["accent"]};
            color: white;
            border-color: {palette["accent"]};
        }}
        [data-testid="stRadio"] > div {{
            gap: 0.45rem;
        }}
        [data-testid="stRadio"] div[role="radiogroup"] {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.55rem;
        }}
        [data-testid="stRadio"] label {{
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            padding: 0.45rem 0.95rem;
            border-radius: 999px;
            border: 1px solid {palette["panel_border"]};
            background: {palette["panel"]};
            color: {palette["text"]};
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
            cursor: pointer;
            transition: background 0.18s ease, border-color 0.18s ease, color 0.18s ease;
        }}
        [data-testid="stRadio"] label:hover {{
            border-color: {palette["accent"]};
            color: {palette["accent"]};
        }}
        [data-testid="stRadio"] input[type="radio"] {{
            accent-color: {palette["accent"]};
        }}
        .stButton > button, .stDownloadButton > button {{
            background: {palette["accent"]};
            color: {palette["button_text"]};
            border: 1px solid {palette["panel_border"]};
            border-radius: 12px;
            font-weight: 600;
            box-shadow: 0 10px 22px rgba(15, 23, 42, 0.08);
        }}
        .stButton > button:hover, .stDownloadButton > button:hover {{
            background: {palette["accent_strong"]};
            color: white;
            border-color: {palette["accent_strong"]};
            filter: none;
        }}
        .stDownloadButton > button {{
            width: 100%;
        }}
        [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-of-type(1) .stDownloadButton > button {{
            background: {palette["csv_button_bg"]};
            color: {palette["csv_button_text"]};
            border-color: {palette["csv_button_border"]};
        }}
        [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-of-type(1) .stDownloadButton > button:hover {{
            background: {palette["csv_button_hover_bg"]};
            color: {palette["csv_button_hover_text"]};
            border-color: {palette["csv_button_hover_border"]};
        }}
        [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-of-type(2) .stDownloadButton > button {{
            background: {palette["zip_button_bg"]};
            color: {palette["zip_button_text"]};
            border-color: {palette["zip_button_border"]};
        }}
        [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-of-type(2) .stDownloadButton > button:hover {{
            background: {palette["zip_button_hover_bg"]};
            color: {palette["zip_button_hover_text"]};
            border-color: {palette["zip_button_hover_border"]};
        }}
        [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-of-type(3) .stDownloadButton > button {{
            background: {palette["excel_button_bg"]};
            color: {palette["excel_button_text"]};
            border-color: {palette["excel_button_border"]};
        }}
        [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-of-type(3) .stDownloadButton > button:hover {{
            background: {palette["excel_button_hover_bg"]};
            color: {palette["excel_button_hover_text"]};
            border-color: {palette["excel_button_hover_border"]};
        }}
        [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-of-type(4) .stDownloadButton > button {{
            background: {palette["parquet_button_bg"]};
            color: {palette["parquet_button_text"]};
            border-color: {palette["parquet_button_border"]};
        }}
        [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-of-type(4) .stDownloadButton > button:hover {{
            background: {palette["parquet_button_hover_bg"]};
            color: {palette["parquet_button_hover_text"]};
            border-color: {palette["parquet_button_hover_border"]};
        }}
        [data-testid="stDataFrame"] {{
            border-radius: 16px;
            overflow: hidden;
            border: 1px solid {palette["panel_border"]};
        }}
        .entsoe-hero {{
            padding: 1.3rem 1.5rem;
            border-radius: 24px;
            background: linear-gradient(135deg, {palette["panel"]} 0%, rgba(255,255,255,0.02) 100%);
            border: 1.5px solid {palette["panel_border"]};
            margin-bottom: 1rem;
            margin-top: -0.35rem;
        }}
        .entsoe-hero h1 {{
            margin: 0;
            font-size: 2rem;
            line-height: 1.1;
            color: {palette["text"]};
        }}
        .entsoe-hero .hero-credit {{
            display: inline-block;
            margin-left: 0.45rem;
            font-size: 0.78rem;
            font-style: italic;
            font-weight: 500;
            letter-spacing: 0.02em;
            color: {palette["muted"]};
            vertical-align: middle;
        }}
        .entsoe-hero p {{
            margin: 0.55rem 0 0 0;
            color: {palette["muted"]};
            font-size: 1rem;
        }}
        .theme-dock {{
            display: flex;
            justify-content: flex-end;
            align-items: center;
            margin-bottom: 0.65rem;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
    return palette


def convert_series_to_utc(series: pd.Series) -> pd.Series:
    converted = pd.to_datetime(series, errors="coerce", utc=True)
    if converted.notna().any():
        return converted
    return series


def ensure_utc_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in normalized.columns:
        if column == "timestamp" or "time" in column.lower() or "date" in column.lower():
            normalized[column] = convert_series_to_utc(normalized[column])
    return normalized


def get_api_key_from_config() -> Optional[str]:
    env_value = os.getenv("ENTSOE_API_TOKEN")
    if env_value:
        return env_value

    try:
        return st.secrets.get("ENTSOE_API_TOKEN")
    except StreamlitSecretNotFoundError:
        return None
    except Exception:
        return None


def prepare_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    for column in prepared.columns:
        if pd.api.types.is_datetime64tz_dtype(prepared[column]):
            prepared[column] = prepared[column].dt.strftime("%Y-%m-%d %H:%M:%S%z")
        elif pd.api.types.is_datetime64_any_dtype(prepared[column]):
            prepared[column] = prepared[column].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif pd.api.types.is_object_dtype(prepared[column]):
            prepared[column] = prepared[column].map(
                lambda value: value
                if value is None
                or isinstance(value, (str, int, float, bool))
                or pd.isna(value)
                else str(value)
            )
    return prepared


def parse_resolution_offset(resolution: str):
    mapping = {
        "PT15M": pd.Timedelta(minutes=15),
        "PT30M": pd.Timedelta(minutes=30),
        "PT60M": pd.Timedelta(hours=1),
        "P1D": pd.Timedelta(days=1),
        "P7D": pd.Timedelta(days=7),
        "P1M": pd.DateOffset(months=1),
        "P1Y": pd.DateOffset(years=1),
    }
    return mapping.get(resolution)


def build_point_timestamp(
    period_start: Optional[pd.Timestamp], position: Optional[int], resolution: Optional[str]
) -> Optional[pd.Timestamp]:
    if period_start is None or position is None or resolution is None:
        return period_start

    offset = parse_resolution_offset(resolution)
    if offset is None:
        return period_start

    timestamp = period_start
    for _ in range(max(position - 1, 0)):
        timestamp = timestamp + offset
    return timestamp


def convert_xml_value(text: Optional[str]) -> Any:
    if text is None:
        return None

    value = text.strip()
    if not value:
        return None

    as_dt = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.notna(as_dt):
        return as_dt

    as_num = pd.to_numeric(value, errors="coerce")
    if pd.notna(as_num):
        return as_num

    return value


def parse_entsoe_timeseries_xml(xml_text: str, category_key: str) -> pd.DataFrame:
    root = ET.fromstring(xml_text)
    records: List[Dict[str, Any]] = []

    for series in root.findall(".//{*}TimeSeries"):
        series_meta: Dict[str, Any] = {}
        for child in series:
            child_name = local_name(child.tag)
            if child_name == "Period":
                continue
            if list(child):
                text_children = [
                    grandchild
                    for grandchild in child
                    if not list(grandchild) and (grandchild.text or "").strip()
                ]
                if len(text_children) == 1:
                    key = f"{child_name}_{local_name(text_children[0].tag)}"
                    series_meta[key] = convert_xml_value(text_children[0].text)
                continue
            series_meta[child_name] = convert_xml_value(child.text)

        for period in series.findall("./{*}Period"):
            period_meta = series_meta.copy()
            resolution = period.findtext("./{*}resolution")
            interval_start = period.findtext("./{*}timeInterval/{*}start")
            interval_end = period.findtext("./{*}timeInterval/{*}end")

            period_start = pd.to_datetime(interval_start, utc=True, errors="coerce")
            period_end = pd.to_datetime(interval_end, utc=True, errors="coerce")

            period_meta["period_start"] = period_start if pd.notna(period_start) else interval_start
            period_meta["period_end"] = period_end if pd.notna(period_end) else interval_end
            period_meta["resolution"] = resolution

            points = period.findall("./{*}Point")
            if not points:
                records.append(period_meta)
                continue

            for point in points:
                row = period_meta.copy()
                position: Optional[int] = None
                for child in point:
                    key = local_name(child.tag)
                    value = convert_xml_value(child.text)
                    row[key] = value
                    if key == "position" and value is not None:
                        try:
                            position = int(value)
                        except (TypeError, ValueError):
                            position = None

                row["timestamp"] = build_point_timestamp(
                    period_start=period_start if pd.notna(period_start) else None,
                    position=position,
                    resolution=resolution,
                )
                records.append(row)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = ensure_utc_datetime_columns(df)
    df.insert(0, "dataset", CATEGORY_LABELS[category_key])
    return df


@st.cache_resource(show_spinner=False)
def get_client(api_key: str) -> EntsoePandasClient:
    return EntsoePandasClient(api_key=api_key)


@st.cache_data(show_spinner=False)
def normalize_result(result: Any, category_key: str) -> pd.DataFrame:
    if result is None:
        return pd.DataFrame()

    if isinstance(result, pd.Series):
        df = result.to_frame(name="value").reset_index()
    elif isinstance(result, pd.DataFrame):
        df = result.reset_index()
    else:
        try:
            df = pd.DataFrame(result).reset_index(drop=True)
        except Exception:
            df = pd.DataFrame({"raw_value": [str(result)]})

    df.columns = flatten_columns(df.columns)

    if "index" in df.columns:
        df = df.rename(columns={"index": "timestamp"})

    df = ensure_utc_datetime_columns(df)
    df.insert(0, "dataset", CATEGORY_LABELS[category_key])
    return df


def find_primary_time_column(df: pd.DataFrame) -> Optional[str]:
    for column in df.columns:
        if column == "timestamp":
            return column

    for column in df.columns:
        lowered = column.lower()
        if "time" in lowered or "date" in lowered:
            return column

    return None


def maybe_prepare_time_column(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    prepared = df.copy()
    prepared[time_col] = pd.to_datetime(prepared[time_col], errors="coerce")
    prepared = prepared.dropna(subset=[time_col])
    return prepared


def filter_dataframe_to_utc_window(
    df: pd.DataFrame,
    utc_start: pd.Timestamp,
    utc_end_exclusive: pd.Timestamp,
) -> pd.DataFrame:
    time_col = find_primary_time_column(df)
    if time_col is None or df.empty:
        return df

    filtered = df.copy()
    filtered[time_col] = pd.to_datetime(filtered[time_col], errors="coerce", utc=True)
    filtered = filtered.dropna(subset=[time_col])
    filtered = filtered[
        (filtered[time_col] >= utc_start) & (filtered[time_col] < utc_end_exclusive)
    ]
    return filtered.reset_index(drop=True)


def resample_timeseries_dataframe(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    time_col = find_primary_time_column(df)
    if time_col is None:
        return df.copy()

    prepared = maybe_prepare_time_column(df, time_col)
    if prepared.empty:
        return prepared

    numeric_cols = prepared.select_dtypes(include="number").columns.tolist()
    if not numeric_cols:
        return prepared

    metadata_cols = [col for col in prepared.columns if col not in numeric_cols + [time_col]]

    parts: List[pd.DataFrame] = []
    grouped = prepared.groupby(metadata_cols, dropna=False) if metadata_cols else [((), prepared)]

    for group_key, group_df in grouped:
        series_part = group_df.sort_values(time_col).set_index(time_col)
        resampled_numeric = series_part[numeric_cols].resample(freq).mean()

        if freq == "15min":
            resampled_numeric = resampled_numeric.ffill(limit=3)

        resampled_numeric = resampled_numeric.dropna(how="all").reset_index()
        if resampled_numeric.empty:
            continue

        if metadata_cols:
            if not isinstance(group_key, tuple):
                group_key = (group_key,)
            for column, value in zip(metadata_cols, group_key):
                resampled_numeric[column] = value

        parts.append(resampled_numeric)

    if not parts:
        return pd.DataFrame(columns=prepared.columns)

    combined = pd.concat(parts, ignore_index=True)
    ordered_columns = [col for col in df.columns if col in combined.columns]
    for column in combined.columns:
        if column not in ordered_columns:
            ordered_columns.append(column)
    return combined[ordered_columns]


def build_granularity_outputs(
    results: Dict[str, pd.DataFrame], selected_frequencies: List[str]
) -> Dict[str, pd.DataFrame]:
    outputs: Dict[str, pd.DataFrame] = {}

    for dataset_key, df in results.items():
        if df.empty:
            continue

        time_col = find_primary_time_column(df)
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        is_time_series = time_col is not None and bool(numeric_cols)

        if not is_time_series or not selected_frequencies:
            outputs[dataset_key] = df.copy()
            continue

        for freq in selected_frequencies:
            resampled = resample_timeseries_dataframe(df, freq)
            if resampled.empty:
                continue
            resampled["granularity"] = granularity_label(freq)
            if "dataset" in resampled.columns:
                resampled["dataset"] = (
                    resampled["dataset"].astype(str) + f" ({granularity_label(freq)})"
                )
            outputs[f"{dataset_key}_{granularity_label(freq)}"] = resampled

    return outputs


@st.cache_data(show_spinner=False)
def build_excel_from_results(results: Dict[str, pd.DataFrame], metadata: Dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss") as writer:
        pd.DataFrame({"Key": list(metadata.keys()), "Value": list(metadata.values())}).to_excel(
            writer, sheet_name="README", index=False
        )
        for label, df in results.items():
            export_df = prepare_dataframe_for_excel(df)
            export_df.columns = [str(column) for column in export_df.columns]
            export_df.to_excel(writer, sheet_name=make_safe_sheet_name(label), index=False)

    buffer.seek(0)
    return buffer.read()


@st.cache_data(show_spinner=False)
def build_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    export_df = prepare_dataframe_for_excel(df)
    export_df.columns = [str(column) for column in export_df.columns]
    export_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer.read()


@st.cache_data(show_spinner=False)
def fetch_dataset(
    api_key: str,
    country_code: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    utc_start: pd.Timestamp,
    utc_end_exclusive: pd.Timestamp,
    category_key: str,
    psr_type: Optional[str],
) -> pd.DataFrame:
    client = get_client(api_key)

    if category_key == "actual_generation_per_unit":
        return fetch_dataframe_in_chunks(
            lambda chunk_start, chunk_end: client.query_generation_per_plant(
                country_code,
                start=chunk_start,
                end=chunk_end,
                psr_type=psr_type,
            ),
            lambda result: normalize_result(result, category_key),
            start_date=start_date,
            end_date=end_date,
            utc_start=utc_start,
            utc_end_exclusive=utc_end_exclusive,
            initial_chunk_days=7,
            min_chunk_days=1,
        )

    if category_key == "day_ahead_prices":
        return fetch_dataframe_in_chunks(
            lambda chunk_start, chunk_end: client.query_day_ahead_prices(
                country_code,
                start=chunk_start,
                end=chunk_end,
            ),
            lambda result: normalize_result(result, category_key),
            start_date=start_date,
            end_date=end_date,
            utc_start=utc_start,
            utc_end_exclusive=utc_end_exclusive,
            initial_chunk_days=365,
            min_chunk_days=30,
        )

    if category_key in {
        "load_forecast_day_ahead",
        "load_forecast_week_ahead",
        "load_forecast_month_ahead",
        "load_forecast_year_ahead",
    }:
        process_type_map = {
            "load_forecast_day_ahead": "A01",
            "load_forecast_week_ahead": "A31",
            "load_forecast_month_ahead": "A32",
            "load_forecast_year_ahead": "A33",
        }
        return fetch_dataframe_in_chunks(
            lambda chunk_start, chunk_end: client.query_load_forecast(
                country_code,
                start=chunk_start,
                end=chunk_end,
                process_type=process_type_map[category_key],
            ),
            lambda result: normalize_result(result, category_key),
            start_date=start_date,
            end_date=end_date,
            utc_start=utc_start,
            utc_end_exclusive=utc_end_exclusive,
            initial_chunk_days=365,
            min_chunk_days=30,
        )

    if category_key == "forecast_margin_year_ahead":
        return fetch_dataframe_in_chunks(
            lambda chunk_start, chunk_end: client._base_request(
                params={
                    "documentType": "A70",
                    "processType": "A33",
                    "outBiddingZone_Domain": lookup_area(country_code).code,
                },
                start=chunk_start,
                end=chunk_end,
            ).text,
            lambda result_text: parse_entsoe_timeseries_xml(result_text, category_key),
            start_date=start_date,
            end_date=end_date,
            utc_start=utc_start,
            utc_end_exclusive=utc_end_exclusive,
            initial_chunk_days=365,
            min_chunk_days=30,
        )

    if category_key == "installed_capacity_per_type":
        result = client.query_installed_generation_capacity(
            country_code, start=start_date, end=end_date, psr_type=psr_type
        )
    elif category_key == "water_reservoirs_hydro":
        result = client.query_aggregate_water_reservoirs_and_hydro_storage(
            country_code, start=start_date, end=end_date
        )
    elif category_key == "actual_generation_per_type":
        result = client.query_generation(
            country_code, start=start_date, end=end_date, psr_type=psr_type
        )
    elif category_key == "generation_forecast_day_ahead":
        result = client.query_generation_forecast(
            country_code, start=start_date, end=end_date
        )
    elif category_key == "wind_solar_forecast":
        result = client.query_wind_and_solar_forecast(
            country_code, start=start_date, end=end_date, psr_type=psr_type
        )
    elif category_key == "installed_capacity_per_unit":
        result = client.query_installed_generation_capacity_per_unit(
            country_code, start=start_date, end=end_date, psr_type=psr_type
        )
    elif category_key == "actual_total_load":
        result = client.query_load(country_code, start=start_date, end=end_date)
    else:
        raise ValueError(f"Unsupported category: {category_key}")

    normalized = normalize_result(result, category_key)
    return filter_dataframe_to_utc_window(normalized, utc_start, utc_end_exclusive)


@st.cache_data(show_spinner=False)
def build_zip_from_results(results: Dict[str, pd.DataFrame]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        combined_frames = []
        for key, df in results.items():
            if df.empty:
                continue
            filename = f"{sanitize_filename_part(key)}.csv"
            zf.writestr(filename, df.to_csv(index=False).encode("utf-8"))
            combined_frames.append(df)

        if combined_frames:
            combined = pd.concat(combined_frames, ignore_index=True)
            zf.writestr(
                "all_entsoe_datasets_combined.csv",
                combined.to_csv(index=False).encode("utf-8"),
            )

    buffer.seek(0)
    return buffer.read()


def render_dataset_tab(
    dataset_label: str,
    df: pd.DataFrame,
    country_label: str,
    start: date,
    end: date,
    zip_bytes: bytes,
    excel_bytes: Optional[bytes],
    fast_mode_enabled: bool,
    fast_mode_row_threshold: int,
) -> None:
    csv_data = df.to_csv(index=False).encode("utf-8")
    parquet_data = build_parquet_bytes(df)
    csv_name = (
        f"{sanitize_filename_part(dataset_label)}_{sanitize_filename_part(country_label)}_{start}_{end}.csv"
    )
    parquet_name = (
        f"{sanitize_filename_part(dataset_label)}_{sanitize_filename_part(country_label)}_{start}_{end}.parquet"
    )
    zip_name = f"entsoe_data_{sanitize_filename_part(country_label)}_{start}_{end}.zip"
    excel_name = f"entsoe_data_{sanitize_filename_part(country_label)}_{start}_{end}.xlsx"

    csv_col, zip_col, excel_col, parquet_col = st.columns(4)
    with csv_col:
        st.download_button(
            label="Download all datasets as CSV",
            data=csv_data,
            file_name=csv_name,
            mime="text/csv",
            key=f"download_csv_{dataset_label}",
        )
    with zip_col:
        st.download_button(
            label="Download all datasets as ZIP",
            data=zip_bytes,
            file_name=zip_name,
            mime="application/zip",
            key=f"download_zip_{dataset_label}",
        )
    with excel_col:
        if excel_bytes is not None:
            st.download_button(
                label="Download all datasets as Excel",
                data=excel_bytes,
                file_name=excel_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_excel_{dataset_label}",
            )
        else:
            st.button(
                "Excel export unavailable",
                disabled=True,
                key=f"download_excel_disabled_{dataset_label}",
                help="Excel export failed for the current result set, but the other downloads are still available.",
            )
    with parquet_col:
        st.download_button(
            label="Download dataset as Parquet",
            data=parquet_data,
            file_name=parquet_name,
            mime="application/octet-stream",
            key=f"download_parquet_{dataset_label}",
        )

    st.metric("Rows returned", len(df))
    if fast_mode_enabled and len(df) > fast_mode_row_threshold:
        st.info(
            f"Fast mode is active for large datasets. Showing the first {FAST_MODE_PREVIEW_ROWS} rows only; downloads contain the full dataset."
        )
        st.dataframe(df.head(FAST_MODE_PREVIEW_ROWS), use_container_width=True, height=450)
    else:
        st.dataframe(df, use_container_width=True, height=450)


settings = load_app_settings(
    available_countries=list(COUNTRY_OPTIONS.keys()),
    available_psr_labels=list(PSR_TYPE_OPTIONS.keys()),
    available_categories=list(CATEGORY_LABELS.keys()),
    available_granularities=list(GRANULARITY_OPTIONS.keys()),
)

theme_left, theme_right = st.columns([1.3, 6])
with theme_left:
    theme_mode = st.radio(
        "Theme",
        options=THEME_OPTIONS,
        index=get_selectbox_index(THEME_OPTIONS, settings["default_theme"]),
        key="theme_mode_selector",
        horizontal=True,
    )

palette = apply_theme(theme_mode)

st.markdown(
    """
    <div class="entsoe-hero">
        <h1>ENTSO-E Generation and Load Data Bot <span class="hero-credit">(Vibecoded by Ashish)</span></h1>
        <p>Fetch ENTSO-E generation, load, and energy price datasets, preview them in the browser, and export everything in exact UTC date windows as CSV, Parquet, ZIP, or Excel.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

api_key = get_api_key_from_config()
if not api_key:
    st.error(
        "Missing API token. Add ENTSOE_API_TOKEN to .streamlit/secrets.toml or set it as an environment variable before running the app."
    )
    st.code('ENTSOE_API_TOKEN = "your_token_here"', language="toml")
    st.stop()

left, right = st.columns([1, 1])
with left:
    country_names = list(COUNTRY_OPTIONS.keys())
    selected_country_names = st.multiselect(
        "Country / bidding zone",
        options=country_names,
        default=get_default_multiselect_values(country_names, settings["default_country"]),
        help="Select one or more countries or bidding-zone groups to fetch together.",
    )
with right:
    psr_labels = list(PSR_TYPE_OPTIONS.keys())
    psr_label = st.selectbox(
        "Production type filter",
        psr_labels,
        index=get_selectbox_index(psr_labels, settings["default_psr_label"]),
    )
selected_country_domains = {
    country_name: COUNTRY_DOMAIN_CONFIG[country_name]
    for country_name in selected_country_names
    if country_name in COUNTRY_DOMAIN_CONFIG
}
zone_codes = unique_in_order(
    [
        zone_code
        for country_domains in selected_country_domains.values()
        for zone_code in country_domains["zones"]
    ]
)
zone_labels = [zone_label(zone_code) for zone_code in zone_codes]

zone_left, zone_right = st.columns([1, 1])
with zone_left:
    fetch_all_zones = st.checkbox(
        "All zones",
        value=True,
        help="Fetch all bidding zones for the selected countries when available.",
    )
with zone_right:
    include_country_total = st.checkbox(
        "Include country total",
        value=True,
        help="Also fetch the aggregate country or bidding-zone total series.",
    )

selected_zone_labels = st.multiselect(
    "Zones",
    options=zone_labels,
    default=zone_labels if fetch_all_zones else [],
    disabled=fetch_all_zones,
    help="Select one or more zones when All zones is disabled.",
)

col1, col2 = st.columns(2)
with col1:
    date_range = st.date_input(
        "Date range",
        value=(
            date.today() - timedelta(days=settings["default_lookback_days"]),
            date.today(),
        ),
    )
with col2:
    granularity_mode = st.selectbox(
        "Output granularity",
        options=list(GRANULARITY_OPTIONS.keys()),
        index=get_selectbox_index(
            list(GRANULARITY_OPTIONS.keys()), settings["default_granularity"]
        ),
        help="Time-series datasets can be exported as 15-minute data, 60-minute data, or both. Non-time-series tables stay unchanged.",
    )

with st.container(border=True):
    perf_left, perf_right = st.columns([1.35, 1])
    with perf_left:
        fast_mode_enabled = st.checkbox(
            "Fast mode for large results",
            value=True,
            help="For very large datasets, show only a lightweight preview in the app while keeping full downloads available.",
        )
    with perf_right:
        fast_mode_row_threshold = st.number_input(
            "Fast mode row threshold",
            min_value=1000,
            max_value=1_000_000,
            value=FAST_MODE_ROW_THRESHOLD,
            step=1000,
            help="Recommended: 50,000. If a dataset has more rows than this, the app shows only a preview table when Fast mode is enabled. Lower it if large tables feel slow; raise it if you want to see more full tables in the app.",
        )

category_label_to_key = {label: key for key, label in CATEGORY_LABELS.items()}
default_category_labels = [
    CATEGORY_LABELS[key] for key in settings["default_categories"] if key in CATEGORY_LABELS
]
selected_categories = st.multiselect(
    "ENTSO-E datasets to fetch",
    options=list(CATEGORY_LABELS.values()),
    default=default_category_labels,
)
if len(selected_categories) == 1:
    st.caption(f"Selected dataset: {selected_categories[0]}")
elif len(selected_categories) > 1:
    st.caption(f"Selected datasets: {len(selected_categories)}")
selected_category_keys = [
    category_label_to_key[label] for label in selected_categories if label in category_label_to_key
]

st.info(
    "Tip: 16.1.A (actual generation per generation unit) can return very large datasets, "
    "so the app now fetches it in smaller chunks for longer date ranges. "
    "Load forecast, forecast margin, and day-ahead prices are chunked across longer horizons too. "
    "Fast mode keeps the app responsive by previewing only the first rows of very large results, "
    "and selected dates are applied as exact UTC day boundaries."
)

fetch = st.button("Fetch ENTSO-E data", type="primary")

if fetch:
    if not selected_country_names:
        st.error("Select at least one country or bidding-zone group before fetching.")
        st.stop()

    if not isinstance(date_range, tuple) or len(date_range) != 2:
        st.error("Select both a start and end date in the date range picker.")
        st.stop()

    start, end = date_range

    if start > end:
        st.error("Start date must be on or before end date.")
        st.stop()

    if not selected_category_keys:
        st.error("Select at least one dataset before fetching.")
        st.stop()

    utc_start = make_utc_day_start(start)
    utc_end_exclusive = make_utc_day_start(end + timedelta(days=1))
    start_ts = make_entsoe_query_timestamp(utc_start)
    end_ts = make_entsoe_query_timestamp(utc_end_exclusive)
    psr_type = PSR_TYPE_OPTIONS[psr_label]
    selected_frequencies = GRANULARITY_OPTIONS[granularity_mode]
    label_to_code = {zone_label(zone_code): zone_code for zone_code in zone_codes}
    selected_countries_label = ", ".join(selected_country_names)

    if fetch_all_zones:
        selected_domain_codes = zone_codes.copy()
    else:
        selected_domain_codes = [
            label_to_code[label] for label in selected_zone_labels if label in label_to_code
        ]

    if include_country_total:
        total_codes = unique_in_order(
            [
                total_code
                for country_domains in selected_country_domains.values()
                for total_code in country_domains["total"]
            ]
        )
        for total_code in total_codes:
            if total_code not in selected_domain_codes:
                selected_domain_codes.append(total_code)

    if not selected_domain_codes:
        st.error("Select at least one zone or include the country total before fetching.")
        st.stop()

    results: Dict[str, pd.DataFrame] = {}
    errors: List[str] = []

    progress = st.progress(0, text="Starting requests...")
    total = len(selected_category_keys) * len(selected_domain_codes)
    completed = 0

    for domain_code in selected_domain_codes:
        domain_name = zone_label(domain_code)
        for category_key in selected_category_keys:
            progress.progress(
                completed / total,
                text=f"Fetching {domain_name} - {CATEGORY_LABELS[category_key]}...",
            )
            try:
                fetched_df = fetch_dataset(
                    api_key=api_key,
                    country_code=domain_code,
                    start_date=start_ts,
                    end_date=end_ts,
                    utc_start=utc_start,
                    utc_end_exclusive=utc_end_exclusive,
                    category_key=category_key,
                    psr_type=psr_type,
                )
                if not fetched_df.empty:
                    fetched_df.insert(0, "domain_name", domain_name)
                    fetched_df.insert(1, "domain_code", domain_code)
                results[f"{domain_name} - {CATEGORY_LABELS[category_key]}"] = fetched_df
            except Exception as exc:
                errors.append(f"{domain_name} - {CATEGORY_LABELS[category_key]}: {exc}")
                results[f"{domain_name} - {CATEGORY_LABELS[category_key]}"] = pd.DataFrame()
            completed += 1

    progress.progress(1.0, text="Done")

    if errors:
        st.warning("Some datasets could not be retrieved:")
        for err in errors:
            st.write(f"- {err}")

    non_empty = {k: v for k, v in results.items() if not v.empty}
    if not non_empty:
        st.error("No datasets were returned for the selected inputs.")
        st.stop()

    output_results = build_granularity_outputs(non_empty, selected_frequencies)
    if not output_results:
        st.error("No output datasets were available after granularity processing.")
        st.stop()

    total_rows = sum(len(df) for df in output_results.values())
    summary_left, summary_right = st.columns(2)
    with summary_left:
        st.metric("Datasets returned", len(output_results))
    with summary_right:
        st.metric("Total rows across datasets", total_rows)

    zip_bytes = build_zip_from_results(output_results)
    excel_bytes: Optional[bytes] = None
    try:
        excel_bytes = build_excel_from_results(
            output_results,
            metadata={
                "Country": selected_countries_label,
                "Domains": ", ".join(zone_label(domain_code) for domain_code in selected_domain_codes),
                "Categories": ", ".join(CATEGORY_LABELS[category] for category in selected_category_keys),
                "Start UTC": str(utc_start),
                "End UTC (exclusive)": str(utc_end_exclusive),
                "Granularity": granularity_mode,
            },
        )
    except Exception as exc:
        st.warning(
            "Excel export could not be prepared for this result set. CSV, ZIP, and Parquet downloads are still available."
        )
        st.caption(f"Excel export error: {exc}")

    tabs = st.tabs(list(output_results.keys()))
    for tab, key in zip(tabs, output_results.keys()):
        with tab:
            render_dataset_tab(
                key,
                output_results[key],
                selected_countries_label,
                start,
                end,
                zip_bytes,
                excel_bytes,
                fast_mode_enabled,
                int(fast_mode_row_threshold),
            )
