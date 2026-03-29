from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import toml


SETTINGS_FILE = Path("user_settings.toml")


def load_app_settings(
    available_countries: List[str],
    available_psr_labels: List[str],
    available_categories: List[str],
    available_granularities: List[str],
) -> Dict[str, object]:
    defaults: Dict[str, object] = {
        "default_country": available_countries[0],
        "default_psr_label": available_psr_labels[0],
        "default_categories": available_categories[:1],
        "default_granularity": "60M" if "60M" in available_granularities else available_granularities[0],
        "default_lookback_days": 7,
        "default_theme": "Light",
    }

    if not SETTINGS_FILE.exists():
        return defaults

    try:
        raw_settings = toml.load(SETTINGS_FILE)
    except Exception:
        return defaults

    settings = defaults.copy()

    country = raw_settings.get("default_country")
    if country in available_countries:
        settings["default_country"] = country

    psr_label = raw_settings.get("default_psr_label")
    if psr_label in available_psr_labels:
        settings["default_psr_label"] = psr_label

    granularity = raw_settings.get("default_granularity")
    if granularity in available_granularities:
        settings["default_granularity"] = granularity

    lookback_days = raw_settings.get("default_lookback_days")
    if isinstance(lookback_days, int) and lookback_days >= 0:
        settings["default_lookback_days"] = lookback_days

    theme = raw_settings.get("default_theme")
    if theme in {"Light", "Dark"}:
        settings["default_theme"] = theme

    categories = raw_settings.get("default_categories")
    if isinstance(categories, list):
        filtered_categories = [
            category for category in categories if category in available_categories
        ]
        if filtered_categories:
            settings["default_categories"] = filtered_categories[:1]

    return settings
