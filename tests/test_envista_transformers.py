"""Test suite for Envista transformers, specifically date format standardization and AQI calculation.

This module tests:
1. Date format standardization: Converts ISO timestamps to YYYY-MM-DD format
2. AQI calculation with date-based formula selection: Applies old or new EPA formula based on cutoff date (2024-05-06)
"""

from pathlib import Path
from datetime import datetime

import pandas as pd
import pytest

from envista.transformers.transform_env import transform_env_daily
from envista.transformers.calculate_aqi import (
    pm25_to_aqi_old,
    pm25_to_aqi_new,
    pm25_to_aqi_with_date_check,
    calculate_aqi,
)


class TestDateFormatStandardization:
    """Test suite for date format standardization in transform_env_daily."""

    def _create_sample_envista_data(self, dates: list[str]) -> pd.DataFrame:
        """Create sample Envista data with specified ISO timestamp dates."""
        return pd.DataFrame(
            {
                "data_datetime": dates,
                "data_channels_value": [10.5, 15.2, 8.3, -9999, 12.1],
                "data_channels_name": ["PM2.5", "PM2.5", "PM2.5", "PM2.5", "PM2.5"],
                "data_channels_valid": [True, True, False, True, True],
                "stationId": ["A", "B", "C", "D", "E"],
            }
        )

    def _create_sample_monitors(self) -> pd.DataFrame:
        """Create sample monitor data for merging."""
        return pd.DataFrame(
            {
                "station_id": ["A", "B", "C", "D", "E"],
                "stations_tag": ["SITE001", "SITE002", "SITE003", "SITE004", "SITE005"],
            }
        )

    def test_iso_timestamp_conversion_to_yyyy_mm_dd(self, tmp_path):
        """Test that ISO 8601 timestamps are converted to YYYY-MM-DD format."""
        # Create sample data with various ISO timestamp formats
        iso_dates = [
            "2024-05-01T08:00:00",
            "2024-05-02T14:30:00",
            "2024-05-03T10:15:30",
            "2024-05-04T00:00:00",
            "2024-05-05T23:59:59",
        ]
        
        df_envista = self._create_sample_envista_data(iso_dates)
        df_monitors = self._create_sample_monitors()
        
        # Write to temp file
        input_file = tmp_path / "test_data.csv"
        df_envista.to_csv(input_file, index=False)
        
        # Transform the data
        result = transform_env_daily("2024", [input_file], df_monitors)
        
        # Verify date_local column has YYYY-MM-DD format
        assert "date_local" in result.columns
        assert all(isinstance(date, str) for date in result["date_local"].dropna())
        
        # Check format: YYYY-MM-DD pattern
        for date in result["date_local"].dropna():
            # Should be exactly 10 characters: YYYY-MM-DD
            assert len(date) == 10, f"Date {date} is not in YYYY-MM-DD format"
            # Should be able to parse back to datetime
            assert pd.to_datetime(date) is not None
            # Check the pattern matches YYYY-MM-DD
            parts = date.split("-")
            assert len(parts) == 3, f"Date {date} doesn't have 3 parts separated by '-'"
            assert len(parts[0]) == 4, f"Year in {date} is not 4 digits"
            assert len(parts[1]) == 2, f"Month in {date} is not 2 digits"
            assert len(parts[2]) == 2, f"Day in {date} is not 2 digits"

    def test_iso_timestamp_with_timezone_conversion(self, tmp_path):
        """Test that ISO timestamps in UTC are correctly converted."""
        # Create sample data with UTC ISO timestamps (realistic format from Envista API)
        iso_dates_utc = [
            "2024-05-01T08:00:00Z",
            "2024-05-02T14:30:00Z",
            "2024-05-03T10:15:30Z",
            "2024-05-04T00:00:00Z",
            "2024-05-05T23:59:59Z",
        ]
        
        df_envista = pd.DataFrame(
            {
                "data_datetime": iso_dates_utc,
                "data_channels_value": [10.5, 15.2, 8.3, 12.1, 14.5],
                "data_channels_name": ["PM2.5"] * 5,
                "data_channels_valid": [True] * 5,
                "stationId": ["A", "B", "C", "D", "E"],
            }
        )
        df_monitors = self._create_sample_monitors()
        
        input_file = tmp_path / "test_data_tz.csv"
        df_envista.to_csv(input_file, index=False)
        
        result = transform_env_daily("2024", [input_file], df_monitors)
        
        # All dates should be in YYYY-MM-DD format (timezone stripped)
        for date in result["date_local"].dropna():
            assert len(date) == 10
            assert date.count("-") == 2
            # Dates should be parseable as datetime
            assert pd.to_datetime(date) is not None

    def test_multiple_iso_timestamps_consistency(self, tmp_path):
        """Test that all ISO timestamps in a batch are consistently converted."""
        iso_dates = [
            "2024-03-15T09:30:00",
            "2024-06-20T15:45:30",
            "2024-12-31T23:59:59",
            "2024-01-01T00:00:00",
            "2024-07-04T12:00:00",
        ]
        
        df_envista = pd.DataFrame(
            {
                "data_datetime": iso_dates,
                "data_channels_value": [10.5, 15.2, 8.3, 12.1, 14.5],
                "data_channels_name": ["PM2.5"] * 5,
                "data_channels_valid": [True] * 5,
                "stationId": ["A", "B", "C", "D", "E"],
            }
        )
        df_monitors = self._create_sample_monitors()
        
        input_file = tmp_path / "test_data_multi.csv"
        df_envista.to_csv(input_file, index=False)
        
        result = transform_env_daily("2024", [input_file], df_monitors)
        
        # Verify each converted date matches expected value
        expected_dates = [
            "2024-03-15",
            "2024-06-20",
            "2024-12-31",
            "2024-01-01",
            "2024-07-04",
        ]
        
        result_dates = sorted(result["date_local"].dropna().unique())
        expected_dates_sorted = sorted(expected_dates)
        
        assert result_dates == expected_dates_sorted


class TestAQICalculationWithDateBasedFormula:
    """Test suite for AQI calculation with date-based formula selection."""

    CUTOFF_DATE = "2024-05-06"
    OLD_FORMULA_CUTOFF = "2024-05-05"
    NEW_FORMULA_CUTOFF = "2024-05-06"

    def test_pm25_aqi_old_formula_low_concentration(self):
        """Test old AQI formula with low PM2.5 concentration."""
        # Old formula: AQI = (50/12.0) * concentration for concentration <= 12.0
        concentration = 6.0
        expected_aqi = round((50 / 12.0) * 6.0)  # Should be 25
        result = pm25_to_aqi_old(concentration)
        assert result == expected_aqi

    def test_pm25_aqi_old_formula_high_concentration(self):
        """Test old AQI formula with high PM2.5 concentration."""
        # Old formula: For concentration > 500.4, return 500
        concentration = 600.0
        result = pm25_to_aqi_old(concentration)
        assert result == 500

    def test_pm25_aqi_new_formula_low_concentration(self):
        """Test new AQI formula with low PM2.5 concentration."""
        # New formula: AQI = (50/9.0) * concentration for concentration <= 9.0
        concentration = 4.5
        expected_aqi = round((50 / 9.0) * 4.5)  # Should be 25
        result = pm25_to_aqi_new(concentration)
        assert result == expected_aqi

    def test_pm25_aqi_new_formula_high_concentration(self):
        """Test new AQI formula with high PM2.5 concentration."""
        # New formula: For concentration > 500.4, return 500
        concentration = 600.0
        result = pm25_to_aqi_new(concentration)
        assert result == 500

    def test_pm25_aqi_old_formula_different_breakpoints(self):
        """Test old AQI formula applies correctly at different breakpoints."""
        test_cases = [
            # (concentration, expected_aqi)
            (12.0, 50),  # Upper bound of first bracket
            (35.4, 100),  # Upper bound of second bracket
            (55.4, 150),  # Upper bound of third bracket
            (150.4, 200),  # Upper bound of fourth bracket
        ]
        
        for concentration, expected_aqi in test_cases:
            result = pm25_to_aqi_old(concentration)
            assert result == expected_aqi, f"Old formula failed for {concentration}"

    def test_pm25_aqi_new_formula_different_breakpoints(self):
        """Test new AQI formula applies correctly at different breakpoints (updated from old)."""
        test_cases = [
            # (concentration, expected_aqi)
            (9.0, 50),  # Upper bound of first bracket (changed from 12.0)
            (35.4, 100),  # Upper bound of second bracket
            (55.4, 150),  # Upper bound of third bracket
            (125.4, 200),  # Upper bound of fourth bracket (changed from 150.4)
        ]
        
        for concentration, expected_aqi in test_cases:
            result = pm25_to_aqi_new(concentration)
            assert result == expected_aqi, f"New formula failed for {concentration}"

    def test_date_check_applies_old_formula_before_cutoff(self):
        """Test that dates before 2024-05-06 use old AQI formula."""
        row = pd.Series({
            "arithmetic_mean": 6.0,  # Concentration
            "date_local": "2024-05-05",  # Day before cutoff
        })
        
        result = pm25_to_aqi_with_date_check(row)
        expected = pm25_to_aqi_old(6.0)  # Should use old formula
        assert result == expected

    def test_date_check_applies_new_formula_on_cutoff_date(self):
        """Test that dates on or after 2024-05-06 use new AQI formula."""
        row = pd.Series({
            "arithmetic_mean": 6.0,  # Concentration
            "date_local": "2024-05-06",  # Cutoff date
        })
        
        result = pm25_to_aqi_with_date_check(row)
        expected = pm25_to_aqi_new(6.0)  # Should use new formula
        assert result == expected

    def test_date_check_applies_new_formula_after_cutoff(self):
        """Test that dates after 2024-05-06 use new AQI formula."""
        row = pd.Series({
            "arithmetic_mean": 6.0,  # Concentration
            "date_local": "2024-05-07",  # Day after cutoff
        })
        
        result = pm25_to_aqi_with_date_check(row)
        expected = pm25_to_aqi_new(6.0)  # Should use new formula
        assert result == expected

    def test_date_check_with_iso_timestamp_before_cutoff(self):
        """Test date check with ISO 8601 timestamp format before cutoff."""
        row = pd.Series({
            "arithmetic_mean": 20.0,
            "date_local": "2024-05-04T15:30:00",  # ISO format before cutoff
        })
        
        result = pm25_to_aqi_with_date_check(row)
        expected = pm25_to_aqi_old(20.0)  # Should use old formula
        assert result == expected

    def test_date_check_with_iso_timestamp_after_cutoff(self):
        """Test date check with ISO 8601 timestamp format after cutoff."""
        row = pd.Series({
            "arithmetic_mean": 20.0,
            "date_local": "2024-05-10T08:00:00",  # ISO format after cutoff
        })
        
        result = pm25_to_aqi_with_date_check(row)
        expected = pm25_to_aqi_new(20.0)  # Should use new formula
        assert result == expected

    def test_calculate_aqi_applies_correct_formula_to_dataframe(self):
        """Test that calculate_aqi applies the date-based formula to entire DataFrame."""
        df = pd.DataFrame({
            "arithmetic_mean": [6.0, 20.0, 50.0, 15.0],
            "date_local": [
                "2024-05-05",  # Before cutoff - uses old formula
                "2024-05-05",  # Before cutoff - uses old formula
                "2024-05-06",  # On cutoff - uses new formula
                "2024-05-10",  # After cutoff - uses new formula
            ],
        })
        
        result = calculate_aqi(df)
        
        # Verify all rows have AQI values
        assert "aqi" in result.columns
        assert not result["aqi"].isna().any()
        
        # Verify correct formulas were applied
        # Row 0 & 1 (before cutoff) should use old formula
        expected_aqi_0 = pm25_to_aqi_old(6.0)
        expected_aqi_1 = pm25_to_aqi_old(20.0)
        assert result.loc[0, "aqi"] == expected_aqi_0
        assert result.loc[1, "aqi"] == expected_aqi_1
        
        # Row 2 & 3 (on/after cutoff) should use new formula
        expected_aqi_2 = pm25_to_aqi_new(50.0)
        expected_aqi_3 = pm25_to_aqi_new(15.0)
        assert result.loc[2, "aqi"] == expected_aqi_2
        assert result.loc[3, "aqi"] == expected_aqi_3

    def test_aqi_calculation_handles_na_values(self):
        """Test that AQI calculation correctly handles NA values."""
        df = pd.DataFrame({
            "arithmetic_mean": [pd.NA, 20.0],
            "date_local": ["2024-05-05", "2024-05-06"],
        })
        
        result = calculate_aqi(df)
        
        # First row should be NA
        assert pd.isna(result.loc[0, "aqi"])
        # Second row should have valid AQI
        assert not pd.isna(result.loc[1, "aqi"])

    def test_pm25_aqi_old_formula_boundary_values(self):
        """Test old formula at exact boundary values."""
        # Test at boundaries between breakpoints
        test_cases = [
            (0, 0),
            (12.1, 51),  # Transition to second bracket
            (35.5, 101),  # Transition to third bracket
            (55.5, 151),  # Transition to fourth bracket
            (150.5, 201),  # Transition to fifth bracket
        ]
        
        for concentration, min_expected_aqi in test_cases:
            result = pm25_to_aqi_old(concentration)
            # Result should be at or above the minimum for this bracket
            assert result >= min_expected_aqi

    def test_pm25_aqi_new_formula_boundary_values(self):
        """Test new formula at exact boundary values (updated breakpoints)."""
        # Test at boundaries between breakpoints (updated in new formula)
        test_cases = [
            (0, 0),
            (9.1, 51),  # Transition to second bracket (changed from 12.1)
            (35.5, 101),  # Transition to third bracket
            (55.5, 151),  # Transition to fourth bracket
            (125.5, 201),  # Transition to fifth bracket (changed from 150.5)
        ]
        
        for concentration, min_expected_aqi in test_cases:
            result = pm25_to_aqi_new(concentration)
            # Result should be at or above the minimum for this bracket
            assert result >= min_expected_aqi


class TestDateFormatAndAQIIntegration:
    """Integration tests combining date format standardization and AQI calculation."""

    def test_transform_env_daily_produces_standardized_dates_and_correct_aqi(self, tmp_path):
        """Test that transform_env_daily produces YYYY-MM-DD dates and calculates AQI correctly."""
        # Create data spanning the cutoff date
        iso_dates = [
            "2024-05-04T10:00:00",
            "2024-05-05T10:00:00",
            "2024-05-06T10:00:00",
            "2024-05-07T10:00:00",
            "2024-05-08T10:00:00",
        ]
        
        df_envista = pd.DataFrame({
            "data_datetime": iso_dates,
            "data_channels_value": [10.0, 15.0, 20.0, 8.0, 12.0],
            "data_channels_name": ["PM2.5"] * 5,
            "data_channels_valid": [True] * 5,
            "stationId": ["SITE001"] * 5,
        })
        
        df_monitors = pd.DataFrame({
            "station_id": ["SITE001"],
            "stations_tag": ["TEST_SITE"],
        })
        
        input_file = tmp_path / "integration_test.csv"
        df_envista.to_csv(input_file, index=False)
        
        result = transform_env_daily("2024", [input_file], df_monitors)
        
        # Verify dates are standardized
        assert all(len(date) == 10 for date in result["date_local"])
        
        # Verify AQI values are calculated
        assert "aqi" in result.columns
        assert not result["aqi"].isna().any()
        
        # Verify dates before cutoff have different AQI values than dates after
        # due to formula change (for same/similar concentrations)
        before_cutoff = result[result["date_local"] < "2024-05-06"]
        after_cutoff = result[result["date_local"] >= "2024-05-06"]
        
        assert not before_cutoff.empty
        assert not after_cutoff.empty
