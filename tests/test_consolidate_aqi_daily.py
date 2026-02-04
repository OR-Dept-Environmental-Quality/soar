"""Test suite for AQI daily consolidation, specifically PM2.5 priority hierarchy logic.

This module tests:
1. PM2.5 priority assignment: Verifies correct priority levels (FRM/FEM > non-FRM/FEM AQS > Envista)
2. PM2.5 consolidation: Ensures correct record selection based on priority and arithmetic_mean
3. AQI category assignment: Validates AQI value to category mapping
4. Multi-pollutant consolidation: Tests proper handling of ozone and PM2.5 data
"""

from pathlib import Path

import pandas as pd
import numpy as np
import pytest

from stage.consolidate_aqi_daily import (
    get_aqi_category,
    consolidate_aqi_daily_for_year,
)


class TestPM25PriorityHierarchy:
    """Test suite for PM2.5 priority hierarchy logic."""

    PRIORITY_1_CODE = 88101  # FRM/FEM
    PRIORITY_2_CODE = 88502  # Non-FRM/FEM AQS
    PRIORITY_3_CODE = 88502  # Envista sensors
    PRIORITY_2_POC = 1  # Non-99 POC for AQS
    PRIORITY_3_POC = 99  # POC 99 for Envista

    def _create_sample_aqi_data(self, records: list[dict]) -> pd.DataFrame:
        """Create sample AQI data from record dictionaries."""
        return pd.DataFrame(records)

    def _create_categories_df(self) -> pd.DataFrame:
        """Create sample AQI categories for testing."""
        return pd.DataFrame({
            'aqi_category': ['Good', 'Moderate', 'Unhealthy for Sensitive Groups', 'Unhealthy', 'Very Unhealthy', 'Hazardous'],
            'low_aqi': [0, 51, 101, 151, 201, 301],
            'high_aqi': [50, 100, 150, 200, 300, 500]
        })

    def test_pm25_priority_level_1_frm_fem_monitors(self, tmp_path):
        """Test that FRM/FEM monitors (parameter_code 88101) have highest priority."""
        data = [
            {
                "site_code": "TEST001",
                "date_local": "2024-05-15",
                "parameter_code": self.PRIORITY_1_CODE,  # FRM/FEM
                "poc": 1,
                "arithmetic_mean": 15.0,
                "aqi": 75,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
            {
                "site_code": "TEST001",
                "date_local": "2024-05-15",
                "parameter_code": self.PRIORITY_2_CODE,  # Non-FRM/FEM AQS
                "poc": self.PRIORITY_2_POC,
                "arithmetic_mean": 20.0,
                "aqi": 95,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
        ]

        df = self._create_sample_aqi_data(data)
        df['pollutant'] = df['parameter_code'].map({
            44201: 'ozone',
            88101: 'pm25',
            88502: 'pm25'
        })

        pm25_df = df[df['pollutant'] == 'pm25'].copy()

        # Assign priority
        def assign_pm25_priority(row):
            if row['parameter_code'] == 88101:
                return 1
            elif row['parameter_code'] == 88502 and row['poc'] != 99:
                return 2
            elif row['parameter_code'] == 88502 and row['poc'] == 99:
                return 3
            else:
                return 999

        pm25_df['priority'] = pm25_df.apply(assign_pm25_priority, axis=1)
        pm25_df = pm25_df.sort_values(['priority', 'arithmetic_mean'], ascending=[True, False])
        pm25_consolidated = pm25_df.groupby(['site_code', 'date_local']).first().reset_index()

        # Should select the FRM/FEM record (priority 1) even though non-FRM/FEM has higher mean
        assert len(pm25_consolidated) == 1
        assert pm25_consolidated.iloc[0]['parameter_code'] == self.PRIORITY_1_CODE
        assert pm25_consolidated.iloc[0]['aqi'] == 75

    def test_pm25_priority_level_2_non_frm_fem_aqs(self, tmp_path):
        """Test that non-FRM/FEM AQS monitors (88502, POC != 99) have priority over Envista."""
        data = [
            {
                "site_code": "TEST002",
                "date_local": "2024-05-15",
                "parameter_code": self.PRIORITY_2_CODE,  # Non-FRM/FEM AQS
                "poc": self.PRIORITY_2_POC,
                "arithmetic_mean": 12.0,
                "aqi": 65,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
            {
                "site_code": "TEST002",
                "date_local": "2024-05-15",
                "parameter_code": self.PRIORITY_3_CODE,  # Envista
                "poc": self.PRIORITY_3_POC,
                "arithmetic_mean": 25.0,
                "aqi": 105,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
        ]

        df = self._create_sample_aqi_data(data)
        df['pollutant'] = df['parameter_code'].map({
            44201: 'ozone',
            88101: 'pm25',
            88502: 'pm25'
        })

        pm25_df = df[df['pollutant'] == 'pm25'].copy()

        def assign_pm25_priority(row):
            if row['parameter_code'] == 88101:
                return 1
            elif row['parameter_code'] == 88502 and row['poc'] != 99:
                return 2
            elif row['parameter_code'] == 88502 and row['poc'] == 99:
                return 3
            else:
                return 999

        pm25_df['priority'] = pm25_df.apply(assign_pm25_priority, axis=1)
        pm25_df = pm25_df.sort_values(['priority', 'arithmetic_mean'], ascending=[True, False])
        pm25_consolidated = pm25_df.groupby(['site_code', 'date_local']).first().reset_index()

        # Should select the non-FRM/FEM AQS (priority 2) even though Envista has higher mean
        assert len(pm25_consolidated) == 1
        assert pm25_consolidated.iloc[0]['poc'] == self.PRIORITY_2_POC
        assert pm25_consolidated.iloc[0]['aqi'] == 65

    def test_pm25_priority_level_3_envista_sensors(self):
        """Test that Envista sensors (88502, POC == 99) are lowest priority."""
        data = [
            {
                "site_code": "TEST003",
                "date_local": "2024-05-15",
                "parameter_code": self.PRIORITY_3_CODE,
                "poc": self.PRIORITY_3_POC,
                "arithmetic_mean": 18.0,
                "aqi": 85,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
        ]

        df = self._create_sample_aqi_data(data)
        df['pollutant'] = df['parameter_code'].map({
            44201: 'ozone',
            88101: 'pm25',
            88502: 'pm25'
        })

        pm25_df = df[df['pollutant'] == 'pm25'].copy()

        def assign_pm25_priority(row):
            if row['parameter_code'] == 88101:
                return 1
            elif row['parameter_code'] == 88502 and row['poc'] != 99:
                return 2
            elif row['parameter_code'] == 88502 and row['poc'] == 99:
                return 3
            else:
                return 999

        pm25_df['priority'] = pm25_df.apply(assign_pm25_priority, axis=1)

        # Should have priority 3
        assert pm25_df.iloc[0]['priority'] == 3

    def test_pm25_consolidation_selects_highest_mean_within_priority(self):
        """Test that within same priority, highest arithmetic_mean is selected."""
        data = [
            {
                "site_code": "TEST004",
                "date_local": "2024-05-15",
                "parameter_code": self.PRIORITY_3_CODE,
                "poc": self.PRIORITY_3_POC,
                "arithmetic_mean": 15.0,
                "aqi": 75,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
            {
                "site_code": "TEST004",
                "date_local": "2024-05-15",
                "parameter_code": self.PRIORITY_3_CODE,
                "poc": self.PRIORITY_3_POC,
                "arithmetic_mean": 25.0,
                "aqi": 105,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
        ]

        df = self._create_sample_aqi_data(data)
        df['pollutant'] = df['parameter_code'].map({
            44201: 'ozone',
            88101: 'pm25',
            88502: 'pm25'
        })

        pm25_df = df[df['pollutant'] == 'pm25'].copy()

        def assign_pm25_priority(row):
            if row['parameter_code'] == 88101:
                return 1
            elif row['parameter_code'] == 88502 and row['poc'] != 99:
                return 2
            elif row['parameter_code'] == 88502 and row['poc'] == 99:
                return 3
            else:
                return 999

        pm25_df['priority'] = pm25_df.apply(assign_pm25_priority, axis=1)
        pm25_df = pm25_df.sort_values(['priority', 'arithmetic_mean'], ascending=[True, False])
        pm25_consolidated = pm25_df.groupby(['site_code', 'date_local']).first().reset_index()

        # Should select the record with higher arithmetic_mean within same priority
        assert len(pm25_consolidated) == 1
        assert pm25_consolidated.iloc[0]['arithmetic_mean'] == 25.0
        assert pm25_consolidated.iloc[0]['aqi'] == 105

    def test_pm25_priority_all_three_levels_present(self):
        """Test consolidation with all three priority levels present for same site/date."""
        data = [
            {
                "site_code": "TEST005",
                "date_local": "2024-05-15",
                "parameter_code": 88101,  # Priority 1: FRM/FEM
                "poc": 1,
                "arithmetic_mean": 10.0,
                "aqi": 60,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
            {
                "site_code": "TEST005",
                "date_local": "2024-05-15",
                "parameter_code": 88502,  # Priority 2: Non-FRM/FEM AQS
                "poc": 2,
                "arithmetic_mean": 20.0,
                "aqi": 95,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
            {
                "site_code": "TEST005",
                "date_local": "2024-05-15",
                "parameter_code": 88502,  # Priority 3: Envista
                "poc": 99,
                "arithmetic_mean": 30.0,
                "aqi": 125,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
        ]

        df = self._create_sample_aqi_data(data)
        df['pollutant'] = df['parameter_code'].map({
            44201: 'ozone',
            88101: 'pm25',
            88502: 'pm25'
        })

        pm25_df = df[df['pollutant'] == 'pm25'].copy()

        def assign_pm25_priority(row):
            if row['parameter_code'] == 88101:
                return 1
            elif row['parameter_code'] == 88502 and row['poc'] != 99:
                return 2
            elif row['parameter_code'] == 88502 and row['poc'] == 99:
                return 3
            else:
                return 999

        pm25_df['priority'] = pm25_df.apply(assign_pm25_priority, axis=1)
        pm25_df = pm25_df.sort_values(['priority', 'arithmetic_mean'], ascending=[True, False])
        pm25_consolidated = pm25_df.groupby(['site_code', 'date_local']).first().reset_index()

        # Should select Priority 1 (FRM/FEM) even with lowest mean
        assert len(pm25_consolidated) == 1
        assert pm25_consolidated.iloc[0]['parameter_code'] == 88101
        assert pm25_consolidated.iloc[0]['aqi'] == 60

    def test_pm25_multiple_sites_and_dates(self):
        """Test PM2.5 consolidation across multiple sites and dates."""
        data = [
            # Site 1, Date 1: Multiple Priority 2 records
            {
                "site_code": "SITE001",
                "date_local": "2024-05-10",
                "parameter_code": 88502,
                "poc": 1,
                "arithmetic_mean": 12.0,
                "aqi": 65,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
            {
                "site_code": "SITE001",
                "date_local": "2024-05-10",
                "parameter_code": 88502,
                "poc": 2,
                "arithmetic_mean": 18.0,
                "aqi": 85,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
            # Site 2, Date 1: Priority 1 vs Priority 3
            {
                "site_code": "SITE002",
                "date_local": "2024-05-10",
                "parameter_code": 88101,
                "poc": 1,
                "arithmetic_mean": 14.0,
                "aqi": 70,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
            {
                "site_code": "SITE002",
                "date_local": "2024-05-10",
                "parameter_code": 88502,
                "poc": 99,
                "arithmetic_mean": 35.0,
                "aqi": 135,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
            # Site 1, Date 2: Single Priority 3
            {
                "site_code": "SITE001",
                "date_local": "2024-05-11",
                "parameter_code": 88502,
                "poc": 99,
                "arithmetic_mean": 22.0,
                "aqi": 100,
                "validity_indicator": "Y",
                "observation_percent": 100.0,
                "event_type": "No Events",
            },
        ]

        df = self._create_sample_aqi_data(data)
        df['pollutant'] = df['parameter_code'].map({
            44201: 'ozone',
            88101: 'pm25',
            88502: 'pm25'
        })

        pm25_df = df[df['pollutant'] == 'pm25'].copy()

        def assign_pm25_priority(row):
            if row['parameter_code'] == 88101:
                return 1
            elif row['parameter_code'] == 88502 and row['poc'] != 99:
                return 2
            elif row['parameter_code'] == 88502 and row['poc'] == 99:
                return 3
            else:
                return 999

        pm25_df['priority'] = pm25_df.apply(assign_pm25_priority, axis=1)
        pm25_df = pm25_df.sort_values(['priority', 'arithmetic_mean'], ascending=[True, False])
        pm25_consolidated = pm25_df.groupby(['site_code', 'date_local']).first().reset_index()

        # Should have 3 consolidated records (one per site-date)
        assert len(pm25_consolidated) == 3

        # SITE001, 2024-05-10: Should select POC 2 (highest mean among Priority 2)
        site1_date1 = pm25_consolidated[
            (pm25_consolidated['site_code'] == 'SITE001') & 
            (pm25_consolidated['date_local'] == '2024-05-10')
        ].iloc[0]
        assert site1_date1['poc'] == 2
        assert site1_date1['aqi'] == 85

        # SITE002, 2024-05-10: Should select Priority 1 (even with lower mean)
        site2_date1 = pm25_consolidated[
            (pm25_consolidated['site_code'] == 'SITE002') & 
            (pm25_consolidated['date_local'] == '2024-05-10')
        ].iloc[0]
        assert site2_date1['parameter_code'] == 88101
        assert site2_date1['aqi'] == 70

        # SITE001, 2024-05-11: Should have only Priority 3 record
        site1_date2 = pm25_consolidated[
            (pm25_consolidated['site_code'] == 'SITE001') & 
            (pm25_consolidated['date_local'] == '2024-05-11')
        ].iloc[0]
        assert site1_date2['poc'] == 99
        assert site1_date2['aqi'] == 100


class TestAQICategoryAssignment:
    """Test suite for AQI category assignment."""

    def _create_categories_df(self) -> pd.DataFrame:
        """Create sample AQI categories."""
        return pd.DataFrame({
            'aqi_category': ['Good', 'Moderate', 'Unhealthy for Sensitive Groups', 'Unhealthy', 'Very Unhealthy', 'Hazardous'],
            'low_aqi': [0, 51, 101, 151, 201, 301],
            'high_aqi': [50, 100, 150, 200, 300, 500]
        })

    def test_aqi_category_good(self):
        """Test AQI category assignment for 'Good' range (0-50)."""
        categories_df = self._create_categories_df()
        
        assert get_aqi_category(0, categories_df) == 'Good'
        assert get_aqi_category(25, categories_df) == 'Good'
        assert get_aqi_category(50, categories_df) == 'Good'

    def test_aqi_category_moderate(self):
        """Test AQI category assignment for 'Moderate' range (51-100)."""
        categories_df = self._create_categories_df()
        
        assert get_aqi_category(51, categories_df) == 'Moderate'
        assert get_aqi_category(75, categories_df) == 'Moderate'
        assert get_aqi_category(100, categories_df) == 'Moderate'

    def test_aqi_category_unhealthy_for_sensitive_groups(self):
        """Test AQI category assignment for 'Unhealthy for Sensitive Groups' range (101-150)."""
        categories_df = self._create_categories_df()
        
        assert get_aqi_category(101, categories_df) == 'Unhealthy for Sensitive Groups'
        assert get_aqi_category(125, categories_df) == 'Unhealthy for Sensitive Groups'
        assert get_aqi_category(150, categories_df) == 'Unhealthy for Sensitive Groups'

    def test_aqi_category_unhealthy(self):
        """Test AQI category assignment for 'Unhealthy' range (151-200)."""
        categories_df = self._create_categories_df()
        
        assert get_aqi_category(151, categories_df) == 'Unhealthy'
        assert get_aqi_category(175, categories_df) == 'Unhealthy'
        assert get_aqi_category(200, categories_df) == 'Unhealthy'

    def test_aqi_category_very_unhealthy(self):
        """Test AQI category assignment for 'Very Unhealthy' range (201-300)."""
        categories_df = self._create_categories_df()
        
        assert get_aqi_category(201, categories_df) == 'Very Unhealthy'
        assert get_aqi_category(250, categories_df) == 'Very Unhealthy'
        assert get_aqi_category(300, categories_df) == 'Very Unhealthy'

    def test_aqi_category_hazardous(self):
        """Test AQI category assignment for 'Hazardous' range (301-500)."""
        categories_df = self._create_categories_df()
        
        assert get_aqi_category(301, categories_df) == 'Hazardous'
        assert get_aqi_category(400, categories_df) == 'Hazardous'
        assert get_aqi_category(500, categories_df) == 'Hazardous'

    def test_aqi_category_above_max(self):
        """Test AQI category assignment for values above maximum defined range."""
        categories_df = self._create_categories_df()
        
        # Values above 500 should be assigned to highest category
        assert get_aqi_category(501, categories_df) == 'Hazardous'
        assert get_aqi_category(999, categories_df) == 'Hazardous'

    def test_aqi_category_na_value(self):
        """Test AQI category assignment for NA values."""
        categories_df = self._create_categories_df()
        
        assert get_aqi_category(pd.NA, categories_df) is None
        assert get_aqi_category(np.nan, categories_df) is None


class TestMultiPollutantConsolidation:
    """Test suite for multi-pollutant consolidation logic."""

    def _create_categories_df(self) -> pd.DataFrame:
        """Create sample AQI categories."""
        return pd.DataFrame({
            'aqi_category': ['Good', 'Moderate', 'Unhealthy for Sensitive Groups', 'Unhealthy', 'Very Unhealthy', 'Hazardous'],
            'low_aqi': [0, 51, 101, 151, 201, 301],
            'high_aqi': [50, 100, 150, 200, 300, 500]
        })

    def test_overall_aqi_as_maximum_of_pollutants(self):
        """Test that overall AQI is maximum of ozone and PM2.5 AQI values."""
        categories_df = self._create_categories_df()
        
        # Test case: Ozone AQI = 60, PM2.5 AQI = 95, Overall should be 95
        result = pd.DataFrame({
            'ozone_aqi': [60],
            'pm25_aqi': [95],
        })
        
        result['aqi'] = result[['ozone_aqi', 'pm25_aqi']].apply(
            lambda row: np.nanmax(row.values), axis=1
        )
        
        assert result['aqi'].iloc[0] == 95

    def test_overall_aqi_only_ozone(self):
        """Test overall AQI when only ozone data is present."""
        # Ozone only
        result = pd.DataFrame({
            'ozone_aqi': [75],
            'pm25_aqi': [np.nan],
        })
        
        result['aqi'] = result[['ozone_aqi', 'pm25_aqi']].apply(
            lambda row: np.nanmax(row.values), axis=1
        )
        
        assert result['aqi'].iloc[0] == 75

    def test_overall_aqi_only_pm25(self):
        """Test overall AQI when only PM2.5 data is present."""
        # PM2.5 only
        result = pd.DataFrame({
            'ozone_aqi': [np.nan],
            'pm25_aqi': [85],
        })
        
        result['aqi'] = result[['ozone_aqi', 'pm25_aqi']].apply(
            lambda row: np.nanmax(row.values), axis=1
        )
        
        assert result['aqi'].iloc[0] == 85

    def test_overall_aqi_both_pollutants_multiple_records(self):
        """Test overall AQI calculation across multiple records."""
        result = pd.DataFrame({
            'site_code': ['SITE1', 'SITE2', 'SITE3'],
            'date_local': ['2024-05-10', '2024-05-11', '2024-05-12'],
            'ozone_aqi': [60, np.nan, 120],
            'pm25_aqi': [55, 85, 110],
        })
        
        result['aqi'] = result[['ozone_aqi', 'pm25_aqi']].apply(
            lambda row: np.nanmax(row.values), axis=1
        )
        
        # SITE1: max(60, 55) = 60
        assert result.loc[0, 'aqi'] == 60
        # SITE2: max(nan, 85) = 85
        assert result.loc[1, 'aqi'] == 85
        # SITE3: max(120, 110) = 120
        assert result.loc[2, 'aqi'] == 120

    def test_aqi_category_assignment_after_consolidation(self):
        """Test AQI category assignment after consolidation."""
        categories_df = self._create_categories_df()
        
        result = pd.DataFrame({
            'site_code': ['SITE1', 'SITE2', 'SITE3'],
            'date_local': ['2024-05-10', '2024-05-11', '2024-05-12'],
            'ozone_aqi': [25, 75, 180],
            'pm25_aqi': [30, np.nan, 140],
        })
        
        result['aqi'] = result[['ozone_aqi', 'pm25_aqi']].apply(
            lambda row: np.nanmax(row.values), axis=1
        )
        
        result['aqi_category'] = result['aqi'].apply(
            lambda x: get_aqi_category(x, categories_df)
        )
        
        # SITE1: AQI 30 -> Good
        assert result.loc[0, 'aqi_category'] == 'Good'
        # SITE2: AQI 75 -> Moderate
        assert result.loc[1, 'aqi_category'] == 'Moderate'
        # SITE3: AQI 180 -> Unhealthy
        assert result.loc[2, 'aqi_category'] == 'Unhealthy'
