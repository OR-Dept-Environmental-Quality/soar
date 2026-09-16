import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aqs.transformers.ventilation_index import calculate_ventilation_index

result = calculate_ventilation_index(2024)
print(result.shape)
print(result.head())
print(result["ventilation_category"].value_counts())

print(result["ventilation_category"].isna().sum())
print(result[result['ventilation_category'].isna()][["mixing_height_m", "wind_speed_ms","ventilation_index_m2_s"]].describe())