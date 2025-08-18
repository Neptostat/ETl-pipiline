import os
import pandas as pd
import pytest
from my_project.core.extract import exoplanets_pscomppars


def _sample_path():
    """Helper to locate sample CSV used for testing."""
    return os.path.join(os.path.dirname(__file__), "data", "input_sample.csv")


def test_exoplanets_pscomppars_reads_csv():
    # Arrange
    path = _sample_path()

    # Act
    df = exoplanets_pscomppars(path)

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # Adjust these expectations based on your sample CSV
    assert len(df) == 3
    assert list(df.columns) == ["col1", "col2", "col3"]


def test_exoplanets_pscomppars_invalid_path_raises():
    # Act + Assert
    with pytest.raises(FileNotFoundError):
        exoplanets_pscomppars("non_existing_file.csv")

