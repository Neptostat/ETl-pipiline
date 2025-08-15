
import os
from my_project.core.extract import read_csv

def _sample_path():
    return os.path.join(os.path.dirname(__file__), "data", "input_sample.csv")

def test_read_csv_full():
    path = _sample_path()
    chunks = list(read_csv(path))
    assert len(chunks) == 1
    df = chunks[0]
    assert len(df) == 3

def test_read_csv_chunksize():
    path = _sample_path()
    total = 0
    for chunk in read_csv(path, chunksize=2):
        total += len(chunk)
    assert total == 3
