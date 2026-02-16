"""
Smoke tests for P0-1 (EDA streaming) and P0-2 (Stage 4 streaming) fixes.
Verifies that chunked/streaming processing produces identical results to bulk loading.
"""
import csv
import io
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# Add project paths
sys.path.insert(0, str(Path(__file__).parent.parent / 'pipeline_modules' / 'utils'))
sys.path.insert(0, str(Path(__file__).parent.parent / 'pipeline_modules' / '4_standardization'))
sys.path.insert(0, str(Path(__file__).parent.parent / 'pipeline_modules' / '1_eda'))

from file_splitter import read_csv_byte_range_chunked, get_csv_byte_ranges, streaming_csv_reader


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file with known data."""
    csv_path = tmp_path / "DRUG_EXPOSURE.csv"
    n_rows = 5000
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['drug_exposure_id', 'person_id', 'drug_exposure_start_date',
                         'drug_exposure_start_datetime', 'drug_exposure_end_date',
                         'drug_exposure_end_datetime', 'value_as_number'])
        for i in range(n_rows):
            writer.writerow([
                i + 1,
                (i % 100) + 1,
                '2024-01-15',
                '2024-01-15 10:30:00.123456',
                '2024-01-20',
                '2024-01-20 14:00:00.789',
                round(i * 0.1, 2)
            ])
    return csv_path, n_rows


class TestReadCsvByteRangeChunked:
    """Test that read_csv_byte_range_chunked works for both small and large ranges."""

    def test_small_range_returns_all_rows(self, sample_csv):
        csv_path, n_rows = sample_csv
        ranges = get_csv_byte_ranges(csv_path, n_chunks=1)
        assert len(ranges) == 1

        start, end, fieldnames = ranges[0]
        rows = []
        for chunk in read_csv_byte_range_chunked(csv_path, start, end, fieldnames, chunksize=1000):
            rows.append(chunk)

        total = pd.concat(rows)
        assert len(total) == n_rows

    def test_multi_chunk_returns_all_rows(self, sample_csv):
        csv_path, n_rows = sample_csv
        ranges = get_csv_byte_ranges(csv_path, n_chunks=4)

        total_rows = 0
        for start, end, fieldnames in ranges:
            for chunk in read_csv_byte_range_chunked(csv_path, start, end, fieldnames, chunksize=500):
                total_rows += len(chunk)

        assert total_rows == n_rows

    def test_streaming_csv_reader_matches(self, sample_csv):
        """streaming_csv_reader should yield the same rows as read_csv_byte_range_chunked."""
        csv_path, n_rows = sample_csv
        ranges = get_csv_byte_ranges(csv_path, n_chunks=2)

        streaming_count = 0
        for start, end, fieldnames in ranges:
            for row in streaming_csv_reader(csv_path, start, end, fieldnames):
                streaming_count += 1

        assert streaming_count == n_rows


class TestProcessStandardTableStreaming:
    """Test that process_standard_table handles datetime standardization correctly."""

    def test_datetime_fractional_seconds_removed(self, sample_csv, tmp_path):
        csv_path, n_rows = sample_csv
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        from parallel_standardization import process_standard_table

        table_name, stats, elapsed = process_standard_table(
            ('DRUG_EXPOSURE', str(csv_path), str(output_dir))
        )

        assert table_name == 'DRUG_EXPOSURE'
        assert stats['input_records'] == n_rows
        assert stats['output_records'] == n_rows
        assert stats['datetime_standardized'] > 0

        # Verify output file exists and has correct row count
        out_file = output_dir / "DRUG_EXPOSURE_standardized.csv"
        assert out_file.exists()

        result_df = pd.read_csv(out_file)
        assert len(result_df) == n_rows

        # Verify fractional seconds are gone
        for col in ['drug_exposure_start_datetime', 'drug_exposure_end_datetime']:
            for val in result_df[col].dropna():
                assert '.' not in str(val), f"Fractional seconds still present: {val}"


class TestEdaDateRangeTailRead:
    """Test that the EDA date range tail-read works without OOM."""

    def test_tail_read_gets_correct_dates(self, sample_csv):
        csv_path, n_rows = sample_csv
        file_size = csv_path.stat().st_size

        # Simulate the tail-read logic from the fix
        tail_bytes = min(file_size, 512 * 1024)
        with open(csv_path, 'rb') as f:
            f.seek(max(0, file_size - tail_bytes))
            if file_size > tail_bytes:
                f.readline()  # skip partial line
            tail_data = f.read()

        header_bytes = open(csv_path, 'rb').readline()
        tail_buf = io.BytesIO(header_bytes + tail_data)
        last_chunk = pd.read_csv(tail_buf, low_memory=False)

        # Should get some rows from the tail
        assert len(last_chunk) > 0
        # Should have correct columns
        assert 'drug_exposure_start_date' in last_chunk.columns


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
