import glob
import os
import unittest
import pandas as pd
from polygon_file_reader import convert_timestamp, convert_minute_csv_to_parquet, process_all_minute_csv_to_parquet


class TestPolygonFileReader(unittest.TestCase):

    def test_convert_timestamp(self):
        # Test converting a timestamp string to a pandas Timestamp object
        self.assertEqual(convert_timestamp("0"),                pd.Timestamp("1970-01-01 00:00:00"))
        self.assertEqual(convert_timestamp("0000000000000000"), pd.Timestamp("1970-01-01 00:00:00"))
        self.assertEqual(convert_timestamp("00000000000001"),   pd.Timestamp("1970-01-01 00:00:01"))
        self.assertEqual(convert_timestamp("1000000001"),       pd.Timestamp("2001-09-09 01:46:41"))
        self.assertEqual(convert_timestamp("5000000000"),       pd.Timestamp("2128-06-11 08:53:20"))
        # 10_000_000_000 is the threshold for milliseconds
        self.assertEqual(convert_timestamp("10000000001"),      pd.Timestamp("1970-04-26 17:46:40.001000"))
        # 100_000_000_000_000 is the threshold for nanoseconds
        self.assertEqual(convert_timestamp("100000000000001"),  pd.Timestamp("1970-01-02 03:46:40.000000001"))

    # def test_convert_minute_csv_to_parquet(self):
    #     # Test converting a minute-level CSV file to Parquet format
    #     csv_path = "data/polygon/flatfiles/us_stocks_sip/minute_aggs_v1/AAPL.csv"
    #     parquet_path = "data/polygon/flatfiles/us_stocks_sip/minute_aggs_v1/AAPL.parquet"
    #     convert_minute_csv_to_parquet(csv_path, extension=".csv", compression="infer", force=True)
    #     self.assertTrue(os.path.exists(parquet_path))

    # def test_process_all_minute_csv_to_parquet(self):
    #     # Test processing all minute-level CSV files in a directory and converting them to Parquet format
    #     data_dir = "data/polygon/flatfiles/us_stocks_sip/minute_aggs_v1"
    #     convert_extension = ".csv"
    #     convert_compression = "infer"
    #     convert_force = True
    #     max_workers = 8
    #     process_all_minute_csv_to_parquet(data_dir, extension=convert_extension, compression=convert_compression, force=convert_force, max_workers=max_workers)
    #     # Assert that all CSV files in the directory have been converted to Parquet
    #     csv_files = glob.glob(os.path.join(data_dir, f"*{convert_extension}"))
    #     parquet_files = glob.glob(os.path.join(data_dir, "*.parquet"))
    #     self.assertEqual(len(csv_files), len(parquet_files))


if __name__ == "__main__":
    unittest.main()
