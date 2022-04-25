from pipe_spire.normalization import date_range_parse, build_description, get_version
from datetime import datetime as dt
import pytest

class TestNormalization:
    def test_data_range(self):
        date = "2012-01-01"
        assert [dt(2012,1,1)] == date_range_parse(date)
        date = "2012-01-01,2012-01-02"
        assert [dt(2012,1,1),dt(2012,1,2)] == date_range_parse(date)

    def test_build_description(self):
        expected = """   * Pipeline: pipe-spire 1.0.0
    * Source: source
    * Command: ./main.py normalize -i source -o destination -dr 2012-01-01"""
        assert expected == build_description('source','destination','2012-01-01')

    def test_get_version(self):
        assert '1.0.0' == get_version()
