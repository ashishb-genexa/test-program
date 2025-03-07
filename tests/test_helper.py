import pytest
import src.util.helper as helper

@pytest.mark.parametrize("a, expected", [
    ("http://www.test.com", True),
    ("www.test.com",False),
    ("https://www.test.com", True),
    ("https://test.com", True),
    ("https://test.com", True),
])

def test_is_valid_url_pass(a, expected):
  assert(helper.is_valid_url(a) == expected)