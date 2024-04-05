"""Tests for functions in extract.py"""
from unittest.mock import MagicMock

import pytest

from extract import (get_bucket_names, get_bucket_objects)


def test_get_bucket_names():
    """Testing if a MagicMock S3 client successfully returns a list
    of bucket names."""

    fake_client = MagicMock()
    fake_client.list_buckets.return_value = {"Buckets": [
        {"Name": "Hi"},
        {"Name": "Hello"},
        {"Name": "Cya!"}
    ]
    }

    result = get_bucket_names(fake_client)

    assert result == ["Hi", "Hello", "Cya!"]


def test_get_bucket_objects():
    """Testing if a MagicMock S3 client successfully returns a list
    of objects, given a bucket name."""

    fake_client = MagicMock()
    fake_client.list_objects(Bucket='bucket').return_value = {'Contents': [
        {'Key': 'response.json'},
        {'Key': 'untitled.csv'},
        {'Key': 'hi.docx'}
    ]
    }

    result = get_bucket_objects(fake_client, 'bucket')

    assert result == ['response.json', 'untitled.csv', 'hi.docx']
