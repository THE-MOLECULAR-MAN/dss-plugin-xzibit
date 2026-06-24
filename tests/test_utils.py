"""Unit tests for python-lib/xzibit/utils.py.

All tests target pure-Python utility functions that require no live DSS
connection.  Functions that call dataiku.api_client() are tested via
unittest.mock.patch so they run offline.
"""

import os
import tempfile
from datetime import datetime
from unittest.mock import patch

import pytest

from xzibit.utils import (
    compare_major_minor_versions,
    extract_keys,
    extract_nested_keys,
    flatten_dict,
    get_dss_base_url,
    get_jq_value,
    get_path_size,
    get_path_size_megabytes,
    get_values_for_key,
    get_values_from_list_of_dicts,
    int_to_datetime,
    list_keys_recursive,
    list_to_error_dict,
    parse_user_datetime,
    recursive_search_all,
    remove_prefix_from_keys,
    replace_empty_arrays_sets_with_none,
)


# ── compare_major_minor_versions ──────────────────────────────────────────


class TestCompareMajorMinorVersions:
    def test_a_less_than_b(self):
        assert compare_major_minor_versions("3.9", "3.11") == -1

    def test_a_greater_than_b(self):
        assert compare_major_minor_versions("3.11", "3.9") == 1

    def test_equal(self):
        assert compare_major_minor_versions("3.9", "3.9") == 0

    def test_major_version_gap(self):
        assert compare_major_minor_versions("2.7", "3.9") == -1
        assert compare_major_minor_versions("3.9", "2.7") == 1

    def test_minor_zero_vs_ten(self):
        assert compare_major_minor_versions("3.0", "3.10") == -1

    def test_invalid_input_returns_minus_two(self):
        assert compare_major_minor_versions("Unknown", "3.9") == -2

    def test_empty_string_returns_minus_two(self):
        assert compare_major_minor_versions("", "3.9") == -2

    def test_both_invalid_returns_minus_two(self):
        assert compare_major_minor_versions("abc", "xyz") == -2


# ── extract_keys ─────────────────────────────────────────────────────────


class TestExtractKeys:
    def test_extracts_keys_where_value_is_true(self):
        d = {"allowAlpha": True, "allowBeta": False, "other": True}
        result = extract_keys(d)
        assert "Alpha" in result
        assert "Beta" not in result
        assert "other" not in result

    def test_extracts_keys_where_value_is_false(self):
        d = {"allowAlpha": True, "allowBeta": False}
        result = extract_keys(d, v=False)
        assert "Beta" in result
        assert "Alpha" not in result

    def test_empty_dict_returns_empty_list(self):
        assert extract_keys({}) == []

    def test_no_matching_prefix(self):
        assert extract_keys({"other": True}) == []

    def test_custom_prefix(self):
        d = {"blockX": True, "blockY": True, "ignoreZ": True}
        result = extract_keys(d, v=True, key_prefix="block")
        assert "X" in result
        assert "Y" in result
        assert "ignoreZ" not in result


# ── recursive_search_all ──────────────────────────────────────────────────


class TestRecursiveSearchAll:
    def test_finds_value_in_nested_dict(self):
        data = {"a": {"b": "target"}}
        result = recursive_search_all(data, "target")
        assert result is not None
        assert "target" in result

    def test_returns_none_when_not_found(self):
        assert recursive_search_all({"a": 1}, "missing") is None

    def test_finds_value_in_list(self):
        data = {"items": ["x", "y", "target"]}
        result = recursive_search_all(data, "target")
        assert result is not None
        assert "target" in result

    def test_finds_key_match_returns_its_value(self):
        data = {"target": "some_value"}
        result = recursive_search_all(data, "target")
        assert result is not None
        assert "some_value" in result

    def test_empty_dict_returns_none(self):
        assert recursive_search_all({}, "anything") is None

    def test_deeply_nested(self):
        data = {"a": {"b": {"c": "deep"}}}
        result = recursive_search_all(data, "deep")
        assert result is not None

    def test_value_in_list_of_dicts(self):
        data = {"items": [{"name": "alice"}, {"name": "target"}]}
        result = recursive_search_all(data, "target")
        assert result is not None


# ── replace_empty_arrays_sets_with_none ────────────────────────────────────


class TestReplaceEmptyArraysSetsWithNone:
    def test_none_input(self):
        assert replace_empty_arrays_sets_with_none(None) is None

    def test_empty_list(self):
        assert replace_empty_arrays_sets_with_none([]) is None

    def test_empty_dict(self):
        assert replace_empty_arrays_sets_with_none({}) is None

    def test_empty_set(self):
        assert replace_empty_arrays_sets_with_none(set()) is None

    def test_string_bracket_list(self):
        assert replace_empty_arrays_sets_with_none("[]") is None

    def test_string_bracket_dict(self):
        assert replace_empty_arrays_sets_with_none("{}") is None

    def test_non_empty_list_unchanged(self):
        assert replace_empty_arrays_sets_with_none([1, 2, 3]) == [1, 2, 3]

    def test_non_empty_dict_unchanged(self):
        assert replace_empty_arrays_sets_with_none({"a": 1}) == {"a": 1}

    def test_non_empty_string_unchanged(self):
        assert replace_empty_arrays_sets_with_none("hello") == "hello"

    def test_integer_unchanged(self):
        assert replace_empty_arrays_sets_with_none(42) == 42

    def test_zero_unchanged(self):
        assert replace_empty_arrays_sets_with_none(0) == 0


# ── list_keys_recursive ───────────────────────────────────────────────────


class TestListKeysRecursive:
    def test_flat_dict(self):
        result = list_keys_recursive({"a": 1, "b": 2})
        assert "a" in result
        assert "b" in result

    def test_nested_dict(self):
        result = list_keys_recursive({"a": {"b": 2}})
        assert "a" in result
        assert "a.b" in result

    def test_deeply_nested(self):
        result = list_keys_recursive({"a": {"b": {"c": 3}}})
        assert "a.b.c" in result

    def test_list_values_traversed(self):
        result = list_keys_recursive({"items": [{"name": "x"}]})
        assert "items" in result
        assert "items.name" in result

    def test_non_dict_input_returns_none(self):
        assert list_keys_recursive("not a dict") is None

    def test_non_dict_input_list_returns_none(self):
        assert list_keys_recursive([1, 2, 3]) is None


# ── extract_nested_keys ───────────────────────────────────────────────────


class TestExtractNestedKeys:
    def test_extracts_simple_key(self):
        result = extract_nested_keys({"a": 1}, ["a"])
        assert result == {"a": 1}

    def test_extracts_nested_key(self):
        result = extract_nested_keys({"a": {"b": 42}}, ["a.b"])
        assert result == {"a.b": 42}

    def test_missing_key_returns_none(self):
        result = extract_nested_keys({}, ["a.b"])
        assert result == {"a.b": None}

    def test_multiple_keys(self):
        d = {"name": "test", "meta": {"version": "1.0"}}
        result = extract_nested_keys(d, ["name", "meta.version"])
        assert result["name"] == "test"
        assert result["meta.version"] == "1.0"

    def test_partial_nested_path_returns_none(self):
        result = extract_nested_keys({"a": 1}, ["a.b.c"])
        assert result["a.b.c"] is None

    def test_empty_keys_list(self):
        assert extract_nested_keys({"a": 1}, []) == {}


# ── int_to_datetime ───────────────────────────────────────────────────────


class TestIntToDatetime:
    def test_epoch_zero(self):
        assert int_to_datetime(0) == datetime(1970, 1, 1, 0, 0, 0)

    def test_seconds_timestamp_returns_datetime(self):
        result = int_to_datetime(1000)
        assert isinstance(result, datetime)

    def test_milliseconds_are_converted(self):
        # 1.5e12 ms > 1e12 threshold → converted to 1.5e9 s (year 2017)
        result = int_to_datetime(1_500_000_000_000)
        assert result.year == 2017

    def test_none_input_defaults_to_epoch(self):
        assert int_to_datetime(None) == datetime(1970, 1, 1, 0, 0, 0)

    def test_string_input_defaults_to_epoch(self):
        assert int_to_datetime("not-an-int") == datetime(1970, 1, 1, 0, 0, 0)

    def test_float_input_defaults_to_epoch(self):
        # float is not int, so treated as zero
        assert int_to_datetime(3.14) == datetime(1970, 1, 1, 0, 0, 0)


# ── parse_user_datetime ───────────────────────────────────────────────────


class TestParseUserDatetime:
    def test_valid_iso_datetime_with_timezone(self):
        result = parse_user_datetime("2025-11-11 15:08:36.439000+00:00")
        assert result is not None
        assert result.year == 2025
        assert result.month == 11
        assert result.day == 11
        assert result.hour == 15

    def test_invalid_string_returns_none(self):
        assert parse_user_datetime("not-a-date") is None

    def test_empty_string_returns_none(self):
        assert parse_user_datetime("") is None

    def test_date_only_string(self):
        result = parse_user_datetime("2024-01-15")
        assert result is not None
        assert result.year == 2024

    def test_space_separator_normalised(self):
        result = parse_user_datetime("2023-06-01 12:00:00")
        assert result is not None


# ── get_jq_value ──────────────────────────────────────────────────────────


class TestGetJqValue:
    def test_simple_key(self):
        assert get_jq_value({"a": 1}, "a") == 1

    def test_nested_key(self):
        assert get_jq_value({"a": {"b": 42}}, "a.b") == 42

    def test_deeply_nested(self):
        d = {"x": {"y": {"z": "found"}}}
        assert get_jq_value(d, "x.y.z") == "found"

    def test_missing_key_returns_none(self):
        assert get_jq_value({}, "a.b") is None

    def test_partial_path_returns_none(self):
        assert get_jq_value({"a": 1}, "a.b") is None

    def test_empty_path(self):
        # single empty-string key navigates to same dict value
        result = get_jq_value({"": "val"}, "")
        assert result == "val"


# ── list_to_error_dict ────────────────────────────────────────────────────


class TestListToErrorDict:
    def test_default_error_value(self):
        assert list_to_error_dict(["a", "b"]) == {"a": "error", "b": "error"}

    def test_custom_value(self):
        assert list_to_error_dict(["x"], value="FAIL") == {"x": "FAIL"}

    def test_empty_list(self):
        assert list_to_error_dict([]) == {}

    def test_single_key(self):
        result = list_to_error_dict(["only"])
        assert result == {"only": "error"}


# ── get_values_for_key ────────────────────────────────────────────────────


class TestGetValuesForKey:
    def test_extracts_unique_values(self):
        data = [{"k": 1}, {"k": 2}, {"k": 1}]
        assert get_values_for_key(data, "k") == {1, 2}

    def test_missing_key_skipped(self):
        data = [{"k": 1}, {"other": 2}]
        assert get_values_for_key(data, "k") == {1}

    def test_empty_list_returns_empty_set(self):
        assert get_values_for_key([], "k") == set()

    def test_non_dict_items_are_skipped(self):
        data = [{"k": 1}, "not a dict", 42]
        assert get_values_for_key(data, "k") == {1}

    def test_key_not_present_at_all(self):
        assert get_values_for_key([{"a": 1}], "missing") == set()


# ── get_values_from_list_of_dicts ─────────────────────────────────────────


class TestGetValuesFromListOfDicts:
    def test_extracts_all_values(self):
        result = get_values_from_list_of_dicts([{"a": 1, "b": 2}])
        assert 1 in result
        assert 2 in result

    def test_no_duplicates_across_dicts(self):
        result = get_values_from_list_of_dicts([{"a": 1}, {"b": 1}])
        assert result.count(1) == 1

    def test_non_dict_items_are_skipped(self):
        result = get_values_from_list_of_dicts(["not a dict", {"a": 99}])
        assert 99 in result

    def test_empty_list_returns_empty_list(self):
        assert get_values_from_list_of_dicts([]) == []

    def test_preserves_order_of_first_appearance(self):
        result = get_values_from_list_of_dicts([{"a": 10, "b": 20}])
        assert result.index(10) < result.index(20)


# ── flatten_dict ─────────────────────────────────────────────────────────


class TestFlattenDict:
    def test_flat_dict_is_unchanged(self):
        assert flatten_dict({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    def test_nested_dict_is_flattened(self):
        assert flatten_dict({"a": {"b": 1}}) == {"a.b": 1}

    def test_deeply_nested(self):
        assert flatten_dict({"a": {"b": {"c": 1}}}) == {"a.b.c": 1}

    def test_include_keys_filter_keeps_matching(self):
        result = flatten_dict({"a": {"b": 1, "c": 2}}, include_keys=["b"])
        assert "a.b" in result
        assert "a.c" not in result

    def test_custom_separator(self):
        result = flatten_dict({"a": {"b": 1}}, sep="_")
        assert "a_b" in result

    def test_mixed_flat_and_nested(self):
        result = flatten_dict({"top": "val", "a": {"b": 1}})
        assert result["top"] == "val"
        assert result["a.b"] == 1


# ── remove_prefix_from_keys ───────────────────────────────────────────────


class TestRemovePrefixFromKeys:
    def test_removes_matching_prefix(self):
        result = remove_prefix_from_keys({"versionTag.modified": "v1"}, "versionTag")
        assert "modified" in result

    def test_non_matching_keys_unchanged(self):
        result = remove_prefix_from_keys({"other": "x"}, "versionTag")
        assert "other" in result

    def test_recursive_nested_dicts(self):
        d = {"prefix.a": {"prefix.b": 1}}
        result = remove_prefix_from_keys(d, "prefix")
        assert "a" in result
        assert "b" in result["a"]

    def test_empty_dict_returns_empty(self):
        assert remove_prefix_from_keys({}, "prefix") == {}

    def test_prefix_not_present_leaves_keys_intact(self):
        result = remove_prefix_from_keys({"hello": 1}, "prefix")
        assert result == {"hello": 1}


# ── get_path_size / get_path_size_megabytes ───────────────────────────────


class TestGetPathSize:
    def test_nonexistent_path_returns_zero(self):
        assert get_path_size("/nonexistent/path/xyz_does_not_exist") == 0

    def test_file_size_matches_content(self):
        content = b"hello world"
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(content)
            path = f.name
        try:
            assert get_path_size(path) == len(content)
        finally:
            os.unlink(path)

    def test_directory_size_includes_files(self):
        with tempfile.TemporaryDirectory() as d:
            fp = os.path.join(d, "test.txt")
            with open(fp, "wb") as f:
                f.write(b"x" * 100)
            assert get_path_size(d) >= 100

    def test_empty_directory_returns_zero_or_small(self):
        with tempfile.TemporaryDirectory() as d:
            assert get_path_size(d) == 0


class TestGetPathSizeMegabytes:
    def test_one_megabyte_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"x" * 1024 * 1024)
            path = f.name
        try:
            size_mb = get_path_size_megabytes(path)
            assert abs(size_mb - 1.0) < 0.01
        finally:
            os.unlink(path)

    def test_nonexistent_path_returns_zero(self):
        assert get_path_size_megabytes("/nonexistent/xyz") == 0.0


# ── get_dss_base_url ──────────────────────────────────────────────────────


class TestGetDssBaseUrl:
    def test_returns_env_url_first(self):
        with patch("xzibit.utils.get_dss_url_from_env", return_value="https://dss.local:10000"):
            with patch("xzibit.utils.get_dss_external_url", return_value=None):
                with patch("xzibit.utils.get_dss_url_from_global_vars", return_value=None):
                    result = get_dss_base_url()
        assert result == "https://dss.local:10000"

    def test_strips_trailing_slash(self):
        with patch("xzibit.utils.get_dss_url_from_env", return_value="https://dss.local/"):
            with patch("xzibit.utils.get_dss_external_url", return_value=None):
                with patch("xzibit.utils.get_dss_url_from_global_vars", return_value=None):
                    result = get_dss_base_url()
        assert result == "https://dss.local"
        assert not result.endswith("/")

    def test_falls_back_to_external_url(self):
        with patch("xzibit.utils.get_dss_url_from_env", return_value=None):
            with patch("xzibit.utils.get_dss_external_url", return_value="https://admin.example.com"):
                with patch("xzibit.utils.get_dss_url_from_global_vars", return_value=None):
                    result = get_dss_base_url()
        assert result == "https://admin.example.com"

    def test_falls_back_to_global_vars(self):
        with patch("xzibit.utils.get_dss_url_from_env", return_value=None):
            with patch("xzibit.utils.get_dss_external_url", return_value=None):
                with patch("xzibit.utils.get_dss_url_from_global_vars", return_value="https://global.example.com"):
                    result = get_dss_base_url()
        assert result == "https://global.example.com"

    def test_returns_none_when_all_sources_return_none(self):
        with patch("xzibit.utils.get_dss_url_from_env", return_value=None):
            with patch("xzibit.utils.get_dss_external_url", return_value=None):
                with patch("xzibit.utils.get_dss_url_from_global_vars", return_value=None):
                    result = get_dss_base_url()
        assert result is None
