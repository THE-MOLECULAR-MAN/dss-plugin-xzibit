"""Unit tests for python-lib/xzibit/deprecations.py.

These tests exercise the CSV-backed lookup functions.  They read the real
CSV files from the repository, so no mocking is required for the file I/O
path.  The tests remain offline (no DSS connection needed).
"""

import pytest

from xzibit.deprecations import (
    load_local_csv_as_dataframe,
    lookup_python_support,
    lookup_recipe_deprecation_status,
    DEPRECATED_PLUGIN_IDS,
    DEPRECATED_PREPROCESSORS,
    DSS_BUILT_IN_PLUGIN_IDS,
)


# ── load_local_csv_as_dataframe ───────────────────────────────────────────


class TestLoadLocalCsvAsDataframe:
    def test_loads_python_support_csv(self):
        df = load_local_csv_as_dataframe("DSS_version_python_support.csv")
        assert df is not None
        assert not df.empty
        assert "DSS_Major_Version" in df.columns

    def test_loads_recipe_deprecation_csv(self):
        df = load_local_csv_as_dataframe("DSS_recipe_deprecation_status.csv")
        assert df is not None
        assert not df.empty
        assert "recipe_type" in df.columns
        assert "DSS_v14_recipe_deprecation_status" in df.columns

    def test_all_values_read_as_strings(self):
        df = load_local_csv_as_dataframe("DSS_version_python_support.csv")
        # dtype=str ensures values are Python strings (pandas may use StringDtype or object)
        assert isinstance(df["DSS_Major_Version"].iloc[0], str)

    def test_nonexistent_file_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_local_csv_as_dataframe("does_not_exist.csv")


# ── lookup_python_support ─────────────────────────────────────────────────


class TestLookupPythonSupport:
    @pytest.fixture(autouse=True)
    def load_df(self):
        self.df = load_local_csv_as_dataframe("DSS_version_python_support.csv")

    # DSS 12 support matrix
    def test_dss12_py39_is_supported(self):
        assert lookup_python_support("12", "3.9", self.df) == "supported"

    def test_dss12_py38_is_supported(self):
        assert lookup_python_support("12", "3.8", self.df) == "supported"

    def test_dss12_py27_is_partial(self):
        assert lookup_python_support("12", "2.7", self.df) == "partial"

    def test_dss12_py34_is_deprecated(self):
        assert lookup_python_support("12", "3.4", self.df) == "deprecated"

    def test_dss12_py312_is_unsupported(self):
        assert lookup_python_support("12", "3.12", self.df) == "unsupported"

    # DSS 13 support matrix
    def test_dss13_py39_is_supported(self):
        assert lookup_python_support("13", "3.9", self.df) == "supported"

    def test_dss13_py37_is_deprecated(self):
        assert lookup_python_support("13", "3.7", self.df) == "deprecated"

    # DSS 14 support matrix
    def test_dss14_py39_is_supported(self):
        assert lookup_python_support("14", "3.9", self.df) == "supported"

    def test_dss14_py38_is_deprecated(self):
        assert lookup_python_support("14", "3.8", self.df) == "deprecated"

    def test_dss14_py314_is_experimental(self):
        assert lookup_python_support("14", "3.14", self.df) == "experimental"

    # Edge cases
    def test_unknown_dss_version_returns_not_found(self):
        result = lookup_python_support("99", "3.9", self.df)
        assert "Not Found" in result

    def test_unknown_python_version_returns_unknown(self):
        result = lookup_python_support("12", "99.9", self.df)
        assert result == "Unknown"

    def test_whitespace_is_stripped(self):
        assert lookup_python_support(" 12 ", " 3.9 ", self.df) == "supported"


# ── lookup_recipe_deprecation_status ─────────────────────────────────────


class TestLookupRecipeDeprecationStatus:
    @pytest.fixture(autouse=True)
    def load_df(self):
        self.df = load_local_csv_as_dataframe("DSS_recipe_deprecation_status.csv")

    def test_sync_is_safe(self):
        assert lookup_recipe_deprecation_status("sync", self.df) == "Safe"

    def test_join_is_safe(self):
        assert lookup_recipe_deprecation_status("join", self.df) == "Safe"

    def test_grouping_is_safe(self):
        assert lookup_recipe_deprecation_status("grouping", self.df) == "Safe"

    def test_python_recipe_needs_manual_checking(self):
        assert lookup_recipe_deprecation_status("python", self.df) == "Needs manual checking"

    def test_shell_recipe_needs_manual_checking(self):
        assert lookup_recipe_deprecation_status("shell", self.df) == "Needs manual checking"

    def test_r_recipe_usually_safe(self):
        assert lookup_recipe_deprecation_status("r", self.df) == "Usually safe"

    def test_unknown_recipe_type_returns_unknown(self):
        assert lookup_recipe_deprecation_status("nonexistent_type_xyz", self.df) == "Unknown"

    def test_empty_string_returns_unknown(self):
        assert lookup_recipe_deprecation_status("", self.df) == "Unknown"

    def test_whitespace_is_stripped(self):
        assert lookup_recipe_deprecation_status(" sync ", self.df) == "Safe"


# ── Constants ─────────────────────────────────────────────────────────────


class TestDeprecationConstants:
    def test_deprecated_plugin_ids_is_a_set(self):
        assert isinstance(DEPRECATED_PLUGIN_IDS, set)

    def test_deprecated_preprocessors_is_a_set(self):
        assert isinstance(DEPRECATED_PREPROCESSORS, set)

    def test_built_in_plugin_ids_is_a_list(self):
        assert isinstance(DSS_BUILT_IN_PLUGIN_IDS, list)

    def test_known_deprecated_plugin_present(self):
        assert "looker-query" in DEPRECATED_PLUGIN_IDS

    def test_known_built_in_plugin_present(self):
        assert "default-samples" in DSS_BUILT_IN_PLUGIN_IDS

    def test_known_deprecated_preprocessor_present(self):
        assert "AnonymizerProcessor" in DEPRECATED_PREPROCESSORS
