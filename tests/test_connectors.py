"""Unit tests for pure functions and URL-construction methods across connectors.

Each connector class is instantiated via __new__ so we can set the private
``__baseurl`` attribute without invoking the constructor (which would call
``dataiku.api_client()``).  This keeps the tests offline and fast.
"""

from unittest.mock import MagicMock, patch

import pytest

from helpers import load_connector

# Load connector modules once at module import time.
_webapps_mod = load_connector("xzibit_webapps")
_plugins_mod = load_connector("xzibit_plugins")
_pyanalysis_mod = load_connector("xzibit_pythonanalysis")
_recipes_mod = load_connector("xzibit_recipes")
_apps_mod = load_connector("xzibit_apps")
_bundles_mod = load_connector("xzibit_bundles")
_clusters_mod = load_connector("xzibit_clusters")
_codeenvs_mod = load_connector("xzibit_codeenvs")
_connections_mod = load_connector("xzibit_connections")
_datasets_mod = load_connector("xzibit_datasets")
_deployments_mod = load_connector("xzibit_deployments")
_meanings_mod = load_connector("xzibit_meanings")
_pluginusages_mod = load_connector("xzibit_pluginusages")
_projects_mod = load_connector("xzibit_projects")
_users_mod = load_connector("xzibit_users")


def _make_connector(cls, base_url="https://my-dss.example.com"):
    """Instantiate a connector class without calling __init__.

    Sets the name-mangled private ``__baseurl`` attribute directly so
    ``get_url()`` methods can be tested in isolation.
    """
    obj = object.__new__(cls)
    # DSS connector classes use Python name-mangling: _ClassName__baseurl
    mangled = f"_{cls.__name__}__baseurl"
    setattr(obj, mangled, base_url)
    return obj


# ── xzibit_webapps: make_url_friendly ────────────────────────────────────


class TestMakeUrlFriendly:
    fn = staticmethod(_webapps_mod.make_url_friendly)

    def test_lowercase_spaces_to_hyphens(self):
        assert self.fn("Hello World") == "hello-world"

    def test_special_characters_removed(self):
        assert self.fn("My App!") == "my-app"

    def test_multiple_spaces_collapse_to_one_hyphen(self):
        assert self.fn("A  B") == "a-b"

    def test_numbers_preserved(self):
        assert self.fn("App 123") == "app-123"

    def test_empty_string(self):
        assert self.fn("") == ""

    def test_non_string_coerced(self):
        result = self.fn(42)
        assert result == "42"

    def test_all_special_chars_become_empty(self):
        assert self.fn("!!!") == ""

    def test_already_url_friendly(self):
        assert self.fn("myapp") == "myapp"


# ── xzibit_webapps: ConnectorWebApps.get_url ─────────────────────────────


class TestConnectorWebAppsGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_webapps_mod.ConnectorWebApps)

    def test_valid_inputs(self):
        url = self.conn.get_url("PROJ", "abc123", "My App")
        assert url == "https://my-dss.example.com/projects/PROJ/webapps/abc123_my-app/edit"

    def test_none_project_key_returns_none(self):
        assert self.conn.get_url(None, "abc123", "My App") is None

    def test_none_webapp_id_returns_none(self):
        assert self.conn.get_url("PROJ", None, "My App") is None

    def test_none_webapp_name_returns_none(self):
        assert self.conn.get_url("PROJ", "abc123", None) is None

    def test_none_baseurl_returns_none(self):
        conn = _make_connector(_webapps_mod.ConnectorWebApps, base_url=None)
        assert conn.get_url("PROJ", "abc123", "App") is None

    def test_url_contains_safe_name(self):
        url = self.conn.get_url("PROJ", "id1", "Hello World!")
        assert "hello-world" in url


# ── xzibit_plugins: extract_allow_keys ───────────────────────────────────


class TestExtractAllowKeys:
    fn = staticmethod(_plugins_mod.extract_allow_keys)

    def test_extracts_allow_prefixed_keys(self):
        d = {"allowGPT4": True, "allowGPT3": False, "maxTokens": 1000}
        result = self.fn(d)
        assert "GPT4" in result
        assert "GPT3" in result
        assert "maxTokens" not in result

    def test_preserves_values(self):
        result = self.fn({"allowX": True, "allowY": False})
        assert result["X"] is True
        assert result["Y"] is False

    def test_empty_dict_returns_empty(self):
        assert self.fn({}) == {}

    def test_no_allow_keys_returns_empty(self):
        assert self.fn({"other": True}) == {}

    def test_non_dict_raises_value_error(self):
        with pytest.raises(ValueError):
            self.fn("not a dict")

    def test_non_dict_list_raises_value_error(self):
        with pytest.raises(ValueError):
            self.fn([1, 2, 3])


# ── xzibit_plugins: ConnectorPlugins.get_url ─────────────────────────────


class TestConnectorPluginsGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_plugins_mod.ConnectorPlugins)

    def test_valid_plugin_id(self):
        url = self.conn.get_url("my-plugin")
        assert url == "https://my-dss.example.com/plugins/my-plugin/summary/"

    def test_none_plugin_id_returns_none(self):
        assert self.conn.get_url(None) is None

    def test_none_baseurl_returns_none(self):
        conn = _make_connector(_plugins_mod.ConnectorPlugins, base_url=None)
        assert conn.get_url("some-plugin") is None


# ── xzibit_pythonanalysis: get_tuples_only ────────────────────────────────


class TestGetTuplesOnly:
    fn = staticmethod(_pyanalysis_mod.get_tuples_only)

    def test_returns_only_tuples(self):
        result = self.fn([1, (3, 9), "abc", (3, 11)])
        assert result == [(3, 9), (3, 11)]

    def test_empty_list(self):
        assert self.fn([]) == []

    def test_no_tuples_returns_empty(self):
        assert self.fn(["a", 1, None]) == []

    def test_all_tuples(self):
        result = self.fn([(1, 0), (3, 9)])
        assert result == [(1, 0), (3, 9)]


# ── xzibit_pythonanalysis: get_code_sample ────────────────────────────────


class TestGetCodeSample:
    def setup_method(self):
        self.conn = object.__new__(_pyanalysis_mod.ConnectorPythonAnalysis)

    def test_returns_first_n_lines(self):
        code = "\n".join(f"line{i}" for i in range(100))
        sample = self.conn.get_code_sample(code, max_lines=5)
        # 5 content lines + "\n..." appended = 5 newlines total
        assert "line0" in sample
        assert "line4" in sample
        assert "line5" not in sample
        assert sample.startswith("line0")

    def test_appends_ellipsis_when_truncated(self):
        code = "\n".join(["a", "b", "c", "d", "e"])
        sample = self.conn.get_code_sample(code, max_lines=3)
        assert sample.endswith("...")

    def test_no_ellipsis_when_within_limit(self):
        code = "line1\nline2"
        sample = self.conn.get_code_sample(code, max_lines=5)
        assert not sample.endswith("...")

    def test_empty_code_returns_empty_string(self):
        assert self.conn.get_code_sample("") == ""

    def test_single_line_no_truncation(self):
        assert self.conn.get_code_sample("only one line", max_lines=40) == "only one line"


# ── xzibit_pythonanalysis: check_default_snippets_presence ───────────────


class TestCheckDefaultSnippetsPresence:
    def setup_method(self):
        self.conn = object.__new__(_pyanalysis_mod.ConnectorPythonAnalysis)

    def test_detects_default_snippet(self):
        code = "_df = ... # Compute a Pandas dataframe to write into "
        assert self.conn.check_default_snippets_presence(code) is True

    def test_custom_code_returns_false(self):
        code = "import pandas as pd\ndf = pd.DataFrame({'a': [1, 2, 3]})"
        assert self.conn.check_default_snippets_presence(code) is False

    def test_empty_code_returns_false(self):
        assert self.conn.check_default_snippets_presence("") is False

    def test_crlf_normalized(self):
        # The snippet uses \n; if code uses \r\n it should still match
        snippet_with_crlf = "_df = ... # Compute a Pandas dataframe to write into \r\n"
        assert self.conn.check_default_snippets_presence(snippet_with_crlf) is True

    def test_partial_snippet_no_match(self):
        code = "# Read recipe inputs"
        assert self.conn.check_default_snippets_presence(code) is False


# ── xzibit_recipes: get_unique_types ─────────────────────────────────────


class TestGetUniqueTypes:
    fn = staticmethod(_recipes_mod.get_unique_types)

    def test_deduplicates_types(self):
        data = [{"type": "join"}, {"type": "grouping"}, {"type": "join"}]
        assert self.fn(data) == {"join", "grouping"}

    def test_empty_list(self):
        assert self.fn([]) == set()

    def test_items_without_type_key_ignored(self):
        data = [{"other": "x"}, {"type": "sync"}]
        assert self.fn(data) == {"sync"}

    def test_single_item(self):
        assert self.fn([{"type": "python"}]) == {"python"}


# ── xzibit_recipes: prepare_recipe_has_deprecated_preprocessors ──────────


class TestPrepareRecipeHasDeprecatedPreprocessors:
    fn = staticmethod(_recipes_mod.prepare_recipe_has_deprecated_preprocessors)

    def _make_recipe_handle(self, recipe_type, steps):
        handle = MagicMock()
        settings = MagicMock()
        settings.type = recipe_type
        settings.obj_payload = {"steps": steps}
        handle.get_settings.return_value = settings
        return handle

    def test_no_deprecated_steps_returns_empty_list(self):
        handle = self._make_recipe_handle("shaker", [{"type": "ColumnsSelector"}])
        assert self.fn(handle) == []

    def test_deprecated_step_returns_list_with_name(self):
        handle = self._make_recipe_handle("shaker", [{"type": "AnonymizerProcessor"}])
        result = self.fn(handle)
        assert "AnonymizerProcessor" in result

    def test_non_shaker_recipe_returns_empty(self):
        handle = self._make_recipe_handle("python", [{"type": "AnonymizerProcessor"}])
        assert self.fn(handle) == []

    def test_multiple_deprecated_steps(self):
        steps = [
            {"type": "AnonymizerProcessor"},
            {"type": "MemoryEquiJoiner"},
        ]
        handle = self._make_recipe_handle("shaker", steps)
        result = self.fn(handle)
        assert len(result) >= 2

    def test_exception_in_recipe_handle_returns_empty(self):
        handle = MagicMock()
        handle.get_settings.side_effect = Exception("broken")
        assert self.fn(handle) == []


# ── xzibit_apps: ConnectorApps.get_url ───────────────────────────────────


class TestConnectorAppsGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_apps_mod.ConnectorApps)

    def test_valid_app_id(self):
        url = self.conn.get_url("MY_APP_ID")
        assert url == "https://my-dss.example.com/apps/MY_APP_ID"

    def test_none_id_returns_none(self):
        assert self.conn.get_url(None) is None

    def test_url_has_no_trailing_slash(self):
        url = self.conn.get_url("APP1")
        assert not url.endswith("/")


# ── xzibit_clusters: ConnectorClusters.get_url ───────────────────────────


class TestConnectorClustersGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_clusters_mod.ConnectorClusters)

    def test_valid_cluster_id(self):
        url = self.conn.get_url("my-cluster")
        assert url == "https://my-dss.example.com/admin/clusters/my-cluster"

    def test_none_id_returns_none(self):
        assert self.conn.get_url(None) is None


# ── xzibit_connections: ConnectorConnections.get_url ─────────────────────


class TestConnectorConnectionsGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_connections_mod.ConnectorConnections)

    def test_valid_connection_name(self):
        url = self.conn.get_url("my_conn")
        assert url == "https://my-dss.example.com/admin/connections/my_conn/"

    def test_url_has_trailing_slash(self):
        url = self.conn.get_url("conn")
        assert url.endswith("/")

    def test_none_id_returns_none(self):
        assert self.conn.get_url(None) is None


# ── xzibit_datasets: ConnectorDatasets.get_url ───────────────────────────


class TestConnectorDatasetsGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_datasets_mod.ConnectorDatasets)

    def test_valid_inputs(self):
        url = self.conn.get_url("my_ds", "MYPROJECT")
        assert "MYPROJECT" in url
        assert "my_ds" in url
        assert "explore" in url

    def test_none_id_returns_none(self):
        assert self.conn.get_url(None, "PROJ") is None

    def test_none_project_key_returns_none(self):
        assert self.conn.get_url("ds", None) is None


# ── xzibit_projects: ConnectorProjects.get_url ───────────────────────────


class TestConnectorProjectsGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_projects_mod.ConnectorProjects)

    def test_valid_project_key(self):
        url = self.conn.get_url("MY_PROJECT")
        assert url == "https://my-dss.example.com/projects/MY_PROJECT/flow/"

    def test_none_project_key_returns_none(self):
        assert self.conn.get_url(None) is None


# ── xzibit_users: ConnectorUsers.get_url ─────────────────────────────────


class TestConnectorUsersGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_users_mod.ConnectorUsers)

    def test_valid_user_login(self):
        url = self.conn.get_url("alice")
        assert "alice" in url
        assert "/admin/security/users/edit/" in url

    def test_none_login_returns_none(self):
        assert self.conn.get_url(None) is None


# ── xzibit_bundles: ConnectorBundles.get_url ─────────────────────────────


class TestConnectorBundlesGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_bundles_mod.ConnectorBundles)

    def test_valid_project_key(self):
        url = self.conn.get_url("MY_PROJECT")
        assert "MY_PROJECT" in url
        assert "bundles-design" in url
        assert url.endswith("/")

    def test_none_project_key_returns_none(self):
        assert self.conn.get_url(None) is None


# ── xzibit_codeenvs: ConnectorCodeEnvs.get_url ───────────────────────────


class TestConnectorCodeEnvsGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_codeenvs_mod.ConnectorCodeEnvs)

    def test_valid_python_env(self):
        url = self.conn.get_url("my_env", "python")
        assert "my_env" in url
        assert "python" in url
        assert url.endswith("/")

    def test_valid_r_env(self):
        url = self.conn.get_url("r_env", "R")
        assert "r" in url

    def test_none_env_name_returns_none(self):
        assert self.conn.get_url(None, "python") is None


# ── xzibit_apiservices: ConnectorAPIServices.get_url ─────────────────────


_apiservices_mod = load_connector("xzibit_apiservices")


class TestConnectorAPIServicesGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_apiservices_mod.ConnectorAPIServices)

    def test_valid_inputs(self):
        url = self.conn.get_url("my_service", "MYPROJECT")
        assert "MYPROJECT" in url
        assert "my_service" in url
        assert "api-designer" in url

    def test_none_id_returns_none(self):
        assert self.conn.get_url(None, "PROJ") is None

    def test_none_project_key_returns_none(self):
        assert self.conn.get_url("svc", None) is None


# ── xzibit_deployments: ConnectorDeployments.get_url ─────────────────────


class TestConnectorDeploymentsGetUrl:
    def setup_method(self):
        self.conn = _make_connector(_deployments_mod.ConnectorDeployments)

    def test_valid_inputs(self):
        url = self.conn.get_url("bundle1", "MY_PROJECT")
        assert "MY_PROJECT" in url
        assert "bundle1" in url
        assert "project-deployer" in url
        assert url.endswith("/")

    def test_none_bundle_id_returns_none(self):
        assert self.conn.get_url(None, "MY_PROJECT") is None

    def test_none_project_key_returns_none(self):
        assert self.conn.get_url("bundle1", None) is None

    def test_none_baseurl_returns_none(self):
        conn = _make_connector(_deployments_mod.ConnectorDeployments, base_url=None)
        assert conn.get_url("bundle1", "MY_PROJECT") is None


# ── XzibitBaseConnector: shared partition/count interface ─────────────────


class TestXzibitBaseConnectorMethods:
    """XzibitBaseConnector provides no-op implementations inherited by all connectors."""

    def setup_method(self):
        # Use ConnectorApps as a representative; any connector would work.
        self.conn = _make_connector(_apps_mod.ConnectorApps)

    def test_get_records_count_returns_none(self):
        assert self.conn.get_records_count() is None

    def test_get_records_count_with_args_returns_none(self):
        assert self.conn.get_records_count(partitioning="x", partition_id="y") is None

    def test_list_partitions_returns_empty_list(self):
        assert self.conn.list_partitions(None) == []

    def test_get_partitioning_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            self.conn.get_partitioning()

    def test_partition_exists_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            self.conn.partition_exists(None, None)
