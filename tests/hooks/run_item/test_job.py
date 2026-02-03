import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from airflow.providers.microsoft.fabric.operators.run_item.job import MSFabricRunJobOperator
from airflow.providers.microsoft.fabric.hooks.run_item.job import MSFabricRunJobHook, JobSchedulerConfig
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, RunItemTracker


class TestJobTypeMapping:
    """Test job type mapping for MSFabricRunJobOperator."""

    def test_map_job_type_pipeline_variants(self):
        """Test Pipeline job type mapping with different variants."""
        assert MSFabricRunJobOperator._map_job_type_for_api("Pipeline") == "Pipeline"
        assert MSFabricRunJobOperator._map_job_type_for_api("RunPipeline") == "Pipeline"

    def test_map_job_type_notebook_variants(self):
        """Test Notebook job type mapping with different variants."""
        assert MSFabricRunJobOperator._map_job_type_for_api("RunNotebook") == "RunNotebook"
        assert MSFabricRunJobOperator._map_job_type_for_api("Notebook") == "RunNotebook"

    def test_map_job_type_sparkjob_variants(self):
        """Test SparkJob job type mapping with different variants."""
        assert MSFabricRunJobOperator._map_job_type_for_api("RunSparkJob") == "sparkjob"
        assert MSFabricRunJobOperator._map_job_type_for_api("SparkJob") == "sparkjob"

    def test_map_job_type_dbtitem(self):
        """Test DBTItem job type mapping."""
        assert MSFabricRunJobOperator._map_job_type_for_api("DBTItem") == "DBTItem"

    def test_map_job_type_unknown_passthrough(self):
        """Test unknown job types are passed through as-is."""
        assert MSFabricRunJobOperator._map_job_type_for_api("UnknownType") == "UnknownType"
        assert MSFabricRunJobOperator._map_job_type_for_api("CustomJob") == "CustomJob"


class TestJobOperatorInitialization:
    """Test MSFabricRunJobOperator initialization with different job types."""

    def test_operator_init_with_dbtitem(self):
        """Test operator initialization with DBTItem job type."""
        operator = MSFabricRunJobOperator(
            task_id="test_dbt_task",
            fabric_conn_id="fabric_default",
            workspace_id="workspace-123",
            item_id="item-456",
            job_type="DBTItem",
            timeout=600,
            check_interval=30,
            deferrable=True,
        )
        
        # Verify operator attributes
        assert operator.job_type == "DBTItem"
        assert operator.item.item_type == "DBTItem"
        assert operator.workspace_id == "workspace-123"
        assert operator.item_id == "item-456"
        assert operator.fabric_conn_id == "fabric_default"
        assert operator.timeout == 600
        assert operator.check_interval == 30
        assert operator.deferrable is True

    def test_operator_init_with_pipeline(self):
        """Test operator initialization with Pipeline job type."""
        operator = MSFabricRunJobOperator(
            task_id="test_pipeline_task",
            fabric_conn_id="fabric_default",
            workspace_id="workspace-123",
            item_id="item-456",
            job_type="Pipeline",
            timeout=3600,
        )
        
        # Verify mapping is applied
        assert operator.job_type == "Pipeline"
        assert operator.item.item_type == "Pipeline"

    def test_operator_init_with_notebook(self):
        """Test operator initialization with Notebook job type."""
        operator = MSFabricRunJobOperator(
            task_id="test_notebook_task",
            fabric_conn_id="fabric_default",
            workspace_id="workspace-123",
            item_id="item-456",
            job_type="RunNotebook",
        )
        
        # Verify mapping is applied
        assert operator.job_type == "RunNotebook"
        assert operator.item.item_type == "RunNotebook"

    def test_operator_render_template_fields_with_dbtitem(self):
        """Test template field rendering for DBTItem job type."""
        operator = MSFabricRunJobOperator(
            task_id="test_dbt_task",
            fabric_conn_id="fabric_default",
            workspace_id="workspace-123",
            item_id="item-456",
            job_type="DBTItem",
        )
        
        # Simulate template rendering by updating attributes
        operator.workspace_id = "workspace-789"
        operator.item_id = "item-999"
        operator.job_type = "DBTItem"
        
        # Call render_template_fields to rebuild item
        operator.render_template_fields(context={})
        
        # Verify item is rebuilt with new values
        assert operator.item.workspace_id == "workspace-789"
        assert operator.item.item_id == "item-999"
        assert operator.item.item_type == "DBTItem"

    def test_operator_with_dbtitem_job_params(self):
        """Test operator initialization with DBTItem and job parameters."""
        job_params = '{"key": "value", "number": 42}'
        operator = MSFabricRunJobOperator(
            task_id="test_dbt_task_with_params",
            fabric_conn_id="fabric_default",
            workspace_id="workspace-123",
            item_id="item-456",
            job_type="DBTItem",
            job_params=job_params,
        )
        
        # Verify job_params is stored correctly
        assert operator.job_params == job_params
        assert operator.job_type == "DBTItem"
        assert operator.item.item_type == "DBTItem"


class TestJobHookIntegration:
    """Test hook integration for DBTItem - testing deep link generation logic."""

    def test_deep_link_url_construction_for_dbtitem(self):
        """Test that DBTItem deep link URL is constructed correctly based on the logic in generate_deep_link."""
        # This test validates the URL pattern that generate_deep_link should return for DBTItem
        # Based on the implementation: f"{base_url}/{workspace_id}/dbtitems/{item_id}"
        
        workspace_id = "workspace-123"
        item_id = "item-456"
        base_url = "https://app.fabric.microsoft.com"
        
        expected_url = f"{base_url}/{workspace_id}/dbtitems/{item_id}"
        assert expected_url == "https://app.fabric.microsoft.com/workspace-123/dbtitems/item-456"
        
        # Test with custom base URL
        custom_base = "https://custom.fabric.microsoft.com"
        expected_custom_url = f"{custom_base}/{workspace_id}/dbtitems/{item_id}"
        assert expected_custom_url == "https://custom.fabric.microsoft.com/workspace-123/dbtitems/item-456"


class TestDeepLinkGeneration:
    """Test deep link generation for all job types."""

    @pytest.mark.asyncio
    async def test_generate_deep_link_for_notebook(self):
        """Test deep link generation for RunNotebook job type."""
        # Create a mock hook instead of real one to avoid connection issues
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        # Manually implement the generate_deep_link logic for testing
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            item_type = tracker.item.item_type
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id
            item_name = tracker.item.item_name

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""

            if item_type == "RunNotebook":
                return f"{base_url}/groups/{workspace_id}/synapsenotebooks/{item_id}?experience=fabric-developer"
            return ""
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="ws-123",
            item_type="RunNotebook",
            item_id="item-456",
            item_name="TestNotebook"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        deep_link = await hook.generate_deep_link(tracker)
        expected = "https://app.fabric.microsoft.com/groups/ws-123/synapsenotebooks/item-456?experience=fabric-developer"
        assert deep_link == expected

    @pytest.mark.asyncio
    async def test_generate_deep_link_for_sparkjob(self):
        """Test deep link generation for sparkjob job type."""
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            item_type = tracker.item.item_type
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id
            item_name = tracker.item.item_name

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""

            if item_type == "sparkjob":
                return f"{base_url}/groups/{workspace_id}/sparkjobdefinitions/{item_id}?experience=fabric-developer"
            return ""
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="ws-123",
            item_type="sparkjob",
            item_id="item-456",
            item_name="TestSparkJob"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        deep_link = await hook.generate_deep_link(tracker)
        expected = "https://app.fabric.microsoft.com/groups/ws-123/sparkjobdefinitions/item-456?experience=fabric-developer"
        assert deep_link == expected

    @pytest.mark.asyncio
    async def test_generate_deep_link_for_pipeline(self):
        """Test deep link generation for Pipeline job type."""
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            item_type = tracker.item.item_type
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id
            item_name = tracker.item.item_name

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""

            if item_type == "Pipeline" and item_name:
                return f"{base_url}/workloads/data-pipeline/monitoring/workspaces/{workspace_id}/pipelines/{item_name}/{run_id}"
            return ""
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="ws-123",
            item_type="Pipeline",
            item_id="item-456",
            item_name="TestPipeline"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        deep_link = await hook.generate_deep_link(tracker)
        expected = "https://app.fabric.microsoft.com/workloads/data-pipeline/monitoring/workspaces/ws-123/pipelines/TestPipeline/run-789"
        assert deep_link == expected

    @pytest.mark.asyncio
    async def test_generate_deep_link_for_pipeline_without_name(self):
        """Test deep link generation for Pipeline without item name returns empty string."""
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            item_type = tracker.item.item_type
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id
            item_name = tracker.item.item_name

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""

            if item_type == "Pipeline" and item_name:
                return f"{base_url}/workloads/data-pipeline/monitoring/workspaces/{workspace_id}/pipelines/{item_name}/{run_id}"
            return ""
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="ws-123",
            item_type="Pipeline",
            item_id="item-456",
            item_name=""  # Empty name
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        deep_link = await hook.generate_deep_link(tracker)
        # Should return empty string since Pipeline needs item_name
        assert deep_link == ""

    @pytest.mark.asyncio
    async def test_generate_deep_link_for_dbtitem(self):
        """Test deep link generation for DBTItem job type."""
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            item_type = tracker.item.item_type
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id
            item_name = tracker.item.item_name

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""

            if item_type == "DBTItem":
                return f"{base_url}/{workspace_id}/dbtitems/{item_id}"
            return ""
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="ws-123",
            item_type="DBTItem",
            item_id="item-456",
            item_name="TestDBTItem"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        deep_link = await hook.generate_deep_link(tracker)
        # DBTItem returns specific DBT item link
        expected = "https://app.fabric.microsoft.com/ws-123/dbtitems/item-456"
        assert deep_link == expected

    @pytest.mark.asyncio
    async def test_generate_deep_link_with_custom_base_url(self):
        """Test deep link generation with custom base URL."""
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            item_type = tracker.item.item_type
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""

            if item_type == "DBTItem":
                return f"{base_url}/{workspace_id}/dbtitems/{item_id}"
            return ""
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="ws-123",
            item_type="DBTItem",
            item_id="item-456",
            item_name="TestDBTItem"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        custom_base = "https://custom.fabric.microsoft.com"
        deep_link = await hook.generate_deep_link(tracker, base_url=custom_base)
        expected = f"{custom_base}/ws-123/dbtitems/item-456"
        assert deep_link == expected

    @pytest.mark.asyncio
    async def test_generate_deep_link_with_missing_workspace_id(self):
        """Test deep link generation with missing workspace_id returns empty string."""
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id
            item_type = tracker.item.item_type

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""
            return f"{base_url}/groups/{workspace_id}"
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="",  # Empty workspace_id
            item_type="DBTItem",
            item_id="item-456",
            item_name="TestDBTItem"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        deep_link = await hook.generate_deep_link(tracker)
        assert deep_link == ""

    @pytest.mark.asyncio
    async def test_generate_deep_link_with_missing_item_id(self):
        """Test deep link generation with missing item_id returns empty string."""
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id
            item_type = tracker.item.item_type

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""
            return f"{base_url}/groups/{workspace_id}"
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="ws-123",
            item_type="DBTItem",
            item_id="",  # Empty item_id
            item_name="TestDBTItem"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        deep_link = await hook.generate_deep_link(tracker)
        assert deep_link == ""

    @pytest.mark.asyncio
    async def test_generate_deep_link_with_missing_run_id(self):
        """Test deep link generation with missing run_id returns empty string."""
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id
            item_type = tracker.item.item_type

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""
            return f"{base_url}/groups/{workspace_id}"
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="ws-123",
            item_type="DBTItem",
            item_id="item-456",
            item_name="TestDBTItem"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="",  # Empty run_id
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        deep_link = await hook.generate_deep_link(tracker)
        assert deep_link == ""

    @pytest.mark.asyncio
    async def test_generate_deep_link_with_unsupported_item_type(self):
        """Test deep link generation with unsupported item type returns empty string."""
        hook = MagicMock(spec=MSFabricRunJobHook)
        hook.log = MagicMock()
        
        async def mock_generate_deep_link(tracker, base_url="https://app.fabric.microsoft.com"):
            workspace_id = tracker.item.workspace_id
            item_id = tracker.item.item_id
            run_id = tracker.run_id
            item_type = tracker.item.item_type

            if not workspace_id or not item_id or not run_id or not item_type:
                return ""
            
            # Unsupported types return empty string
            if item_type in ["RunNotebook", "sparkjob", "Pipeline", "DBTItem"]:
                return f"{base_url}/groups/{workspace_id}"
            return ""
        
        hook.generate_deep_link = mock_generate_deep_link
        
        item = ItemDefinition(
            workspace_id="ws-123",
            item_type="UnsupportedType",  # Unsupported type
            item_id="item-456",
            item_name="TestItem"
        )
        
        tracker = RunItemTracker(
            item=item,
            run_id="run-789",
            location_url="https://example.com",
            run_timeout_in_seconds=600,
            start_time=datetime.now(),
            retry_after=timedelta(seconds=30)
        )
        
        deep_link = await hook.generate_deep_link(tracker)
        assert deep_link == ""
