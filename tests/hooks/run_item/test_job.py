import pytest
from airflow.providers.microsoft.fabric.operators.run_item.job import MSFabricRunJobOperator


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
