import uuid
from datetime import datetime

# from typing import TYPE_CHECKING
from pyspark.dbutils import DBUtils

# if TYPE_CHECKING:
from pyspark.sql import SparkSession

from dataDP.decorators.loggers import with_logging_and_spark
from dataDP.utils.logger import logger

def get_job_context(spark: SparkSession) -> dict:
    """Get all available job context information"""
    context = {}
    
    try:
        context['job_run_id'] = spark.conf.get("spark.databricks.job.runId")
    except:
        context['job_run_id'] = None
    
    try:
        context['job_id'] = spark.conf.get("spark.databricks.job.id")
    except:
        context['job_id'] = None
    
    try:
        context['task_key'] = spark.conf.get("spark.databricks.job.taskKey")
    except:
        context['task_key'] = None
    
    try:
        context['parent_run_id'] = spark.conf.get("spark.databricks.job.parentRunId")
    except:
        context['parent_run_id'] = None
    
    return context

@with_logging_and_spark
def get_execution_id(spark: SparkSession) -> str:
    """Get execution ID from multiple sources with fallbacks"""
    dbutils = DBUtils(spark)

    try:
        # Try to get from context
        # Note:
        #   - This only works if the notebook is run as a job, not in interactive mode
        #   - Not working on Serverless SQL endpoints, as they don't have the same job context
        #   - Not working on Unity Catalog enabled workspaces, as the API has different access patterns
        logger.info("Trying to get run_id from notebook context...")
        run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
    except Exception:
        pass

    try:
        # Get from job parameter widget
        logger.info("Trying to get run_id from job parameter widget...")
        run_id = dbutils.widgets.get("job.run_id")
        if run_id and run_id != "default_run_id":
            return run_id
    except Exception:
        pass

    # Get from Spark context
    try:
        logger.info("Trying to get run_id from Spark config...")
        run_id = get_job_context(spark).get('job_run_id')
    except Exception:
        pass

    # Get from job ID + timestamp
    try:
        logger.info("Trying to get run_id from job ID + timestamp...")
        job_id = spark.conf.get("spark.databricks.job.id")
        return f"{job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    except Exception:
        pass

    # Fallback: Generate UUID
    logger.info("Trying to generate UUID...")
    return str(uuid.uuid4())
