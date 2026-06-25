-- ============================================================================
-- Real-Time Events Analytics Pipeline - BigQuery dataset bootstrap
-- ----------------------------------------------------------------------------
-- Creates the analytics dataset. The Spark job also creates this automatically
-- via BigQueryTableManager.ensure_dataset(); this script is provided for manual
-- / IaC provisioning and for reviewers who want the DDL explicitly.
--
-- Replace `my-gcp-project` with your project id (or run through the `bq` CLI
-- which infers the default project):
--
--   bq --location=US query --use_legacy_sql=false < sql/create_dataset.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS `my-gcp-project.rtep_analytics`
OPTIONS (
  location = 'US',
  description = 'Real-Time Events Analytics Pipeline — stock market event stream',
  labels = [('project', 'rtep'), ('managed_by', 'spark-streaming')]
);
