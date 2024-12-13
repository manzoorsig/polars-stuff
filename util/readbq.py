from google.cloud import bigquery
import polars as pl





def read_customers_from_bq():
    client = bigquery.Client()
    # Perform a query.
    q = """SELECT id, name, state, tenant_id, created_at, updated_at 
           FROM `gcp-sig-datalake-staging.stg_sink_data.tenant_service_tenant`  """
    QUERY = (q)
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    print(df)
    print(f"number of records read: {df.shape[0]}")
    return df


def read_applications_with_branches_from_bq():
    client = bigquery.Client()
    # Perform a query.
    QUERY = (
           """SELECT app.organization_id as organization_id,app.id as application_id, 
                     app.name as application_name,app.description as application_description, 
                     app.created_at as application_created_at,app.updated_at as application_updated_at,
                     app.in_trash as application_in_trash, 
                     proj.id as project_id, proj.name as project_name, proj.description as project_description, 
                     proj.created_at as project_created_at,proj.updated_at as project_updated_at, proj.in_trash as project_in_trash,
                     proj.state as project_state, proj.entry_point_url as project_entry_point_url , -- proj.entry_point_private ,proj.proxy_type,
                     br.id as branch_id, br.name as branch_name, br.description as branch_description,br.is_default as is_default,
                     br.in_trash as branch_in_trash,
                     br.created_at as branch_created_at, br.updated_at as branch_updated_at
            FROM `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
            JOIN `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
            JOIN `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_branch` as br on proj.organization_id = br.organization_id and proj.id = br.project_id
            --ORDER BY app.id
            --LIMIT 100"""
    )
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    print(df)
    print(f"number of records read: {df.shape[0]}")

    return df




def read_applications_without_branches_from_bq():
    client = bigquery.Client()
    # Perform a query.
    QUERY = (
           """SELECT app.organization_id as organization_id,app.id as application_id, 
                     app.name as application_name,app.description as application_description,
                     app.created_at as application_created_at,app.updated_at as application_updated_at,
                     app.in_trash as application_in_trash,
                     proj.id as project_id, proj.name as project_name, proj.description as project_description, 
                     proj.created_at as project_created_at,proj.updated_at as project_updated_at, proj.in_trash as project_in_trash,
                     proj.state as project_state, proj.entry_point_url as project_entry_point_url , -- proj.entry_point_private ,proj.proxy_type,                   
            FROM `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
            JOIN `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
            --LIMIT 60000"""
    )
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    print(df)
    print(f"number of records read: {df.shape[0]}")

    return df


def read_scans_from_bq():
    client = bigquery.Client()
    # Perform a query.
    QUERY = (
           """SELECT
                    ID as TEST_ID,
                    tenant_id as ORGANIZATION_ID,
                    APPLICATION_ID,
                    PROJECT_ID,
                    BRANCH_ID,
                    ENTITLEMENT_ID,
                    SUBSCRIPTION_ID,
                    CATALOG_ID,
                    TEST_SHORT_ID,
                    STREAM_ID,
                    SCAN_ID,
                    SCAN_MODE,
                    TEST_MODE,
                    TOOL as TOOL_NAME,
                    ASSESSMENT_TYPE,
                    WORKFLOW_TYPE,
                    state as SCAN_STATE,
                    TRIAGE,
                    IS_DEFAULT_BRANCH,
                    IS_DELETED,
                    START_DATE,
                    CREATED_DATE,
                    UPDATED_DATE
            FROM `gcp-sig-datalake-staging.stg_sink_data.test_manager_test` 
            WHERE STREAM_ID IS NOT NULL AND SCAN_ID IS NOT NULL AND TOOL IS NOT NULL
            AND ( STATE = 'COMPLETED' OR STATE = 'FAILED')
            --LIMIT 40000"""
    )
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    df = pl.from_arrow(rows.to_arrow())
    print(df)
    print(f"number of records read: {df.shape[0]}")
    return df
