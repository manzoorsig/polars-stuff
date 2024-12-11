import polars as pl
import google.auth
import os
from google.cloud import bigquery
from sqlalchemy import create_engine
from datetime import datetime






def read_customers_from_bq():
    
    client = bigquery.Client()

    # Perform a query.
    QUERY = (
            'SELECT id, name, state, tenant_id, created_at, updated_at FROM `gcp-sig-datalake-staging.stg_sink_data.tenant_service_tenant`  '
  
    )
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    print(df)
    print(f"number of records read: {df.shape[0]}")

    return df

 


def write_customers_to_dw(bqdf):
   
    

    result = bqdf.select(
                 pl.col("id").alias("organization_id"),
                 pl.col("name").str.slice(0,length=128).alias("customer_name"),
                 pl.col("tenant_id").str.slice(0,length=4).alias("tenant_short_id"),
                 pl.col("state").str.slice(0,length=30).alias("state"),
                 pl.col("created_at"),
                 pl.col("updated_at")
                 #pl.col("created_at").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").alias("created_at"),
                 #pl.col("updated_at").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").alias("updated_at")
    )

    result = result.with_columns([
        pl.lit(0).alias("is_deleted"),
        pl.lit(datetime.now()).alias("create_datetime"),
        pl.lit(datetime.now()).alias("update_datetime")
    ])

     # Read database credentials from environment variables
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")

    mysql_uri = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"
    engine = create_engine(mysql_uri)

    result.write_database(
          table_name="CUSTOMER_DIMENSION",
          connection=engine,
          if_table_exists="append"

    )  

    records = pl.read_database("SELECT count(*) as count FROM CUSTOMER_DIMENSION", connection=engine)

    print(f"number of records written to db: {records['count'][0]}")


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
            LIMIT 100"""
  
    )
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    print(df)
    print(f"number of records read: {df.shape[0]}")

    return df



def write_applications_with_branches_to_dw(bqdf):

    result = bqdf.select(
                 pl.col("organization_id"),
                 pl.col("application_id"),
                 pl.col("application_name").str.slice(0,length=256).alias("application_name"),
                 pl.col("application_description").str.slice(0,length=2048).alias("application_description"),
                 pl.col("application_created_at"),
                 pl.col("application_updated_at"),
                 pl.col("application_in_trash"),
                 pl.col("project_id"),
                 pl.col("project_name").str.slice(0,length=256).alias("project_name"),
                 pl.col("project_description").str.slice(0,length=2048).alias("project_description"),
                 pl.col("project_created_at"),
                 pl.col("project_updated_at"),
                 pl.col("project_in_trash"),
                 pl.col("project_state"),
                 pl.col("project_entry_point_url"),
                 pl.col("branch_id"),
                 pl.col("branch_name").str.slice(0,length=256).alias("branch_name"),
                 pl.col("branch_description").str.slice(0,length=256).alias("branch_description"),
                 pl.col("is_default"),
                 pl.col("branch_in_trash"),
                 pl.col("branch_created_at"),
                 pl.col("branch_updated_at"),
                 #pl.col("created_at").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").alias("created_at"),
                 #pl.col("updated_at").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").alias("updated_at")
    )
    #.drop_nulls(["application_id","project_id","branch_id"])
    print(result.columns)
    print(result.head(5))
    result = result.with_columns([
        pl.when(pl.col("is_default") == True).then(1).otherwise(0).alias("branch_is_default"),
        pl.when(pl.col("application_in_trash") == True).then(1).otherwise(0).alias("is_application_deleted"),
        pl.when(pl.col("project_in_trash") == True).then(1).otherwise(0).alias("is_project_deleted"),
        pl.when(pl.col("branch_in_trash") == True).then(1).otherwise(0).alias("is_branch_deleted"),
        pl.lit(datetime.now()).alias("create_datetime"),
        pl.lit(datetime.now()).alias("update_datetime")
    ])

    # Drop columns that are not needed in the database
    result = result.drop(["application_in_trash", "project_in_trash", "branch_in_trash","is_default"])

    print(result.columns)
    print(result.head(5)) 

     # Read database credentials from environment variables
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")

    mysql_uri = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"
    engine = create_engine(mysql_uri)

    result.write_database(
          table_name="APPLICATION_DIMENSION",
          connection=engine,
          if_table_exists="append"

    )  

    print(f"number of records in result: {result.shape[0]}")

    records = pl.read_database("SELECT count(*) as count FROM APPLICATION_DIMENSION", connection=engine)

    print(f"number of records in db: {records['count'][0]}")



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
            LIMIT 60000"""
  
    )
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    print(df)
    print(f"number of records read: {df.shape[0]}")

    return df





def write_applications_without_branches_to_dw(bqdf):

    result = bqdf.select(
                 pl.col("organization_id"),
                 pl.col("application_id"),
                 pl.col("application_name").str.slice(0,length=256).alias("application_name"),
                 pl.col("application_description").str.slice(0,length=2048).alias("application_description"),
                 pl.col("application_created_at"),
                 pl.col("application_updated_at"),
                 pl.col("application_in_trash"),
                 pl.col("project_id"),
                 pl.col("project_name").str.slice(0,length=256).alias("project_name"),
                 pl.col("project_description").str.slice(0,length=2048).alias("project_description"),
                 pl.col("project_created_at"),
                 pl.col("project_updated_at"),
                 pl.col("project_in_trash"),
                 pl.col("project_state"),
                 pl.col("project_entry_point_url"),
                 )
   
    print(result.columns)
    print(result.head(5))

    result = result.with_columns([

        pl.when(pl.col("application_in_trash") == True).then(1).otherwise(0).alias("is_application_deleted"),
        pl.when(pl.col("project_in_trash") == True).then(1).otherwise(0).alias("is_project_deleted"),

        pl.lit(datetime.now()).alias("create_datetime"),
        pl.lit(datetime.now()).alias("update_datetime")
    ])

    # Drop columns that are not needed in the database
    result = result.drop(["application_in_trash", "project_in_trash", ])

    print(result.columns)
    print(result.head(5)) 

     # Read database credentials from environment variables
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")

    mysql_uri = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"
    engine = create_engine(mysql_uri)

    result.write_database(
          table_name="APPLICATION_DIMENSION",
          connection=engine,
          if_table_exists="append"

    )  

    print(f"number of records in result: {result.shape[0]}")

    records = pl.read_database("SELECT count(*) as count FROM APPLICATION_DIMENSION", connection=engine)

    print(f"number of records in db: {records['count'][0]}")



def read_from_csv():
    
    testdf = pl.read_csv(source='./source-data/data-1732902681765.csv',has_header=True, infer_schema=True)
    print(testdf)

    result = testdf.select(
    pl.col("start_date").alias("start_date_str"),
    pl.col("start_date").str.to_datetime("%Y-%m-%d %H:%M:%S%#z"),
    )
   
    print(result)

    testdf2 = testdf.filter(pl.col("state").str.contains("COMPLETED")).select(pl.col("id"),pl.col("state"))

    print(testdf2) 

    testdf3 = testdf.filter(pl.col("id").str.contains("02f6d322-4c4c-483c-a76f-f323fae87cb3"))
    print(testdf3) 
    

def main():
    credentials, project = google.auth.default()
    print(f"gcp project: {project}")
    df = read_customers_from_bq()
    write_customers_to_dw(df)
    df1 = read_applications_without_branches_from_bq()
    write_applications_without_branches_to_dw(df1)

if __name__ == "__main__":
    main()
