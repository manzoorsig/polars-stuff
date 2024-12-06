import polars as pl
import google.auth
import os
from google.cloud import bigquery
from sqlalchemy import create_engine
from datetime import datetime


def main():
 credentials, project = google.auth.default()
 print(f"gcp project: {project}")
 #df = read_customers_from_bq()
 #write_customers_to_singlestore(df)
 read_applications_from_bq()



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

 


def write_customers_to_singlestore(bqdf):
   
    

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


def read_applications_from_bq():
    
    client = bigquery.Client()

    # Perform a query.
    QUERY = (
           """SELECT app.id, app.name,app.description, app.created_at, app.in_trash, 
                     proj.id, proj.application_id, proj.name, proj.description, 
                     proj.created_at, proj.in_trash , 
                     br.id, br.name, br.description,br.in_trash, br.created_at
            FROM `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
            left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
            left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_branch` as br on app.id = br.organization_id and proj.id = br.project_id
            ORDER BY app.id
            LIMIT 100"""
  
    )
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    print(df)
    print(f"number of records read: {df.shape[0]}")

    return df


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
    



if __name__ == "__main__":
    main()
