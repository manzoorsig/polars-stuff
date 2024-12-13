
from sqlalchemy import create_engine
from datetime import datetime
import polars as pl
import os

from util.readbq import read_scans_from_bq


def get_connection_string():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    return f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"





def write_customers_to_dw(bqdf):
    result = bqdf.select(
                 pl.col("id").alias("organization_id"),
                 pl.col("name").str.slice(0, length=128).alias("customer_name"),
                 pl.col("tenant_id").str.slice(0, length=4).alias("tenant_short_id"),
                 pl.col("state").str.slice(0, length=30).alias("state"),
                 pl.col("created_at"),
                 pl.col("updated_at")
#                pl.col("created_at").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").alias("created_at"),
#                pl.col("updated_at").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").alias("updated_at")
    )

    result = result.with_columns([
        pl.lit(0).alias("is_deleted"),
        pl.lit(datetime.now()).alias("create_datetime"),
        pl.lit(datetime.now()).alias("update_datetime")
    ])

    mysql_uri = get_connection_string()
    engine = create_engine(mysql_uri)

    result.write_database(
          table_name="CUSTOMER_DIMENSION",
          connection=engine,
          if_table_exists="append"
    )

    records = pl.read_database("SELECT count(*) as count FROM CUSTOMER_DIMENSION", connection=engine)

    print(f"number of records written to db: {records['count'][0]}")





def write_applications_with_branches_to_dw(bqdf):

    result = bqdf.select(
                 pl.col("organization_id"),
                 pl.col("application_id"),
                 pl.col("application_name").str.slice(0, length=256).alias("application_name"),
                 pl.col("application_description").str.slice(0, length=2048).alias("application_description"),
                 pl.col("application_created_at"),
                 pl.col("application_updated_at"),
                 pl.col("application_in_trash"),
                 pl.col("project_id"),
                 pl.col("project_name").str.slice(0, length=256).alias("project_name"),
                 pl.col("project_description").str.slice(0, length=2048).alias("project_description"),
                 pl.col("project_created_at"),
                 pl.col("project_updated_at"),
                 pl.col("project_in_trash"),
                 pl.col("project_state"),
                 pl.col("project_entry_point_url"),
                 pl.col("branch_id"),
                 pl.col("branch_name").str.slice(0, length=256).alias("branch_name"),
                 pl.col("branch_description").str.slice(0, length=256).alias("branch_description"),
                 pl.col("is_default"),
                 pl.col("branch_in_trash"),
                 pl.col("branch_created_at"),
                 pl.col("branch_updated_at"),
#                pl.col("created_at").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").alias("created_at"),
#                pl.col("updated_at").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").alias("updated_at")
    )
#    .drop_nulls(["application_id","project_id","branch_id"])
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

    mysql_uri = get_connection_string()
    engine = create_engine(mysql_uri)

    result.write_database(
          table_name="APPLICATION_WITH_BRANCHES_DIMENSION",
          connection=engine,
          if_table_exists="append"

    )  

    print(f"number of records in result: {result.shape[0]}")

    records = pl.read_database("SELECT count(*) as count FROM APPLICATION_WITH_BRANCHES_DIMENSION", connection=engine)

    print(f"number of records in db: {records['count'][0]}")








def write_applications_without_branches_to_dw(bqdf):

    result = bqdf.select(
                 pl.col("organization_id"),
                 pl.col("application_id"),
                 pl.col("application_name").str.slice(0, length=256).alias("application_name"),
                 pl.col("application_description").str.slice(0, length=2048).alias("application_description"),
                 pl.col("application_created_at"),
                 pl.col("application_updated_at"),
                 pl.col("application_in_trash"),
                 pl.col("project_id"),
                 pl.col("project_name").str.slice(0, length=256).alias("project_name"),
                 pl.col("project_description").str.slice(0, length=2048).alias("project_description"),
                 pl.col("project_created_at"),
                 pl.col("project_updated_at"),
                 pl.col("project_in_trash"),
                 pl.col("project_state").str.slice(0, length=255).alias("project_state"),
                 pl.col("project_entry_point_url").str.slice(0, length=255).alias("project_entry_point_url"),
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

    mysql_uri = get_connection_string()
    engine = create_engine(mysql_uri)

    result.write_database(
          table_name="APPLICATION_DIMENSION",
          connection=engine,
          if_table_exists="append"

    )  

    print(f"number of records in result: {result.shape[0]}")

    records = pl.read_database("SELECT count(*) as count FROM APPLICATION_DIMENSION", connection=engine)

    print(f"number of records in db: {records['count'][0]}")








def read_customer_dim_from_dw():

    mysql_uri = get_connection_string()
    engine = create_engine(mysql_uri)

    customers = pl.read_database("SELECT * FROM CUSTOMER_DIMENSION", connection=engine)

    print(f"number of customers in dw: {customers.shape[0]}")

    return customers


def read_application_dim_from_dw():
    mysql_uri = get_connection_string()
    engine = create_engine(mysql_uri)
    applications = pl.read_database("SELECT * FROM APPLICATION_DIMENSION", connection=engine)
    print(f"number of applications in dw: {applications.shape[0]}")
    return applications



def write_scans_to_dw():
    customers = read_customer_dim_from_dw()
    print(f"customer columns: {customers.columns}")
    applications = read_application_dim_from_dw()
    print(f"application columns: {applications.columns}")
    scans = read_scans_from_bq()
    print(f"scans columns: {scans.columns}")
     
    result = scans.join(customers,  on="ORGANIZATION_ID", how="inner").join(applications, on=["ORGANIZATION_ID","APPLICATION_ID","PROJECT_ID"], how="inner")
    print(f"result columns: {result.columns}")
    print(result.head(5))
    print(result.shape[0])

    scans_to_write = map_scans_to_scans_fact(result)
    print(f"scans_to_write columns: {scans_to_write.columns}")
    print(f"writing scans to dw: {scans_to_write.shape[0]}")

    mysql_uri = get_connection_string()
    engine = create_engine(mysql_uri)

    scans_to_write.write_database(
          table_name="SCAN_FACT",
          connection=engine,
          if_table_exists="append"
    )  


def map_scans_to_scans_fact(scans):
     
    result = scans.select(
                    pl.col("TEST_ID"),
                    pl.col("ID").alias("CUSTOMER_DIM_ID"),
                    pl.col("ID_right").alias("APPLICATION_DIM_ID"),
                    pl.col("ORGANIZATION_ID"),
                    pl.col("APPLICATION_ID"),
                    pl.col("PROJECT_ID"),
                    pl.col("BRANCH_ID"),
                    pl.col("ENTITLEMENT_ID"),
                    pl.col("SUBSCRIPTION_ID"),
                    pl.col("CATALOG_ID"),
                    pl.col("TEST_SHORT_ID").str.slice(0,length=7).alias("TEST_SHORT_ID"),
                    pl.col("STREAM_ID"),
                    pl.col("SCAN_ID"),
                    pl.col("SCAN_MODE"),
                    pl.col("TEST_MODE"),
                    pl.col("TOOL_NAME"),
                    pl.col("ASSESSMENT_TYPE"),
                    pl.col("WORKFLOW_TYPE"),
                    pl.col("SCAN_STATE"),
                    pl.col("TRIAGE"),
                    pl.col("IS_DEFAULT_BRANCH"),
                    pl.col("IS_DELETED"),
                    pl.col("START_DATE"),
                    pl.col("CREATED_AT"),
                    pl.col("UPDATED_AT")

              )
     
    result = result.with_columns([
        pl.lit(datetime.now()).alias("CREATE_DATETIME"),
        pl.lit(datetime.now()).alias("UPDATE_DATETIME")
    ])

#     Drop columns that are not needed in the database
#     result = result.drop(['ID', 'CUSTOMER_NAME', 'TENANT_SHORT_ID', 
#                           'STATE', 'IS_DELETED', 'CREATED_AT', 'UPDATED_AT', 
#                            'CREATE_DATETIME', 'UPDATE_DATETIME', 
#                           'ID_right', 'APPLICATION_NAME', 
#                           'APPLICATION_DESCRIPTION', 'IS_APPLICATION_DELETED', 'APPLICATION_CREATED_AT',
#                            'APPLICATION_UPDATED_AT', 'PROJECT_NAME', 'PROJECT_DESCRIPTION', 'IS_PROJECT_DELETED',
#                            'PROJECT_CREATED_AT', 'PROJECT_UPDATED_AT', 'PROJECT_STATE', 'PROJECT_ENTRY_POINT_URL',
#                            'CREATE_DATETIME_right', 'UPDATE_DATETIME_right'])
    print("after mapping columns")
    print(result.head(5))
    return result

