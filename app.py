import polars as pl
import google.auth
from google.cloud import bigquery


def main():



 credentials, project = google.auth.default()
 print(credentials)
 print(project)
 read_from_bq()
   



def read_from_bq():
    
    client = bigquery.Client()

    # Perform a query.
    QUERY = (
            'SELECT * FROM `gcp-sig-datalake-staging.stg_sink_data.test_manager_test`  '
   #         'WHERE state = "TX" '
            'LIMIT 100')
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    print(df)
    
    
    # Read a DataFrame from BigQuery
    #df = pl.read_bigquery(
    #    project_id="your_project_id",
    #    table="your_table",
    #    location="US",
    #    credentials="path/to/credentials.json",
    #)

    # Do some computation
    #result = df.groupby("column_name").agg(pl.sum("column_name"))

    # Write the result back to BigQuery
    #result.write_bigquery(
    #    project_id="your_project_id",
    #    table="your_table",
    #    location="US",
    #    credentials="path/to/credentials.json",
    #)




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
