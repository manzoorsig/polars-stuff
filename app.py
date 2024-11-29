import polars as pl



def main():


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
