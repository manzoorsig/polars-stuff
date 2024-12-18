from sqlalchemy import create_engine
import polars as pl
import os


def main():
    db_user = os.getenv("SDB_USER")
    db_password = os.getenv("SDB_PASSWORD")
    db_host = os.getenv("SDB_HOST")
    db_port = os.getenv("SDB_PORT")
    db_name = "information_schema"
    mysql_uri = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    engine = create_engine(mysql_uri)
    applications = pl.read_database("SELECT @@VERSION", connection=engine)
    print(f"version of db: {applications.shape[0]}")
    return applications


if __name__ == "__main__":
    main()
