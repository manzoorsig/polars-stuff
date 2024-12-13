
import google.auth
from util.readbq import read_applications_without_branches_from_bq, read_customers_from_bq
from util.writedw import write_applications_without_branches_to_dw, write_customers_to_dw, write_scans_to_dw
import timeit

def main():
    credentials, project = google.auth.default()
    print(f"gcp project: {project}")
    df = read_customers_from_bq()
    write_customers_to_dw(df)
    df1 = read_applications_without_branches_from_bq()
    write_applications_without_branches_to_dw(df1)
    write_scans_to_dw()
    print("done")


if __name__ == "__main__":
    # Benchmark the main function
    execution_time = timeit.timeit("main()", setup="from __main__ import main", number=1)
    print(f"Execution time: {execution_time} seconds")
