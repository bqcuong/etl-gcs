from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd

@task(log_prints=True, retries=3)
def fetch_data(dataset_url: str) -> pd.DataFrame:
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def write_data_gcs(data_path: Path):
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-taxi")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=data_path, to_path=data_path)

@flow(name="Data Ingestion for GCS with remote flow code on Github")
def main_flow(color: str = "green", year: int = 2020, months: list = [11]):
    db = "ny_taxi"
    
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        print("---> ", dataset_url)

        raw_data = fetch_data(dataset_url)
        data = clean_data(raw_data)
        data_path = write_local(data, color, dataset_file)
        write_data_gcs(data_path)

if __name__ == '__main__':
    main_flow()