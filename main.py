import logging
from uuid import uuid4
from fastapi import FastAPI, UploadFile, HTTPException
from io import BytesIO
from io import StringIO
import boto3
from psycopg2 import connect
import pandas as pd
import os
import pandas_schema
from pandas_schemas import departments_schema, hired_employees_schema, jobs_schema
from mangum import Mangum
from datetime import date
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("my-api")

app = FastAPI()
handler = Mangum(app)

AWS_BUCKET = "raw-challenge-globant-uploads"
AWS_KEY_ID = os.getenv("KEY_ID")
AWS_SECRET_KEY = os.getenv("SECRET_KEY")
DB_NAME = os.getenv("DB_NAME")
HOST = os.getenv("DB_HOST")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")


class TableNames(str, Enum):
    jobs = "jobs"
    hired_employees = "hired_employees"
    departments = "departments"


def clean_data_format(data: pd.DataFrame, schema: pandas_schema.Schema):
    logger.info("Checking data format")
    data = data.convert_dtypes(infer_objects=True)
    errors = schema.validate(data)
    errors_index_rows = [e.row for e in errors]
    logger.warning("Errors detected and skip it:")
    for error in errors:
        logger.warning(error)
    cleaned_df = data.drop(index=errors_index_rows)
    if cleaned_df.empty:
        raise HTTPException(
            status_code=400,
            detail=f"Data is corrupted or empty!, schema should be: {schema.get_column_names()}",
        )
    return cleaned_df


def check_file_format(file: UploadFile | None = None):
    logger.info("Checking file format to be csv")
    if not file:
        raise HTTPException(status_code=400, detail="No file found!")
    filename = file.filename
    logger.info(f"Processing file: {filename}")
    if not filename.endswith(".csv"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {filename}. Supported type is CSV",
        )


async def s3_remove_file(path_list: list):
    final_paths = [
        path.replace("s3://", "").split("/", maxsplit=1)[-1] for path in path_list
    ]
    try:
        logger.info(f"Something was wrong. Deleting {path_list}")
        s3_client = boto3.client(
            "s3",
            AWS_ACCESS_KEY_ID=AWS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_KEY,
        )

        response = s3_client.delete_objects(
            Bucket=AWS_BUCKET,
            Delete={"Objects": [{"Key": path} for path in final_paths]},
        )
        return response
    except Exception as error:
        raise HTTPException(status_code=400, detail=error)


async def s3_upload(contents: bytes, key: str):
    logger.info(f"Uploading {key} to s3")
    try:
        s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=AWS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_KEY,
        )
        s3_resource.Object(AWS_BUCKET, key).put(Body=contents.getvalue())
        return f"s3://{AWS_BUCKET}/{key}"  # from_path
    except Exception as error:
        raise HTTPException(status_code=400, detail=error)


async def redshift_upload(path_list: list, table_name: str, columns: str):
    logger.info(f"Uploading {path_list} to {table_name} table in Redshift db")
    try:
        conn = connect(
            dbname=DB_NAME,
            host=HOST,
            port="5439",
            user=USER,
            password=PASSWORD,
            connect_timeout=5,
        )
        for path in path_list:
            cursor = conn.cursor()
            query = f"COPY {table_name} {columns} FROM '{path}' CREDENTIALS 'aws_access_key_id={AWS_KEY_ID};aws_secret_access_key={AWS_SECRET_KEY}' IGNOREHEADER 1 TIMEFORMAT 'YYYY-MM-DDTHH:MI:SSZ' CSV;"
            logger.info(
                f"Executing query: COPY {table_name} {columns} FROM '{path} IGNOREHEADER 1 TIMEFORMAT 'YYYY-MM-DDTHH:MI:SSZ' CSV;"
            )
            cursor.execute(query)
            conn.commit()
        cursor.close()
        conn.close()
        return {
            "status_code": 200,
            "message": f"Files {path_list} were uploaded into db correctly!",
        }
    except Exception as err:
        if conn:
            conn.close()
        if cursor:
            cursor.close()
        await s3_remove_file(path_list)
        raise HTTPException(status_code=400, detail=err.pgerror)


@app.get("/")
def root():
    return {"message": "Welcome from file-upload 👋🏻🚀"}


@app.post("/api/v1/upload_departments")
async def upload(file: UploadFile | None = None):
    check_file_format(file)
    contents = await file.read()
    buffer = BytesIO(contents)
    path_list = []
    for i, chunk in enumerate(
        pd.read_csv(buffer, names=["id", "department"], chunksize=1000)
    ):
        cleaned_df = clean_data_format(chunk, departments_schema)
        csv_buffer = StringIO()
        cleaned_df.to_csv(csv_buffer, index=False)
        from_path = await s3_upload(
            contents=csv_buffer, key=f"upload_departments/{uuid4()}_chuncksize{i}.csv"
        )
        path_list.append(from_path)
    logger.info(f"Path list: {path_list}")
    return await redshift_upload(
        path_list=path_list, table_name="departments", columns="(id, department)"
    )


@app.post("/api/v1/upload_jobs")
async def upload(file: UploadFile | None = None):
    check_file_format(file)
    contents = await file.read()
    buffer = BytesIO(contents)
    path_list = []
    for i, chunk in enumerate(pd.read_csv(buffer, names=["id", "job"], chunksize=1000)):
        cleaned_df = clean_data_format(chunk, jobs_schema)
        csv_buffer = StringIO()
        cleaned_df.to_csv(csv_buffer, index=False)
        from_path = await s3_upload(
            contents=csv_buffer, key=f"upload_jobs/{uuid4()}_chuncksize{i}.csv"
        )
        path_list.append(from_path)
    logger.info(f"Path list: {path_list}")
    return await redshift_upload(
        path_list=path_list, table_name="jobs", columns="(id, job)"
    )


@app.post("/api/v1/upload_hired_employees")
async def upload(file: UploadFile | None = None):
    check_file_format(file)
    contents = await file.read()
    buffer = BytesIO(contents)
    path_list = []
    for i, chunk in enumerate(
        pd.read_csv(
            buffer,
            names=["id", "name", "datetime", "department_id", "job_id"],
            chunksize=1000,
        )
    ):
        cleaned_df = clean_data_format(chunk, hired_employees_schema)
        csv_buffer = StringIO()
        cleaned_df.to_csv(csv_buffer, index=False)
        from_path = await s3_upload(
            contents=csv_buffer,
            key=f"upload_hired_employees/{uuid4()}_chuncksize{i}.csv",
        )
        path_list.append(from_path)
    logger.info(f"Path list: {path_list}")
    return await redshift_upload(
        path_list=path_list,
        table_name="hired_employees",
        columns="(id, name, datetime, department_id, job_id)",
    )


@app.post("/api/v1/create_backup")
async def create_backup(table: TableNames):
    try:
        conn = connect(
            dbname=DB_NAME,
            host=HOST,
            port="5439",
            user=USER,
            password=PASSWORD,
            connect_timeout=5,
        )
        cursor = conn.cursor()
        today = date.today()
        location = f"s3://{AWS_BUCKET}/backup_{table}/{today}_"
        query = f"UNLOAD ('select * from {table}') TO '{location}' CREDENTIALS 'aws_access_key_id={AWS_KEY_ID};aws_secret_access_key={AWS_SECRET_KEY}' ALLOWOVERWRITE PARQUET"
        logger.info(
            f"Executing query: UNLOAD ('select * from {table}') TO {location} ALLOWOVERWRITE PARQUET"
        )
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        return {
            "status_code": 200,
            "message": f"Backup were created for {today} to table: {table}; S3 location: {location}*",
        }
    except Exception as err:
        if conn:
            conn.close()
        if cursor:
            cursor.close()
        raise HTTPException(status_code=400, detail=err.pgerror)


@app.post("/api/v1/upload_parquet_backup")
async def create_backup(s3_location: str, table: TableNames):
    logger.info(f"Uploading {s3_location} to {table.value} table in Redshift db")
    try:
        conn = connect(
            dbname=DB_NAME,
            host=HOST,
            port="5439",
            user=USER,
            password=PASSWORD,
            connect_timeout=5,
        )
        cursor = conn.cursor()
        query = f"COPY {table.value} FROM '{s3_location}' CREDENTIALS 'aws_access_key_id={AWS_KEY_ID};aws_secret_access_key={AWS_SECRET_KEY}' FORMAT AS PARQUET;"
        logger.info(
            f"Executing query: COPY {table.value} FROM '{s3_location}' FORMAT AS PARQUET;"
        )
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        return {
            "status_code": 200,
            "message": f"File: {s3_location} was uploaded into db correctly!",
        }
    except Exception as err:
        if conn:
            conn.close()
        if cursor:
            cursor.close()
        raise HTTPException(status_code=400, detail=err.pgerror)
