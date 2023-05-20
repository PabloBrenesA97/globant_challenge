import logging
from uuid import uuid4
from fastapi import FastAPI, UploadFile, HTTPException
import magic
import boto3
from psycopg2 import connect
import sys
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("my-api")

app = FastAPI()

AWS_BUCKET = "raw-challenge-globant-uploads"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def check_file_format(file: UploadFile | None = None):
    if not file:
        raise HTTPException(
            status_code=400,
            detail="No file found!"
        )
    filename = file.filename
    logger.info(f"Processing file: {filename}")
    if not filename.endswith(".csv"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {filename}. Supported type is CSV"
        )


async def s3_remove_file(from_path: str):
    path = from_path.replace("s3://", "").split("/", maxsplit=1)[-1]
    try:
        s3_client = boto3.client("s3",
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        
        response = s3_client.delete_object(Bucket=AWS_BUCKET, Key=path)
        logger.info(f"Something was wrong. Deleting {from_path}")
        return response
    except Exception as error:
        raise HTTPException(
            status_code=400,
            detail=error
        )

async def s3_upload(contents: bytes, key:str):
    logger.info(f"Uploading {key} to s3")
    try:
        s3 = boto3.resource("s3",
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        bucket = s3.Bucket(AWS_BUCKET)
        bucket.put_object(Key=key, Body=contents)
        return f"s3://{AWS_BUCKET}/{key}" # from_path
    except Exception as error:
        raise HTTPException(
            status_code=400,
            detail=error
        )
 
async def redshift_upload(from_path: str, table_name: str, columns: str):
    logger.info(f"Uploading {from_path} to {table_name} table in Redshift db")
    dbname = os.getenv("DB_NAME")
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    try:
        conn = connect(
            dbname = dbname,
            host = host,
            port = '5439',
            user = user,
            password = password,
            connect_timeout=5
        )
        cursor = conn.cursor()
        query = f"COPY {table_name} {columns} FROM '{from_path}' CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}' TIMEFORMAT 'YYYY-MM-DDTHH:MI:SSZ' CSV;"
        logger.info(f"Executing query: COPY {table_name} FROM '{from_path} TIMEFORMAT 'YYYY-MM-DDTHH:MI:SSZ' CSV;")
        cursor.execute(query)
        result = conn.commit()
        logger.info(f"Result: {result}")
        cursor.close()
        conn.close()
        return {
            "status_code": 200,
            "message": f"File {from_path} was uploaded into db correctly!"
        }
    except Exception as err:
        if conn:
           conn.close()
        if cursor:
            cursor.close()
        await s3_remove_file(from_path)
        raise HTTPException(
            status_code=400,
            detail=err.pgerror
        )
@app.get("/")
def root():
    return {"message": "Welcome from file-upload 👋🏻🚀"}

@app.post("/api/v1/upload_departments")
async def upload(file: UploadFile | None = None):
    check_file_format(file)
    contents = await file.read()
    from_path = await s3_upload(contents=contents, key=f"upload_departments/{uuid4()}.csv")
    return await redshift_upload(from_path=from_path, table_name="departments", columns="(id, department)")

@app.post("/api/v1/upload_jobs")
async def upload(file: UploadFile | None = None):
    check_file_format(file)
    contents = await file.read()
    from_path = await s3_upload(contents=contents, key=f"upload_jobs/{uuid4()}.csv")
    return await redshift_upload(from_path=from_path, table_name="jobs", columns="(id, job)")

@app.post("/api/v1/upload_hired_employees")
async def upload(file: UploadFile | None = None):
    check_file_format(file)
    contents = await file.read()
    from_path = await s3_upload(contents=contents, key=f"upload_hired_employees/{uuid4()}.csv")
    return await redshift_upload(from_path=from_path, table_name="hired_employees", columns="(id, name, datetime, department_id, job_id)")