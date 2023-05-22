from pandas_schema import Column, Schema
import pandas as pd
from pandas_schema.validation import DateFormatValidation, CustomElementValidation

null_validation = CustomElementValidation(
    lambda x: not pd.isna(x), "this field cannot be null"
)
datetime_validation = DateFormatValidation("%Y-%m-%dT%H:%M:%SZ")

hired_employees_schema = Schema(
    [
        Column("id", [null_validation], allow_empty=False),
        Column("name", [null_validation], allow_empty=False),
        Column("datetime", [datetime_validation], allow_empty=False),
        Column("department_id", [null_validation], allow_empty=False),
        Column("job_id", [null_validation], allow_empty=False),
    ]
)

jobs_schema = Schema(
    [
        Column("id", [null_validation], allow_empty=False),
        Column("job", [null_validation], allow_empty=False),
    ]
)

departments_schema = Schema(
    [
        Column("id", [null_validation], allow_empty=False),
        Column("department", [null_validation], allow_empty=False),
    ]
)
