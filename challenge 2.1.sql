WITH hired_by_month_2021
AS (
    select 
    d.department as department,
    j.job as job,
    CAST(CAST(DATEPART(MONTH,he.datetime) AS VARCHAR(2)) AS int) as month_num
    from hired_employees as he 
    join departments as d on he.department_id = d.id
    join jobs as j on he.job_id = j.id
    where datetime >= '2021-01-01' AND datetime < '2022-01-01'
)

select
    department,
    job,
    SUM(CASE WHEN month_num BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Q1,
    SUM(CASE WHEN month_num BETWEEN 3 AND 6 THEN 1 ELSE 0 END) AS Q2,
    SUM(CASE WHEN month_num BETWEEN 6 AND 9 THEN 1 ELSE 0 END) AS Q3,
    SUM(CASE WHEN month_num BETWEEN 9 AND 12 THEN 1 ELSE 0 END) AS Q4
from hired_by_month_2021
group by department, job
order by department, job;