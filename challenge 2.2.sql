with emplooyes_hired_by_department_2021 AS
(
    select
    d.department,
    count(he.name) as employees_hired
    from hired_employees as he 
    join departments as d on he.department_id = d.id
    where datetime >= '2021-01-01' AND datetime < '2022-01-01'
    group by d.department
)

select 
d.id, d.department, count(he.name) as hired 
from hired_employees as he 
join departments as d on he.department_id = d.id
GROUP BY d.id, d.department
HAVING COUNT(he.name) > (select avg(employees_hired) from emplooyes_hired_by_department_2021)
ORDER BY hired DESC;

