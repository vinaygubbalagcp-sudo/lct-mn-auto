from airflow import EmployeeOperator

emp1 = EmployeeOperator("Alice", "Developer", 30, "2015-06-01", "IT", 70000)
emp1.display_employee_details()
emp1.give_raise(5000)
emp1.change_department("Research and Development")  
emp1.years_of_service(2024)
emp1.display_employee_details()