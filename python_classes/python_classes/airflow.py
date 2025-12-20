# give a very small example of a class
class EmployeeOperator:
    def __init__(self, name, designation, age, doj, depart, salary): # constructor method, initializes the object, called when an object is created
        self.name = name
        self.designation = designation
        self.age = age
        self.doj = doj
        self.depart = depart
        self.salary = salary
    # method to display employee details
    def display_employee_details(self): 
        print(f"Name: {self.name}, Designation: {self.designation}, Age: {self.age}, Date of Joining: {self.doj}, Department: {self.depart}, Salary: {self.salary}")    
    # method to give raise
    def give_raise(self, amount):
        self.salary += amount
        print(f"{self.name}'s new salary after raise is: {self.salary}")
    # method to change department
    def change_department(self, new_department):
        self.depart = new_department
        print(f"{self.name} has been moved to the {self.depart} department.")
    # method to calculate years of service
    def years_of_service(self, current_year):
        joining_year = int(self.doj.split("-")[0])
        years = current_year - joining_year
        print(f"{self.name} has {years} years of service.")
     
