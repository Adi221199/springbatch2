package com.adityaworkspace.springbatch2.component;

import com.adityaworkspace.springbatch2.model.Employee;
import com.adityaworkspace.springbatch2.repository.EmployeeRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
public class EmployeeWriter implements ItemWriter<Employee> {

    @Autowired
    private EmployeeRepository employeeRepository;

    @Override
    public void write(Chunk<? extends Employee> chunk) throws Exception {
        System.out.println("Thread Name: " + Thread.currentThread().getName());

        List<? extends Employee> employees = chunk.getItems();
        for (Employee employee : employees) {
            // Check if an employee with the same unique fields already exists
            Optional<Employee> existingEmployee = employeeRepository.findByNameAndUsernameAndGenderAndSalary(
                    employee.getName(), employee.getUsername(), employee.getGender(), employee.getSalary());

            // If not exists, then save the new employee record
            if (!existingEmployee.isPresent()) {
                employeeRepository.save(employee);
            } else {
                System.out.println("Duplicate employee found: " + employee.getUsername());
            }
        }
    }
}



//import com.adityaworkspace.springbatch2.model.Employee;
//import com.adityaworkspace.springbatch2.repository.EmployeeRepository;
//import org.springframework.batch.item.Chunk;
//import org.springframework.batch.item.ItemWriter;
//import org.springframework.beans.factory.annotation.Autowired;
//
//public class EmployeeWriter implements ItemWriter<Employee> {
//
//    @Autowired
//    private EmployeeRepository employeeRepository;
//
//    @Override
//    public void write(Chunk<? extends Employee> chunk) throws Exception {
//        System.out.println("Thread Name: " + Thread.currentThread().getName());
//        employeeRepository.saveAll(chunk.getItems());
//    }
//}
