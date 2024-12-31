package com.adityaworkspace.springbatch2.repository;

import com.adityaworkspace.springbatch2.model.Employee;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EmployeeRepository extends JpaRepository<Employee, Integer> {
    Optional<Employee> findByNameAndUsernameAndGenderAndSalary(String name, String username, String gender, long salary);
}
