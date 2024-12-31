package com.adityaworkspace.springbatch2.component;

import com.adityaworkspace.springbatch2.model.Employee;
import org.apache.poi.ss.usermodel.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;

@Component
@StepScope
public class ExcelEmployeeReader implements ItemReader<Employee> {

    private Iterator<Row> rowIterator;
    private boolean isInitialized = false;

    @Value("#{jobParameters['filePath']}")
    private String filePath;

    private void initializeReader(String filePath) throws Exception {
        InputStream inputStream = new FileInputStream(filePath);
        Workbook workbook = WorkbookFactory.create(inputStream);
        Sheet sheet = workbook.getSheetAt(0); // Assuming the first sheet contains the data
        rowIterator = sheet.iterator();
        rowIterator.next(); // Skip the header row
        isInitialized = true;
    }

    @Override
    public Employee read() throws Exception {
        if (!isInitialized) {
            initializeReader(filePath);
        }

        if (rowIterator != null && rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Employee employee = new Employee();

            // Assuming the order of columns: id, name, username, gender, salary
            Cell nameCell = row.getCell(1);
            Cell usernameCell = row.getCell(2);
            Cell genderCell = row.getCell(3);
            Cell salaryCell = row.getCell(4);

            if (nameCell != null) {
                employee.setName(nameCell.getStringCellValue());
            }
            if (usernameCell != null) {
                employee.setUsername(usernameCell.getStringCellValue());
            }
            if (genderCell != null) {
                employee.setGender(genderCell.getStringCellValue());
            }
            if (salaryCell != null) {
                if (salaryCell.getCellType() == CellType.NUMERIC) {
                    employee.setSalary((long) salaryCell.getNumericCellValue());
                } else if (salaryCell.getCellType() == CellType.STRING) {
                    employee.setSalary(Long.parseLong(salaryCell.getStringCellValue()));
                }
            }

            return employee;
        }
        return null; // Return null to indicate no more records
    }
}

