package com.adityaworkspace.springbatch2.controller;

import org.springframework.batch.core.Job;
    import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


@RestController
@RequestMapping("/jobs/")
public class JobController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @PostMapping("employee")
    public ResponseEntity<String> importExcelToDBjob(@RequestParam("file") MultipartFile file){
        try {
            // Save the uploaded file to a temporary location
            Path tempFile = Files.createTempFile("employee_data_", ".xlsx");
            Files.write(tempFile, file.getBytes());
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("startAt", System.currentTimeMillis())
                    .addString("filePath", tempFile.toString()) // add filePath as Job Parameter
                    .toJobParameters(); // adding Job Params
            jobLauncher.run(job, jobParameters); // running Job - Entry Point for Spring Batch - This will look for Job Bean - job() in our case
            return ResponseEntity.ok("Data Uploaded Successfully");
        } catch (
                JobExecutionAlreadyRunningException | JobRestartException |
                JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Exception while data upload: " + e.getMessage());
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Exception while data upload: " + e.getMessage());
        }
    }
}
