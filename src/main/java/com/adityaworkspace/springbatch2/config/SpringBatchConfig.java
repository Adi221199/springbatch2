package com.adityaworkspace.springbatch2.config;

import com.adityaworkspace.springbatch2.component.EmployeeProcessor;
import com.adityaworkspace.springbatch2.component.EmployeeWriter;
import com.adityaworkspace.springbatch2.component.ExcelEmployeeReader;
import com.adityaworkspace.springbatch2.model.Employee;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    @Value("classpath:org/springframework/batch/core/schema-drop-postgresql.sql")
    private Resource dropRepositoryTables;

    @Value("classpath:org/springframework/batch/core/schema-postgresql.sql")
    private Resource dataRepositorySchema;

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${spring.datasource.driver-class-name}")
    private String driverClassName;

//    @Bean
//    public FlatFileItemReader<Employee> reader() {
//        FlatFileItemReader<Employee> itemReader = new FlatFileItemReader<>();
//        itemReader.setResource(new FileSystemResource("src/main/resources/employee_data.csv")); // setting resource for reading
//        itemReader.setName("employeeReader"); //setting reader component name
//        itemReader.setLinesToSkip(1); // how many lines to skip from top
//        itemReader.setLineMapper(lineMapper());
//
//        return itemReader;
//    }
//
//    private LineMapper<Employee> lineMapper() {
//        DefaultLineMapper<Employee> lineMapper = new DefaultLineMapper<>();
//
//        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
//        lineTokenizer.setDelimiter(","); // setting delimiter used in csv
//        lineTokenizer.setStrict(false); // setting this false means - if few entries were missing for some rows, then it can read that as well
//        lineTokenizer.setNames("id", "name", "username", "gender", "salary");
//
//        BeanWrapperFieldSetMapper<Employee> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
//        fieldSetMapper.setTargetType(Employee.class); // mapping with the entity - remember that Entity variables are same as CSV header names
//
//        lineMapper.setLineTokenizer(lineTokenizer);
//        lineMapper.setFieldSetMapper(fieldSetMapper);
//
//        return lineMapper;
//
//    }

    @Bean
    @StepScope
    public ExcelEmployeeReader excelReader() {
        return new ExcelEmployeeReader();
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("step1",jobRepository).<Employee, Employee> chunk(10, transactionManager) // means 1 chunk is of 10 size
                .reader(excelReader())
                .processor(processor())
                .writer(writer())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean(name = "batchJob")
    public Job job(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("importEmployeeFromCsvToDb", jobRepository)
                .preventRestart() // set flag to prevent restart of the execution of this job
                .start(step1(jobRepository, transactionManager)) // calling 1st step
                //.next(nextStep) // to call next steps
                .build();
    }

    @Bean(name = "transactionManager")
    public PlatformTransactionManager getTransactionManager(){
        return new JpaTransactionManager();
    }

    @Bean(name = "jobRepository")
    public JobRepository getJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(postgresDataSource());
        factory.setTransactionManager(getTransactionManager());
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    public DataSource postgresDataSource() {
        DriverManagerDataSource datasource = new DriverManagerDataSource();
        datasource.setDriverClassName(driverClassName);
        datasource.setUrl(dbUrl);
        datasource.setUsername(username);
        datasource.setPassword(password);

        dataSourceInitializer(datasource);
        return datasource;
    }

    @Bean
    public DataSourceInitializer dataSourceInitializer(DataSource dataSource) {

        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();
        databasePopulator.addScript(dropRepositoryTables);
        databasePopulator.addScript(dataRepositorySchema);
        databasePopulator.setIgnoreFailedDrops(false);

        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        initializer.setDatabasePopulator(databasePopulator);

        return initializer;
    }

    @Bean
    public EmployeeProcessor processor() {
        return new EmployeeProcessor();
    }

    @Bean
    public EmployeeWriter writer() {
        return new EmployeeWriter();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(10); // 10 threads will process the data concurrently
        return simpleAsyncTaskExecutor;
    }
}
