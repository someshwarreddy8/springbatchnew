package com.example.springbatch.config;

import com.example.springbatch.entity.Customer;
import com.example.springbatch.repository.CustomerRepository;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.database.JpaItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.LineMapper;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    @Autowired
    private CustomerRepository customerRepository;

    @Bean
    public FlatFileItemReader<Customer> customerItemReader() {
        return new FlatFileItemReaderBuilder<Customer>()
                .name("customerItemReader")
                .resource(new ClassPathResource("customers.csv"))
                .linesToSkip(1)
                .lineMapper(lineMapper())
                .build();
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public ItemProcessor<Customer, Customer> customerItemProcessor() {
        return customer -> {
            customer.setEmail(customer.getEmail().toLowerCase());
            return customer;
        };
    }
    @Bean
    public JpaItemWriter<Customer> customerItemWriter(
            EntityManagerFactory emf) {

        return new JpaItemWriterBuilder<Customer>()
                .entityManagerFactory(emf)
                .build();
    }

    @Bean
    public Step customerStep(JobRepository jobRepository, JpaItemWriter<Customer> writer) {

        return new StepBuilder("customerStep", jobRepository)
                .<Customer, Customer>chunk(10)
                .reader(customerItemReader())
                .processor(customerItemProcessor())
                .writer(writer)
                .build();
    }

    @Bean
    public Job customerJob(JobRepository jobRepository, Step customerStep) {
        return new JobBuilder("customerJob", jobRepository)
                .start(customerStep)
                .build();
    }

}
