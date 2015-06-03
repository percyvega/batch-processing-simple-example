package com.percyvega;

import com.percyvega.model.PersonCSV;
import com.percyvega.model.PersonDB;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Bean
    public ItemReader<PersonCSV> reader() {
        FlatFileItemReader<PersonCSV> reader = new FlatFileItemReader<PersonCSV>();
        reader.setResource(new ClassPathResource("us-500.csv"));
        reader.setLineMapper(new DefaultLineMapper<PersonCSV>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[]{"firstName", "lastName", "street", "city", "state", "zipCode"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<PersonCSV>() {{
                setTargetType(PersonCSV.class);
            }});
        }});
        return reader;
    }

    @Bean
    public ItemProcessor<PersonCSV, PersonDB> processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public ItemWriter<PersonDB> writer(DataSource dataSource) {
        JdbcBatchItemWriter<PersonDB> writer = new JdbcBatchItemWriter<PersonDB>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<PersonDB>());
        writer.setSql("INSERT INTO people (full_name, address) VALUES (:fullName, :address)");
        writer.setDataSource(dataSource);
        return writer;
    }

    @Bean
    public Job importUserJob(JobBuilderFactory jobs, Step s1, JobExecutionListener listener) {
        return jobs.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(s1)
                .end()
                .build();
    }

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory, ItemReader<PersonCSV> reader,
                      ItemWriter<PersonDB> writer, ItemProcessor<PersonCSV, PersonDB> processor) {
        return stepBuilderFactory.get("step1")
                .<PersonCSV, PersonDB> chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}
