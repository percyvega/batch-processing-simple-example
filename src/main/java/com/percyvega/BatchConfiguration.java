package com.percyvega;

import com.percyvega.model.PersonCSV;
import com.percyvega.model.PersonDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    public ItemReader<PersonCSV> itemReader() {
        FlatFileItemReader<PersonCSV> flatFileItemReader = new FlatFileItemReader<PersonCSV>();
        flatFileItemReader.setResource(new ClassPathResource("us-500.csv"));
        flatFileItemReader.setLineMapper(new DefaultLineMapper<PersonCSV>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[]{"firstName", "lastName", "street", "city", "state", "zipCode"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<PersonCSV>() {{
                setTargetType(PersonCSV.class);
            }});
        }});
        return flatFileItemReader;
    }

    @Bean
    public ItemProcessor<PersonCSV, PersonDB> itemProcessor() {
        final Logger LOGGER = LoggerFactory.getLogger(BatchConfiguration.class);
        return new ItemProcessor<PersonCSV, PersonDB>() {
            @Override
            public PersonDB process(final PersonCSV personCSV) throws Exception {
                final String fullName = personCSV.getFirstName() + " " + personCSV.getLastName();
                final String address = personCSV.getStreet() + ", " + personCSV.getCity() + ", " + personCSV.getState() + " " + personCSV.getZipCode();
                final PersonDB personDB = new PersonDB(fullName, address);

                LOGGER.info("Converting " + personCSV + " into " + personDB);
                return personDB;
            }
        };
    }

    @Bean
    public ItemWriter<PersonDB> itemWriter(DataSource dataSource) {
        JdbcBatchItemWriter<PersonDB> jdbcBatchItemWriter = new JdbcBatchItemWriter<PersonDB>();
        jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<PersonDB>());
        jdbcBatchItemWriter.setSql("INSERT INTO people (full_name, address) VALUES (:fullName, :address)");
        jdbcBatchItemWriter.setDataSource(dataSource);
        return jdbcBatchItemWriter;
    }

    @Bean
    public Job importUserJob(JobBuilderFactory jobBuilderFactory, Step step, JobExecutionListener jobExecutionListener) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobExecutionListener)
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, ItemReader<PersonCSV> itemReader,
                      ItemWriter<PersonDB> itemWriter, ItemProcessor<PersonCSV, PersonDB> itemProcessor) {
        return stepBuilderFactory.get("step")
                .<PersonCSV, PersonDB> chunk(10)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}
