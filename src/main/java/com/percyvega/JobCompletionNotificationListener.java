package com.percyvega;

import com.percyvega.model.PersonDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Percy Vega on 6/1/2015.
 */
@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        LOGGER.info("Executing afterJob -----------------------------------------------------------------------------------------------");
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            List<PersonDB> results = jdbcTemplate.query("SELECT full_name, address FROM people", new RowMapper<PersonDB>() {
                @Override
                public PersonDB mapRow(ResultSet resultSet, int i) throws SQLException {
                    return new PersonDB(resultSet.getString("full_name"), resultSet.getString("address"));
                }
            });

            for (PersonDB personDB : results) {
                LOGGER.info("Found <" + personDB + "> in the database.");
            }
        } else {
            LOGGER.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! jobExecution.getStatus(): " + jobExecution.getStatus() + " !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
    }
}
