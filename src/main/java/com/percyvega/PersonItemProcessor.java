package com.percyvega;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

/**
 * Created by Percy Vega on 6/1/2015.
 */
public class PersonItemProcessor implements ItemProcessor<PersonCSV, PersonDB> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PersonItemProcessor.class);

    @Override
    public PersonDB process(final PersonCSV personCSV) throws Exception {
        final String fullName = personCSV.getFirstName() + " " + personCSV.getLastName();
        final String address = personCSV.getStreet() + ", " + personCSV.getCity() + ", " + personCSV.getState() + " " + personCSV.getZipCode();

        final PersonDB personDB = new PersonDB(fullName, address);

        LOGGER.info("Converting (" + personCSV + ") into (" + personDB + ")");

        return personDB;
    }
}
