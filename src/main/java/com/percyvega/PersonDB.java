package com.percyvega;

/**
 * Created by Percy Vega on 6/1/2015.
 */
public class PersonDB {

    private String fullName;
    private String address;

    public PersonDB() {
    }

    public PersonDB(String fullName, String address) {
        this.fullName = fullName;
        this.address = address;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "PersonDB{" +
                "fullName='" + fullName + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}
