package com.shpp.dto;

import com.github.javafaker.Faker;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Locale;

public class Store {
    @NotBlank
    @NotNull
    private String location;
    public Store(String location) {
        this.location = location;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
