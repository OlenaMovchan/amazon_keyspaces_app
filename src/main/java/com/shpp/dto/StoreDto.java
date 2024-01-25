package com.shpp.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public class StoreDto {
    private UUID storeId;
    @NotBlank
    @NotNull
    private String location;

    public StoreDto(UUID storeId, String location) {
        this.location = location;
        this.storeId = storeId;
    }

    public StoreDto(String location) {
        this.location = location;
    }

    public String getLocation() {
        return location;
    }

    public UUID getStoreId() {
        return storeId;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
