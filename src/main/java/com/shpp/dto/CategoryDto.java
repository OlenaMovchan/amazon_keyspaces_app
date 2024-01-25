package com.shpp.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public class CategoryDto {
    @NotBlank
    @NotNull
    private String categoryName;
    private UUID categoryId;

    public CategoryDto(String categoryName) {
        this.categoryName = categoryName;
    }

    public CategoryDto(UUID categoryId, String categoryName) {
        this.categoryName = categoryName;
        this.categoryId = categoryId;
    }

    public UUID getCategoryId() {
        return categoryId;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }
}
