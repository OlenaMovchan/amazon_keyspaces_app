package com.shpp.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public class ProductDto {

    private UUID productId;
    @NotNull
    @NotBlank
    private String name;
    private UUID categoryId;

    public ProductDto(UUID productId, String name, UUID categoryId) {
        this.productId = productId;
        this.name = name;
        this.categoryId = categoryId;
    }

    public UUID getCategoryId() {
        return categoryId;
    }

    public String getName() {
        return name;
    }

    public ProductDto(UUID productId) {
        this.productId = productId;
    }

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

}
