package com.shpp.repository;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.github.javafaker.Faker;
import com.shpp.ValidatorClass;
import com.shpp.dto.CategoryDto;
import com.shpp.dto.ProductDto;
import com.shpp.dto.StoreDto;
import jakarta.validation.ConstraintViolation;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataGenerator {
    ValidatorClass validatorClass = new ValidatorClass<>();
    Faker faker = new Faker(new Locale("uk"));

    public List<CategoryDto> generateCategoryData(int totalCategories) {
        return IntStream.range(0, totalCategories)
                .mapToObj(category -> {
                    UUID categoryId = UUID.randomUUID();
                    String categoryName = String.valueOf(faker.commerce().department());
                    CategoryDto categoryDto = new CategoryDto(categoryId, categoryName);
                    Set<ConstraintViolation<CategoryDto>> violations = validatorClass.validateDTO(categoryDto);
                    return violations.isEmpty() ? categoryDto : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<StoreDto> generateStoreData(int totalStores) {
        return IntStream.range(0, totalStores)
                .mapToObj(store -> {
                    UUID storeId = UUID.randomUUID();
                    String storeAddress = String.valueOf(faker.address().fullAddress());
                    StoreDto storeDto = new StoreDto(storeId, storeAddress);
                    Set<ConstraintViolation<StoreDto>> violations = validatorClass.validateDTO(storeDto);
                    return violations.isEmpty() ? storeDto : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<ProductDto> generateProductData(int totalProducts) {
        return IntStream.range(0, totalProducts)
                .mapToObj(product -> {
                    UUID productId = UUID.randomUUID();
                    String productName = String.valueOf(faker.commerce().productName());
                    ProductDto productDto = new ProductDto(productId, productName);
                    Set<ConstraintViolation<ProductDto>> violations = validatorClass.validateDTO(productDto);
                    return violations.isEmpty() ? productDto : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

}

