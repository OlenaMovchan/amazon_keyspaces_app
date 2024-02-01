package com.shpp;

import java.util.*;

import com.shpp.dto.CategoryDto;
import com.shpp.dto.ProductDto;
import com.shpp.dto.StoreDto;
import jakarta.validation.ConstraintViolation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValidatorClassTest {
    @BeforeAll
    public static void setUp() {
        Locale.setDefault(new Locale("uk", "UA"));
    }

    @Test
    void validateCategoryPositive() {

        ValidatorClass<CategoryDto> validatorClass = new ValidatorClass<>();
        CategoryDto validDTO = new CategoryDto("Музика");

        Set<ConstraintViolation<CategoryDto>> violations = validatorClass.validateDTO(validDTO);

        assertTrue(violations.isEmpty());
    }

    @Test
    @DisplayName("Validation of the category with name null 😱")
    void validateCategoryNegative() {

        ValidatorClass<CategoryDto> validatorClass = new ValidatorClass<>();
        CategoryDto invalidDTO = new CategoryDto(null);

        Set<ConstraintViolation<CategoryDto>> violations = validatorClass.validateDTO(invalidDTO);

        assertFalse(violations.isEmpty());

        assertEquals(2, violations.size());
    }

    @Test
    @DisplayName("Validation of the category with blank name 😱")
    void validateCategoryBad() {

        ValidatorClass<CategoryDto> validatorClass = new ValidatorClass<>();
        CategoryDto invalidDTO = new CategoryDto("");

        Set<ConstraintViolation<CategoryDto>> violations = validatorClass.validateDTO(invalidDTO);

        assertFalse(violations.isEmpty());

        assertEquals(1, violations.size());
    }

    @Test
    void validateStorePositive() {

        ValidatorClass<StoreDto> validatorClass = new ValidatorClass<>();
        StoreDto validDTO = new StoreDto("вулиця Староміська, 8, Запоріжжя,  84478");

        Set<ConstraintViolation<StoreDto>> violations = validatorClass.validateDTO(validDTO);

        assertTrue(violations.isEmpty());
    }

    @Test
    @DisplayName("Validation of the store with address null 😱")
    void validateStoreNegative() {

        ValidatorClass<StoreDto> validatorClass = new ValidatorClass<>();
        StoreDto invalidDTO = new StoreDto(null);

        Set<ConstraintViolation<StoreDto>> violations = validatorClass.validateDTO(invalidDTO);

        assertFalse(violations.isEmpty());
        assertEquals(2, violations.size());
    }

    @Test
    @DisplayName("Validation of the store with blank address 😱")
    void validateStoreBad() {

        ValidatorClass<StoreDto> validatorClass = new ValidatorClass<>();
        StoreDto invalidDTO = new StoreDto("");

        Set<ConstraintViolation<StoreDto>> violations = validatorClass.validateDTO(invalidDTO);

        assertFalse(violations.isEmpty());
        assertEquals(1, violations.size());
    }

    @Test
    void validateProductPositive() {

        ValidatorClass<ProductDto> validatorClass = new ValidatorClass<>();
        ProductDto validDTO = new ProductDto(UUID.randomUUID(), "продукт", UUID.randomUUID());

        Set<ConstraintViolation<ProductDto>> violations = validatorClass.validateDTO(validDTO);

        assertTrue(violations.isEmpty());
    }

    @Test
    @DisplayName("Validation of the product with name null 😱")
    void validateProductNegative() {

        ValidatorClass<ProductDto> validatorClass = new ValidatorClass<>();
        ProductDto invalidDTO = new ProductDto(UUID.randomUUID(), null, UUID.randomUUID());

        Set<ConstraintViolation<ProductDto>> violations = validatorClass.validateDTO(invalidDTO);

        assertFalse(violations.isEmpty());
        assertEquals(2, violations.size());
    }

    @Test
    @DisplayName("Validation of the product with blank name 😱")
    void validateProductBad() {

        ValidatorClass<ProductDto> validatorClass = new ValidatorClass<>();
        ProductDto invalidDTO = new ProductDto(UUID.randomUUID(), "", UUID.randomUUID());

        Set<ConstraintViolation<ProductDto>> violations = validatorClass.validateDTO(invalidDTO);

        assertFalse(violations.isEmpty());
        assertEquals(1, violations.size());
    }

    @Test
    void testValidationMessagesInUkrainian() {
        ValidatorClass<StoreDto> validatorClass = new ValidatorClass<>();
        StoreDto dto = new StoreDto(UUID.randomUUID(), null);

        Set<ConstraintViolation<StoreDto>> violations = validatorClass.validateDTO(dto);

        assertEquals(2, violations.size());
        List<String> list = new ArrayList<>();

        for (ConstraintViolation<StoreDto> violation : violations) {
            String message = violation.getMessage();
            list.add(message);
            System.out.println(message);
        }
        //assertEquals("не може бути відсутнім, має бути задано", list.get(0));
        //assertEquals("не може бути пустим", list.get(1));
    }


}
