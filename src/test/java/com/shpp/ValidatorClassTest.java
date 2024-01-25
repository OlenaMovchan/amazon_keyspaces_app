package com.shpp;

import java.util.Set;
import com.shpp.dto.CategoryDto;
import com.shpp.dto.StoreDto;
import jakarta.validation.ConstraintViolation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ValidatorClassTest {

    @Test
    void validateCategoryPositive() {

        ValidatorClass<CategoryDto> validatorClass = new ValidatorClass<>();
        CategoryDto validDTO = new CategoryDto("Музика");

        Set<ConstraintViolation<CategoryDto>> violations = validatorClass.validateDTO(validDTO);

        assertTrue(violations.isEmpty());
    }

    @Test
    @DisplayName("😱")
    void validateCategoryNegative() {

        ValidatorClass<CategoryDto> validatorClass = new ValidatorClass<>();
        CategoryDto invalidDTO = new CategoryDto(null);

        Set<ConstraintViolation<CategoryDto>> violations = validatorClass.validateDTO(invalidDTO);

        assertFalse(violations.isEmpty());

        assertEquals(2, violations.size());
    }

    @Test
    @DisplayName("😱")
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
    @DisplayName("😱")
    void validateStoreNegative() {

        ValidatorClass<StoreDto> validatorClass = new ValidatorClass<>();
        StoreDto invalidDTO = new StoreDto(null);

        Set<ConstraintViolation<StoreDto>> violations = validatorClass.validateDTO(invalidDTO);

        assertFalse(violations.isEmpty());
        assertEquals(2, violations.size());
    }

    @Test
    @DisplayName("😱")
    void validateStoreBad() {

        ValidatorClass<StoreDto> validatorClass = new ValidatorClass<>();
        StoreDto invalidDTO = new StoreDto("");

        Set<ConstraintViolation<StoreDto>> violations = validatorClass.validateDTO(invalidDTO);

        assertFalse(violations.isEmpty());
        assertEquals(1, violations.size());
    }
}
