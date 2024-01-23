package com.shpp;
import java.util.Set;

import com.shpp.dto.Store;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
public class ValidatorClassTest {

    @Test
    void testValidDTO() {
        // Arrange
        ValidatorClass<Store> validatorClass = new ValidatorClass<>();
        Store validDTO = new Store("Valid Value");

        // Act
        Set<ConstraintViolation<Store>> violations = validatorClass.validateDTO(validDTO);

        // Assert
        assertTrue(violations.isEmpty());
    }

    @Test
    void testInvalidDTO() {
        // Arrange
        ValidatorClass<Store> validatorClass = new ValidatorClass<>();
        Store invalidDTO = new Store(null); // Invalid value

        // Act
        Set<ConstraintViolation<Store>> violations = validatorClass.validateDTO(invalidDTO);

        // Assert
        assertFalse(violations.isEmpty());
        assertEquals(2, violations.size());//TODO

    }

    @Test
    @DisplayName("😱")
    void testExceptionThrownForInvalidDTO() {
        // Arrange
        ValidatorClass<Store> validatorClass = new ValidatorClass<>();
        Store invalidDTO = new Store(null); // Invalid value

        // Act & Assert
        assertThrows(ValidationException.class, () -> validatorClass.validateDTO(invalidDTO));
    }
}
