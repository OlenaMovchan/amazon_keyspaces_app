package com.shpp;

import jakarta.validation.*;
import java.util.Set;

public class ValidatorClass<T> {

    private final Validator validator;

    public ValidatorClass() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        this.validator = factory.getValidator();
    }

    public Set<ConstraintViolation<T>> validateDTO(T dto) {
        return validator.validate(dto);
    }
}

