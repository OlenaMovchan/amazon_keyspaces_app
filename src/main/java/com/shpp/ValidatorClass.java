package com.shpp;

import jakarta.validation.*;

import java.util.Set;


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
//public class ValidatorClass {
//    private final Validator validator;
//
//    public ValidatorClass() {
//        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
//        this.validator = factory.getValidator();
//    }
//
//    public <T> boolean validateDTO(T dto) {
//        Set<ConstraintViolation<T>> violations = validator.validate(dto);
//        if (!violations.isEmpty()) {
//            throw new ValidationException("DTO validation failed");
//        } else {
//            return true;
//        }
//    }
//}
