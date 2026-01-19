package com.floe.server.api.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.quartz.CronExpression;

/** Validates cron expressions using Quartz's built-in validator. */
public class CronExpressionValidator implements ConstraintValidator<ValidCronExpression, String> {

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (value == null || value.isBlank()) {
            return true;
        }

        if (!CronExpression.isValidExpression(value)) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate("Invalid cron expression: " + value)
                    .addConstraintViolation();
            return false;
        }

        return true;
    }
}
