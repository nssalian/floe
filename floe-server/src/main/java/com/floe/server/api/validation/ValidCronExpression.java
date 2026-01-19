package com.floe.server.api.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.*;

/**
 * Validates that a string is a valid cron expression.
 *
 * <p>Supports standard 6-field cron expressions (second minute hour day-of-month month day-of-week)
 * as well as 5-field expressions (minute hour day-of-month month day-of-week).
 */
@Documented
@Constraint(validatedBy = CronExpressionValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidCronExpression {
    String message() default "Invalid cron expression";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
