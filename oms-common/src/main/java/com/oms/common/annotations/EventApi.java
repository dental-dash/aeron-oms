package com.oms.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated class serves as an Aggregate Root
 * within the Domain-Driven Design (DDD) structure.
 * * This annotation can only be applied to Type declarations (Classes, Interfaces, Enums).
 */
@Target(ElementType.TYPE) // Enforces that this can ONLY be used on classes/interfaces/enums
@Retention(RetentionPolicy.RUNTIME) // Allows the annotation to be inspected at runtime via reflection
public @interface EventApi {
    // Optional: Add attributes relevant to an aggregate, like a classification type
    String type() default "Root";
}