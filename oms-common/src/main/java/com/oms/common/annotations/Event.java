package com.oms.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated method serves as an event handler.
 * This annotation can only be applied to methods.
 */
@Target(ElementType.METHOD) // Enforces that this can ONLY be used on methods
@Retention(RetentionPolicy.RUNTIME) // Makes the annotation available at runtime for reflection
public @interface Event {
    // You can optionally add attributes here, for example:
    String name() default "";
    boolean async() default false;
}