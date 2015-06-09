package com.dell.doradus.persistence.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target({METHOD, FIELD}) 
@Retention(RUNTIME)
public @interface Link {

    /**
     * The name of the application.
     *
     */
    String name();
    String inverseName() default "";
    String tableName();
    String fieldName();
}
