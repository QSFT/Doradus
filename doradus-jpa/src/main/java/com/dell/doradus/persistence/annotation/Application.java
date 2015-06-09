package com.dell.doradus.persistence.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target(TYPE) 
@Retention(RUNTIME)
public @interface Application {

    /**
     * The name of the application.
     *
     */
    String name();

    /**
     * 
     * @return
     */
    boolean ddlAutoCreate() default false;
    /**
     * 
     * @return
     */
    String storageService() default "SpiderService";
}
