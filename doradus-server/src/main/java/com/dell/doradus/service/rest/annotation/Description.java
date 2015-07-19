/*
 * Copyright (C) 2014 Dell, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.service.rest.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.rest.CommandParameter;

/**
 * Defines annotations that describe a REST command. If a command uses a {@link #uri()}
 * that includes parameters, each parameter is assumed to be text and required by default.
 * To describe parameters that are not text, optional, or compound, the command should
 * create one or more public static methods that return a {@link CommandParameter} object.
 * Also, the method should be marked with the {@link ParamDescription} annotation. Each
 * such "parameter describer" method will be called to build the metadata for the
 * corresponding parameter. Below is an example parameter describer method:
 * <pre>
 *      {@literal @}ParamDescription
 *      public static CommandParameter describeParam() {
 *          return CommandParameter("size", "integer", false);
 *      }
 * </pre>
 * This example describes {size} as an optional integer parameter.
 */
@Retention(value=RetentionPolicy.RUNTIME)
public @interface Description {
    /**
     * @return A simple name that is unique within the command's owning service.
     */
    String name();

    /**
     * @return A comment that describes the command's function.
     */
    String summary() default "";
    
    /**
     * @return The {@link HttpMethod}s that can be used to invoke the command. Some
     *         commands can be invoked with multiple methods (e.g., GET and PUT).
     */
    HttpMethod[] methods();
    
    /**
     * @return The command's URI template. URI elements that are intended to be replaced
     *         with literal values are enclosed in curly braces. Example:
     * <pre>
     *         /{application}/{table}/_stuff?{params}
     * </pre>
     *         In this URI, {application}, {table}, and {params} are parameters. Each is
     *         considered a required text parameter unless the {@link ParamDescription}
     *         annotation is used to define one or more "parameter describers" that
     *         provide more details.
     */
    String uri();
    
    /**
     * @return Indicates if the command should be visible to client applications via the
     *         "describe" command. All commands are visible unless this is set to false.
     *         One reason to make a command not visible is because a better, more general
     *         alternative exists.
     */
    boolean visible() default true;
    
    /**
     * @return Indicates if the command is considered privileged. Depending on the server
     *         configuration, privileged commands may only be accessible when "super user"
     *         credentials are supplied with the command.
     */
    boolean privileged() default false;
    
    /**
     * @return When non-null, indicates that the command requires an input entity. When
     *         the input entity name is a literal (e.g., "search"), it may be further
     *         described by a "parameter describer" method.
     */
    String inputEntity() default "";
    
    /**
     * @return When non-null, indicates that the command normally returns an output
     *         entity. (All commands may return an output entity when an error occurs.)
     *         When the output entity name is a literal (e.g., "results"), it may be
     *         further described by a "parameter describer" method.
     */
    String outputEntity() default "";
}
