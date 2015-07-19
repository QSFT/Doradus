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

import java.lang.annotation.*;

import com.dell.doradus.common.rest.CommandParameter;

/** 
 * This marks a static method as one that returns a {@link CommandParameter} to descrube
 * a parameter used in a REST command. The parameter may be used in the URI or in an
 * input parameter. Such "parameter describers" are only needed for parameters that need
 * extra semantics such as being optional, having a type other than text, or being
 * compound. For example, suppose a REST command has the format: 
 * <pre>
 *      /{foo}/{bar}/?{params}
 * </pre>
 * If {foo} and {bar} are both text and requird, no extra semantics are needed. However, a
 * {@link ParamDescription} annotation might be used to denote a method that describes the
 * {params} parameter as compound.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ParamDescription { }
