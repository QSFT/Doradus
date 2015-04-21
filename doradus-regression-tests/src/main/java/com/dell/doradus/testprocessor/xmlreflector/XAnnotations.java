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

package com.dell.doradus.testprocessor.xmlreflector;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class XAnnotations
{
    static public IXFieldReflector getXFieldReflector(Field field)
    {
        for (Annotation annotation : field.getAnnotations()) {
            if (annotation instanceof IXFieldReflector)
                return (IXFieldReflector) annotation;
        }
        return null;
    }

    static public IXSetterReflector getXSetterReflector(Method method)
    {
        for (Annotation annotation : method.getAnnotations()) {
            if (annotation instanceof IXSetterReflector)
                return (IXSetterReflector) annotation;
        }
        return null;
    }

    static public IXTypeReflector getXTypeReflector(Class<?> type)
    {
        for (Annotation annotation : type.getAnnotations()) {
            if (annotation instanceof IXTypeReflector)
                return (IXTypeReflector) annotation;
        }
        return null;
    }

    static public IXMLReflectorSetter getXMLReflectorSetter(Method method)
    {
        for (Annotation annotation : method.getAnnotations()) {
            if (annotation instanceof IXMLReflectorSetter)
                return (IXMLReflectorSetter) annotation;
        }
        return null;
    }
}
