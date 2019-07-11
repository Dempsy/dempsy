/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.lifecycle.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *  <p>Marker annotation to tell the Container that a method should be called
 *  to check if the {@link Mp} can be evicted. This method should return 
 *  boolean indicating the state of {@link Mp}.</p>
 *  
 *  <p>Return value of True indicated state of {@link Mp} as ready
 *  to be evicted and hence the {@link Mp} should NOT be used to handle
 *  any new messages or be part of output cycle.</p>
 *  
 *  <p>Only one method in a class should be marked as an Evictable method. If
 *  multiple methods are so annotated, the behavior is undefined. For this
 *  reason, this annotation is not inherited; subclasses must explicitly call
 *  their parent's evictable method.</p>
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Evictable
{

}
