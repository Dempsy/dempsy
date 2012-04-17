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

package com.nokia.dempsy.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 *  <p>Marker annotation to tell the message dispatcher that the annotated object
 *  can receive messages.</p>
 *  
 *  <p>This annotation is inherited; you can subclass a {@link MessageProcessor} and
 *  declare the annotation on the parent class.</p>
 *  
 *  <p>A {@link MessageProcessor} is required to have at least one method annotated
 *  as a {@link MessageHandler} to receive messages being passed through Dempsy.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface MessageProcessor
{
   // nothing to see here, move along
}
