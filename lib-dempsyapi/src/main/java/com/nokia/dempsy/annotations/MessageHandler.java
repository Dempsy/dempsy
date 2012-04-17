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

import com.nokia.dempsy.Dispatcher;


/**
 *  <p>Marker annotation to tell the Message Dispatcher that a method is responsible
 *  for handling a message. The method must take a single parameter, which may be
 *  a concrete class or interface type. If concrete, the {@link Dispatcher} will invoke
 *  the method for messages of any concrete subclass that is not already handled
 *  by another method.</p>
 *  
 *  <p>Multiple methods may be so annotated, and the annotation is inherited.</p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface MessageHandler
{
   // nothing to see here, move along
}
