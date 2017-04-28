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
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to indicate a method on a MessageProcessor that should
 * be called during the start dempsy, after setting the prototype, but before
 * initializing the listener or accepting any messages.  That implies it is also
 * before cloning any MessageProcessor instances.
 * 
 * Hopefully, most MessageProcessors are stateless except for the per instance state.
 * In some cases, however, there is shared static state for the MessageProcessor
 * instances.  In that case, this annotation guarantees that the method annotated will
 * be called only in the cluster where that MessageProcessor is used (thus it is an
 * improvement on using an <code>init-method</code> in Spring), and before dispatching
 * any messages (thus it is simpler than lazy initialization on the first message in that 
 * it avoids concurrency concerns in the application code.)
 *
 * Only one such method may be annotated with this Annotation per MessageProcessor
 * 
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Start {}
