package net.dempsy.lifecycle.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Declares the message type identifiers that a message class represents. The framework uses these
 * identifiers to route messages to the correct {@link Mp}-annotated processor cluster.
 *
 * <p>When placed on a message class, the {@link #value()} array specifies one or more type strings.
 * If no value is provided, the framework derives the type from the fully qualified class name.</p>
 *
 * <p>A single message class may carry multiple type identifiers, enabling it to be routed to
 * multiple clusters simultaneously.</p>
 */
@Retention(RUNTIME)
@Target({TYPE})
public @interface MessageType {
    String[] value() default {};
}
