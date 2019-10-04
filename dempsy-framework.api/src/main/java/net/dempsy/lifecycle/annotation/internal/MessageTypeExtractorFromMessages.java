package net.dempsy.lifecycle.annotation.internal;

/**
 * This class is used to extract MessageType information from messages.
 */
public class MessageTypeExtractorFromMessages {
    public final String[] classDefinedMessageTypes;

    public MessageTypeExtractorFromMessages(final Class<?> messageClass) throws IllegalStateException {
        // search for the MessageType annotation on the class
        classDefinedMessageTypes = MessageUtils.getAllMessageTypeTypeAnnotationValues(messageClass, true);

    }

    public String[] get(final Object message) {
        return doIt(message, false);
    }

    public String[] getThrows(final Object message) {
        return doIt(message, true);
    }

    private String[] doIt(final Object message, final boolean iThrow) {
        return classDefinedMessageTypes;
    }

}
