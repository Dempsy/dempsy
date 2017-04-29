package net.dempsy.container;

import net.dempsy.DempsyException;

public class ContainerException extends DempsyException {

    private static final long serialVersionUID = 1L;

    public ContainerException(final String message) {
        super(message);
    }

    public ContainerException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
