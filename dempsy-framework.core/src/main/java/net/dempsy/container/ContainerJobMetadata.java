package net.dempsy.container;

import net.dempsy.container.Container.ContainerSpecific;

public final class ContainerJobMetadata {
    public final Container c;
    public final ContainerSpecific p;

    public ContainerJobMetadata(final Container c, final ContainerSpecific p) {
        this.c = c;
        this.p = p;
    }
}
