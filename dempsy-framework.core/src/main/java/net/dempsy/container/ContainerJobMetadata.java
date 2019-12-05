package net.dempsy.container;

import net.dempsy.container.Container.ContainerSpecific;

public final class ContainerJobMetadata {
    public final Container container;
    public final ContainerSpecific containerSpecificData;

    public ContainerJobMetadata(final Container c, final ContainerSpecific p) {
        this.container = c;
        this.containerSpecificData = p;
    }
}
