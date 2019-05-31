package net.dempsy.transport.tcp;

import java.net.InetAddress;
import java.util.function.Supplier;

import net.dempsy.Infrastructure;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.Receiver;

public abstract class AbstractTcpReceiver<A extends TcpAddress, T extends AbstractTcpReceiver<A, ?>> implements Receiver {
    public final static int DEFAULT_MAX_MESSAGE_SIZE_BYTES = 1024 * 1024;
    protected final Serializer serializer;

    protected int internalPort;
    protected boolean useLocalHost = false;
    protected TcpAddressResolver<A> resolver = a -> a;
    protected final String serId;
    protected int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE_BYTES;
    protected Supplier<InetAddress> addrSupplier = null;

    public AbstractTcpReceiver(final Serializer serializer, final int port) {
        this.internalPort = port;
        this.serializer = serializer;
        this.serId = serializer.getClass().getPackage().getName();
    }

    public AbstractTcpReceiver(final Serializer serializer) {
        this(serializer, -1);
    }

    @SuppressWarnings("unchecked")
    public T useLocalHost(final boolean useLocalHost) {
        this.useLocalHost = useLocalHost;
        return (T)this;
    }

    @SuppressWarnings("unchecked")
    public T addressSupplier(final Supplier<InetAddress> addrSupplier) {
        this.addrSupplier = addrSupplier;
        return (T)this;
    }

    @SuppressWarnings("unchecked")
    public T resolver(final TcpAddressResolver<A> resolver) {
        this.resolver = resolver;
        return (T)this;
    }

    public AbstractTcpReceiver<A, T> maxMessageSize(final int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
        return this;
    }

    @Override
    public abstract TcpAddress getAddress(Infrastructure infra);

    public abstract AbstractTcpReceiver<A, T> numHandlers(int numHandlerThreads);

    // =============================================================================
    // These methods are to support spring dependency injection which (stupidly) requires
    // adherence to a 15 year old JavaBeans spec.
    // =============================================================================
    public void setUseLocalHost(final boolean useLocalHost) {
        useLocalHost(useLocalHost);
    }

    public boolean getUseLocalHost() {
        return useLocalHost;
    }

    public void setAddressSupplier(final Supplier<InetAddress> addrSupplier) {
        addressSupplier(addrSupplier);
    }

    public final Supplier<InetAddress> getAddressSupplier() {
        return addrSupplier;
    }

    public void setResolver(final TcpAddressResolver<A> resolver) {
        resolver(resolver);
    }

    public TcpAddressResolver<A> getResolver() {
        return resolver;
    }

    public void setMaxMessageSize(final int maxMessageSize) {
        maxMessageSize(maxMessageSize);
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }
    // =============================================================================

}
