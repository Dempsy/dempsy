package net.dempsy.lifecycle.simple;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public interface MpFactory<T extends Mp> extends Supplier<T> {

    public Set<String> messageTypesHandled();

    /**
     * The default setting for isEvictable is <code>false</code>. If your {@link Mp} implementation actually
     * implements {@link Mp#shouldBeEvicted()} then you should override this
     * method to return <code>true</code> or Dempsy will never call {@link Mp#shouldBeEvicted()}.
     */
    public default boolean isEvictionSupported() {
        return false;
    }

    /**
     * The default setting for hasOutput is <code>false</code>. If your {@link Mp} implementation actually
     * implements {@link Mp#output()} method then you should override this method to return <code>true</code>
     * or Dempsy will never call {@link Mp#output()}.
     */
    public default boolean isOutputSupported() {
        return false;
    }

    /**
     * You can override this method to be notified when Dempsy is ready to start the
     * message processor.
     */
    public default void start() {}

    /**
     * You can override this method to be notified when Dempsy wants to validate the
     * message processor configuration. This happens right after the configuration
     * and before starting. This should throw an IllegalStateException if the mp's configuration
     * is invalid.
     */
    public default void validate() throws IllegalStateException {}

    /**
     * A convenience method for using a supplier as an MpFactory and taking all of the
     * defaults.
     */
    public static <R extends Mp> MpFactory<R> make(final Supplier<R> supplier, final String... messageTypes) {
        return new MpFactory<R>() {
            Set<String> mts = new HashSet<>(Arrays.asList(messageTypes));

            @Override
            public R get() {
                return supplier.get();
            }

            @Override
            public Set<String> messageTypesHandled() {
                return mts;
            }
        };
    }

}
