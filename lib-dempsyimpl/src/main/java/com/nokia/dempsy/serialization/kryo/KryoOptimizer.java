package com.nokia.dempsy.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;

/**
 * If you want to optimize the Kryo performance you can implement this class
 * and give it to the {@link KryoSerializer}. It will be called for multiple
 * Kryo instances since the {@link KryoSerializer} creates an instance per 
 * thread and pools them. This will be called as part of the creation and
 * initialization of a Kryo instance.
 */
public interface KryoOptimizer
{
   /**
    * The implementor should provide whatever class specific optimizations they
    * would like to make. This will be called after any class registrations
    * the were provided to the {@link KryoSerializer} constructor are registered.
    */
   public void optimize(Kryo kryo);
}
