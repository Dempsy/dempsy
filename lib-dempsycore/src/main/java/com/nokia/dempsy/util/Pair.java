package com.nokia.dempsy.util;

/**
 * A generic immutable tuple that holds two values.
 */
public final class Pair<F,S>
{
   private final F first;
   private final S second;
   
   public Pair(F first, S second) { this.first = first; this.second = second; }
   public Pair(Pair<F,S> other)
   {
      this.first = other.first;
      this.second = other.second;
   }

   public final F getFirst() {  return first; }
   public final S getSecond() { return second; }
   
   @Override
   public String toString() { return "[" + getFirst() + "," + getSecond() + "]"; }
}
