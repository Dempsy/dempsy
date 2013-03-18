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
   
   @Override
   public int hashCode()
   {
      return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode());
   }
   
   @Override
   public boolean equals(Object obj)
   {
      if(this == obj) return true;
      if(obj == null) return false;
      if(getClass() != obj.getClass()) return false;
      @SuppressWarnings("unchecked")
      Pair<F,S> other = (Pair<F,S>)obj;
      if(first == null && other.first != null)
         return false;
      else if(!first.equals(other.first))
         return false;
      if(second == null && other.second != null)
         return false;
      else if(!second.equals(other.second))
         return false;
      return true;
   }

}
