package com.nokia.dempsy.serialization.kryo;

/**
 * This class holds Kryo class registration information. You can assign an ID to
 * a class for optimized Kryo serialization.
 */
public class Registration
{
   protected String classname;
   protected int id;
   
   /**
    * Use this constructor to assign a class an id in a Kryo registration.
    */
   public Registration(String classname, int id)
   {
      this.classname = classname;
      this.id = id;
   }
   
   /**
    * Use this constructor to register a class but let Kryo assign the id. This implies
    * an "order based" registration and the serialization and deserialization need to 
    * register classes with Kryo in the same order.
    */
   public Registration(String classname)
   {
      this.classname = classname;
      this.id = -1;
   }
   
   public String getClassname() {  return classname; }

   public int getId(){ return id; }
}
