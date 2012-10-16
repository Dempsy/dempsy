package com.nokia.dempsy.serialization.kryo;

public class Registration
{
   protected String classname;
   protected int id;
   
   public Registration(String classname, int id)
   {
      this.classname = classname;
      this.id = id;
   }
   
   public Registration()
   {
      this.classname= null;
      this.id = 0;
   }
   
   public String getClassname() {  return classname; }

   public void setClassname(String classname) {  this.classname = classname;  }

   public int getId(){ return id; }

   public void setId(int id) {  this.id = id; }
}
