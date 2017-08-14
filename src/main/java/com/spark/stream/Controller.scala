package com.spark.stream

import com.google.gson.JsonObject
import com.google.gson.JsonElement

class  Controller extends WareHouse {
 
  def lookUp:JsonObject= {
    val x:JsonObject=new JsonObject
    x.addProperty("clientId", "cid")
    x
  }  
  var dataValue = data
  
  def myDat(js:List[JsonObject])={
    
  }
  
}