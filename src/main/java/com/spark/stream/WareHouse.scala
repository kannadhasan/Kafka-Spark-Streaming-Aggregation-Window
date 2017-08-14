package com.spark.stream

import com.google.gson.JsonObject

trait WareHouse {
  def lookUp:JsonObject
  var data = "new"
}