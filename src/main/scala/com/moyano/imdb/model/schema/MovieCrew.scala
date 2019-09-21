package com.moyano.imdb.model.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object MovieCrew{
  val Id ="tconst"
  val Directors = "directors"
  val Writers = "writers"

  val schema = StructType(
    Seq(
      StructField(Id, StringType, false),
      StructField(Directors, StringType, false),
      StructField(Writers, StringType, false)
    )
  )
}

case class MovieCrew(tconst:String, directors:String, writers:String) {

}
