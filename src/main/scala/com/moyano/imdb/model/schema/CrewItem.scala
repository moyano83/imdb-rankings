package com.moyano.imdb.model.schema

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CrewItem{
  val Film ="filmId"
  val Crew = "crew"

  val schema = StructType(
    Seq(
      StructField(Film, StringType, false),
      StructField(Crew, StringType, false)
    )
  )

  def fromRow(row:Row):CrewItem = CrewItem(row.getAs[String](Film), row.getAs[String](Crew))
}

case class CrewItem(film:String, crew:String)
