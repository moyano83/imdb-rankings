package com.moyano.imdb.model.schema

import org.apache.spark.sql.types.{ArrayType, BooleanType, StringType, StructField, StructType}


object TitleBasics {

  val Id = "tconst"
  val TitleType = "titleType"
  val PrimaryTitle = "primaryTitle"
  val OriginalTitle = "originalTitle"
  val Adult = "isAdult"
  val StartYear = "startYear"
  val EndYear = "endYear"
  val RuntimeMinutes = "runtimeMinutes"
  val Genres = "genres"

  val schema = StructType(
    Seq(
      StructField(Id, StringType, false),
      StructField(TitleType, StringType, false),
      StructField(PrimaryTitle, StringType, false),
      StructField(OriginalTitle, StringType, true),
      StructField(Adult, BooleanType, true),
      StructField(StartYear, StringType, false),
      StructField(EndYear, StringType, false),
      StructField(RuntimeMinutes, StringType, false),
      StructField(Genres, StringType, false)
    )
  )
}

case class TitleBasics(tconst:String,
                       titleType:String,
                       primaryTitle:String,
                       originalTitle:String,
                       isAdult:Boolean,
                       startYear:String,
                       endYear:String,
                       runtimeMinutes: String,
                       genres:String);