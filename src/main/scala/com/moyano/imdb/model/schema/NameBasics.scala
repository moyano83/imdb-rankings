package com.moyano.imdb.model.schema

import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object NameBasics {

  val Id = "nconst"
  val PrimaryName = "primaryName"
  val BirthYear = "birthYear"
  val DeathYear = "deathYear"
  val PrimaryProfession = "primaryProfession"
  val KnownForTitles = "knownForTitles"

  val schema = StructType(
    Seq(
      StructField(Id, StringType, false),
      StructField(PrimaryName, StringType, false),
      StructField(BirthYear, StringType, false),
      StructField(DeathYear, StringType, true),
      StructField(PrimaryProfession, StringType, false),
      StructField(KnownForTitles, StringType, false)
    )
  )
}

case class NameBasics(nconst:String,
                      primaryName:String,
                      birthYear:String,
                      deathYear:String,
                      primaryProfession:String,
                      knownForTitles:String);