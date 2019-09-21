package com.moyano.imdb.model.schema

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Rating{
  val Id = "tconst"
  val AverageRating = "averageRating"
  val NumVotes = "numVotes"

  val schema = StructType(
    Seq(
      StructField(Id, StringType, false),
      StructField(AverageRating, DoubleType, false),
      StructField(NumVotes, IntegerType, false)
    )
  )
}

case class Rating(tconst:String, averageRating:Double, numVotes:Int)