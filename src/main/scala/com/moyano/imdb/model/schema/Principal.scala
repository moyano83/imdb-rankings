package com.moyano.imdb.model.schema

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Principal{
  val Id ="tconst"
  val Ordering = "ordering"
  val CrewId = "nconst"
  val Category = "category"
  val Job = "job"
  val Characters = "characters"

  val schema = StructType(
    Seq(
      StructField(Id, StringType, false),
      StructField(Ordering, IntegerType, false),
      StructField(CrewId, StringType, false),
      StructField(Category, StringType, false),
      StructField(Job, StringType, false),
      StructField(Characters, StringType, false)
    )
  )

  def fromRow(row:Row):Principal = Principal(
      row.getAs[String](Id),
      row.getAs[Int](Ordering),
      row.getAs[String](CrewId),
      row.getAs[String](Category),
      row.getAs[String](Job),
      row.getAs[String](Characters)
    )

}

case class Principal(tconst:String, ordering:Int, nconst:String, category:String, job:String, characters:String)
