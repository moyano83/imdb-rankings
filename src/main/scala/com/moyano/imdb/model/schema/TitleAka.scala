package com.moyano.imdb.model.schema

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}

object TitleAka{

  val Id = "titleId"
  val Ordering = "ordering"
  val Title = "title"
  val Region = "region"
  val Language = "language"
  val Types = "types"
  val Attributes = "attributes"
  val OriginalTitle = "isOriginalTitle"

  val schema = StructType(
    Seq(
      StructField(Id, StringType, false),
      StructField(Ordering, StringType, false),
      StructField(Title, StringType, false),
      StructField(Region, StringType, false),
      StructField(Language, StringType, false),
      StructField(Types, StringType, false),
      StructField(Attributes, StringType, false),
      StructField(OriginalTitle, StringType, false)
    )
  )

  def fromRow(row:Row):TitleAka = {
    TitleAka(
      row.getAs[String](Id),
      row.getAs[String](Ordering),
      row.getAs[String](Title),
      row.getAs[String](Region),
      row.getAs[String](Language),
      row.getAs[String](Types),
      row.getAs[String](Attributes),
      row.getAs[String](OriginalTitle)
    )
  }
}


case class TitleAka (titleId:String,
                      ordering:String,
                      title:String,
                      region:String,
                      language:String,
                      types:String,
                      attributes:String,
                      isOriginalTitle:String)
