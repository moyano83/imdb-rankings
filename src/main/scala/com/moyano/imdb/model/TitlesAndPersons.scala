package com.moyano.imdb.model

case class TitlesAndPersons(name:String, movieTitles:Array[String], crew:Array[String]){

  override def toString: String =
    s"""
      | MovieName: $name
      |     AlternativeTitles: [${movieTitles.mkString(",")}]
      |     CrewNames: [${crew.mkString(",")}]
    """.stripMargin
}
