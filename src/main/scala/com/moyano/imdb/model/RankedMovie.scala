package com.moyano.imdb.model

object RankedMovie{
  val Position = "position"
  val Id = "id"
  val RankingScoreValue = "rankingScoreValue"
}

case class RankedMovie(position:Int, id:String, name:String, score:Double){
  @Override
  override def toString: String =
    s"""
      | BestMovie: $name
      |   Position: $position
      |   idbm Id: $id
      |   score: $score
    """.stripMargin
}
