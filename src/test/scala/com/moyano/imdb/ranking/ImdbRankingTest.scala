package com.moyano.imdb.ranking

import com.moyano.imdb.config.AppConfig
import com.moyano.imdb.model.{RankedMovie, TitlesAndPersons}
import com.moyano.imdb.model.schema.{MovieCrew, NameBasics, Principal, Rating, TitleAka, TitleBasics}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FlatSpec
import org.scalatest._


class ImdbRankingTest extends FlatSpec with Matchers {

  val session = SparkSession.builder().master("local[1]").appName("test").getOrCreate()

  import session.implicits._
  val config = AppConfig(
    "local[1]",
    "/Users/jorge/Downloads",
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
  )

  class TestImdb extends ImdbRanking(config, session) {
    override def loadRatings(): DataFrame =
      session.sparkContext.parallelize(
        List(
          Rating("1", 5.0D, 100),
          Rating("2", 7.0D, 600),
          Rating("3", 3.0D, 10),
          Rating("4", 2.0D, 90))
      ).toDF

    override def loadMovieTitles():DataFrame =
      session.sparkContext.parallelize(
        List(
          TitleAka("1", 1, "MatrixEN", "EN", "EN", "", "",  false),
          TitleAka("1", 2, "MatrixES", "ES", "ES", "", "",  false),
          TitleAka("2", 1, "HeatEN", "EN", "EN", "", "",  false),
          TitleAka("2", 2, "HeatES", "EN", "EN", "", "",  false),
          TitleAka("3", 1, "AmelieEN", "EN", "EN", "", "",  false),
          TitleAka("4", 1, "ShrekEN", "EN", "EN", "", "",  false)
        )).toDF

    override def loadAllMovies(): DataFrame = session.sparkContext.parallelize(
      List(
        TitleBasics("1", "movie", "Matrix", "Matrix", false, "2000", "3000", "120", "action"),
        TitleBasics("2", "movie", "Heat", "Heat", false, "2000", "3000", "120", "action"),
        TitleBasics("3", "movie", "Amelie", "Matrix", false, "2000", "3000", "120", "romantic"),
        TitleBasics("4", "movie", "Shrek", "Matrix", false, "2000", "3000", "120", "comedy"),
        TitleBasics("5", "serie", "A Team", "A Team", false, "2000", "3000", "120", "action")
      )).toDF

    override def loadPrincipal(): DataFrame = session.sparkContext.parallelize(
      List(
        Principal("1", 1, "1", "writer", "",""),
        Principal("1", 1, "2", "writer", "",""),
        Principal("2", 1, "3", "writer", "",""),
        Principal("2", 1, "4", "writer", "",""),
        Principal("3", 1, "3", "writer", "",""),
        Principal("4", 1, "2", "writer", "","")
      )).toDF

    override def loadNames():DataFrame = session.sparkContext.parallelize(
      List(
        NameBasics("1", "Name1","","","",""),
        NameBasics("2", "Name2","","","",""),
        NameBasics("3", "Name3","","","",""),
        NameBasics("4", "Name4","","","",""),
        NameBasics("5", "Name5","","","",""),
        NameBasics("6", "Name6","","","","")
      )).toDF
  }

  val Id ="tconst"
  val Ordering = "ordering"
  val CrewId = "nconst"
  val Category = "category"
  val Job = "job"
  val Characters = "characters"
  val imdb = new TestImdb
  val imdbReal = new ImdbRanking(config, session)

  val expectedBestRankedMovieResult = Array(
    RankedMovie(1, "2", "Heat", 21.0),
    RankedMovie(2, "1", "Matrix", 2.5),
    RankedMovie(3, "4", "Shrek", 0.9)
  )

  "The App " should "calculate the best ranked movies in" in {
    imdb.calculateBestRankedMovies(10) should be(expectedBestRankedMovieResult)
  }

  "The App " should "get the crew names and titles of the films" in {
    val expected = Array(
      TitlesAndPersons(
        "Heat",
        Array("HeatEN","HeatES"),
        Array("Name3", "Name4")
      ),
      TitlesAndPersons(
        "Matrix",
        Array("MatrixEN","MatrixES"),
        Array("Name1", "Name2")
      ),
      TitlesAndPersons(
        "Shrek",
        Array("ShrekEN"),
        Array("Name2")
      )
    )
    imdbReal.getTitlesAndPersons(None).foreach(println(_))
    //imdb.getTitlesAndPersons(None,3) should be(expected)
  }

}
