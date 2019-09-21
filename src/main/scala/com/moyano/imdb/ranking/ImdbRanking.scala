package com.moyano.imdb.ranking

import com.moyano.imdb.config.AppConfig
import com.moyano.imdb.model.schema.{CrewItem, MovieCrew, NameBasics, Principal, Rating, TitleAka, TitleBasics}
import com.moyano.imdb.model.{RankedMovie, TitlesAndPersons}
import com.moyano.imdb.utils.AppConstants
import com.moyano.imdb.utils.AppConstants._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

trait ImdbRankingProvider{
  def calculateBestRankedMovies(num:Int):Array[RankedMovie]
  def getTitlesAndPersons(bestRankedMoviesOpt:Option[Array[RankedMovie]], num:Int):Array[TitlesAndPersons]
}

/**
  * Class that holds the logic of the movie rankings and crew names retrieval
  * @param config the application configuration
  * @param session the spark session
  */
class ImdbRanking(config:AppConfig, session:SparkSession) extends ImdbRankingProvider {
  implicit val encoder = RowEncoder(CrewItem.schema)

  import session.implicits._

  def apply(session: SparkSession, config: AppConfig): ImdbRanking = new ImdbRanking(config, session)

  def calculateBestRankedMovies(numberOfResults:Int = AppResultSizeDefault):Array[RankedMovie] = {

    val titleBasics = loadAllMovies()

    val ratings = loadRatings()

    val titleWithRatings = titleBasics.as("t")
      .join(ratings.as("r"), titleBasics(TitleBasics.Id) === ratings(Rating.Id),"inner")
      .select(
        col("t.".concat(TitleBasics.Id)).as(TitleBasics.Id),
        col("t.".concat(TitleBasics.TitleType)).as(TitleBasics.TitleType),
        col("t.".concat(TitleBasics.PrimaryTitle)).as(TitleBasics.PrimaryTitle),
        col("r.".concat(Rating.AverageRating)).as(Rating.AverageRating),
        col("r.".concat(Rating.NumVotes)).as(Rating.NumVotes)
      )

    val avg = titleWithRatings.select(mean(Rating.NumVotes)).map(_.getDouble(0)).collect()(0)

    titleWithRatings
      .withColumn("averageNumberOfVotes", lit(avg))
      .filter(col(Rating.NumVotes) >= AppMinVotesNumber)
      .filter(col(Rating.AverageRating) > 0) // avoids 0 division
      .withColumn(RankedMovie.RankingScoreValue, (col(Rating.NumVotes)/$"averageNumberOfVotes") * col(Rating.AverageRating))
      .sort(desc(RankedMovie.RankingScoreValue))
      .take(numberOfResults)
      .zipWithIndex
      .map({
        case(row, index) => RankedMovie(index + 1,
          row.getAs[String](Rating.Id),
          row.getAs[String](TitleBasics.PrimaryTitle),
          row.getAs[Double](RankedMovie.RankingScoreValue))
      })
  }

  def getTitlesAndPersons(bestRankedMoviesOpt:Option[Array[RankedMovie]],
                          numberOfResults:Int = AppResultSizeDefault):Array[TitlesAndPersons] = {

    val bestRankedMovies: Array[RankedMovie] = bestRankedMoviesOpt.getOrElse(calculateBestRankedMovies(numberOfResults))

    val names = loadNames()
    val principals = loadPrincipal()

    val crewsWithNames:Array[CrewItem] =
      principals.filter(col(Principal.Id) isin (bestRankedMovies.map(_.id).toList:_*)).as("p")
        .join(names.as("n"), principals(Principal.CrewId) === names(NameBasics.Id), "inner")
      .select(
        col("p.".concat(Principal.Id)).as(CrewItem.Film),
        col("n.".concat(NameBasics.PrimaryName)).as(CrewItem.Crew))
      .map(CrewItem.fromRow(_))
      .collect()

    val movieTitles = loadMovieTitles()
      .filter(col(TitleAka.Id) isin (bestRankedMovies.map(_.id).toList:_*))
      .map(TitleAka.fromRow(_))
      .collect()

    bestRankedMovies.map(movie =>
      TitlesAndPersons(
        movie.name,
        movieTitles.filter(_.titleId == movie.id).map(_.title),
        crewsWithNames.filter(_.film == movie.id).map(_.crew)
      )
    )
  }

  /**
    * Load the movie titles
    * @return the movie titles dataframe
    */
  def loadMovieTitles():DataFrame = loadDataFrame(config.qualifiedTitleAkasPath(), TitleAka.schema)

  /**
    * Loads the ratings
    * @return the ratings dataframe
    */
  protected def loadRatings():DataFrame = loadDataFrame(config.qualifiedRatingsPath(), Rating.schema)

  /**
    * Loads the titles corresponding to movies
    * @return the movies dataframe
    */
  protected def loadAllMovies():DataFrame =
    loadDataFrame(config.qualifiedTitleBasicsPath(), TitleBasics.schema)
      .filter(col(TitleBasics.TitleType) === AppConstants.MovieType)

  /**
    * Loads the movie crew details
    * @return the movie crew details
    */
  protected def loadMovieCrews():DataFrame = loadDataFrame(config.qualifiedTitleCrewPath(), MovieCrew.schema)

  /**
    * Loads the movie crew details
    * @return the movie crew details dataframe
    */
  protected def loadNames():DataFrame =
    loadDataFrame(config.qualifiedNameBasicsPath(), NameBasics.schema)

  /**
    * Loads the movie crew principal details
    * @return the movie crew principal details dataframe
    */
  protected def loadPrincipal():DataFrame =
    loadDataFrame(config.qualifiedTitlePrincipalPath(), Principal.schema)

  /**
    * Generic method to load dataframes
    * @param fullPath the full path to the file to load as a dataframe
    * @param schema the schema to apply
    * @return the dataframe
    */
  private def loadDataFrame(fullPath:String, schema:StructType):DataFrame = {
    session.read.format("csv")
      .schema(schema)
      .option("header", "true")
      .option("delimiter", "\\t")
      .load(fullPath)
  }
}
