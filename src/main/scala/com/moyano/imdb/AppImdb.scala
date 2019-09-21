package com.moyano.imdb

import com.moyano.imdb.config.AppConfig
import com.moyano.imdb.ranking.{ImdbRanking, ImdbRankingProvider}
import com.moyano.imdb.config.PropertiesReader
import com.moyano.imdb.model.{RankedMovie, TitlesAndPersons}
import com.moyano.imdb.utils.AppConstants
import org.apache.spark.sql.SparkSession

object AppImdb {

    def main(args:Array[String]):Unit = {
        val config = loadConfig(args)
        val session = SparkSession.builder().master(config.master).enableHiveSupport().getOrCreate()
        val rankingProvider: ImdbRankingProvider = new ImdbRanking(config,session)

        val movies = rankingProvider.calculateBestRankedMovies(AppConstants.AppResultSizeDefault)

        val cast = rankingProvider.getTitlesAndPersons(Some(movies), AppConstants.AppResultSizeDefault)

        printResults(movies, cast)
    }

    def printResults(movies:Array[RankedMovie], cast:Array[TitlesAndPersons]) = {
        println("Retrieval of the top 20 movies with a minimum of 50 votes by ranking ")
        movies.foreach(println(_))

        println("persons who are most often credited and list the different titles of the most popular movies")
        cast.foreach(println(_))
    }


    def loadConfig(args:Array[String]): AppConfig= {
        require(args.size == 1, "Invalid number of parameters")
        PropertiesReader.read(args(0))
    }

}