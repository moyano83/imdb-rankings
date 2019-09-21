package com.moyano.imdb.config

import java.io.File

case class AppConfig(master:String,
                     basePath: String,
                     nameBasics:String = "name.basics.tsv.gz",
                     titleAkas:String ="title.akas.tsv.gz",
                     titleBasics:String = "title.basics.tsv.gz",
                     titleCrew:String ="title.crew.tsv.gz",
                     titleEpisode:String ="title.episode.tsv.gz",
                     titlePrincipal:String ="title.principals.tsv.gz",
                     ratings:String ="title.ratings.tsv.gz"){

  private def basePathWithProperty(property:String):String = basePath.concat(File.separator).concat(property)

  def qualifiedTitleAkasPath():String = basePathWithProperty(titleAkas)
  def qualifiedTitleBasicsPath():String = basePathWithProperty(titleBasics)
  def qualifiedTitleCrewPath():String = basePathWithProperty(titleCrew)
  def qualifiedRatingsPath():String = basePathWithProperty(ratings)
  def qualifiedNameBasicsPath():String = basePathWithProperty(nameBasics)
  def qualifiedTitlePrincipalPath():String = basePathWithProperty(titlePrincipal)

}
