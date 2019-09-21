package com.moyano.imdb.config

import java.io.{File, FileInputStream}

import java.util.Properties
import com.moyano.imdb.utils.AppConstants.AppProperties._

object PropertiesReader {
  /**
    * Reads the config file into an AppConfig object
    * @param path the path to the config file
    * @return the AppConfig object
    */
  def read(path:String):AppConfig = {
    val prop = new Properties()
    val fileInputStream = new FileInputStream(new File(path))

    prop.load(fileInputStream)
    fileInputStream.close()
    AppConfig(
      readProperty(prop, Master),
      readProperty(prop, BasePath),
      readProperty(prop, NameBasics),
      readProperty(prop, TitleAkas),
      readProperty(prop, TitleBasics),
      readProperty(prop, TitleCrew),
      readProperty(prop, TitleEpisode),
      readProperty(prop, TitlePrincipal),
      readProperty(prop, Ratings)
    )  
  }
  
  private def readProperty(props:Properties, name:String, required:Boolean = true): String ={
    Option(props.getProperty(name)).getOrElse(
      if(required) throw new IllegalArgumentException("Unable to find property ".concat(name)) else ""
    )
  } 
}
