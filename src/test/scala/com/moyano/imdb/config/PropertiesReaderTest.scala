package com.moyano.imdb.config

import java.io.File

import org.scalatest.{FlatSpec, Matchers}

class PropertiesReaderTest extends FlatSpec with Matchers {

  behavior of "PropertiesReader"

  val basePath = getClass().getClassLoader.getResource(".").getPath

  it should "read the properties from a file" in {
    PropertiesReader.read(basePath.concat(File.separator).concat("app.properties")) should be(
      AppConfig(
        "local[1]",
        "/tmp",
        "name.basics.tsv.gz",
        "title.akas.tsv.gz",
        "title.basics.tsv.gz",
        "title.crew.tsv.gz",
        "title.episode.tsv.gz",
        "title.principals.tsv.gz",
        "title.ratings.tsv.gz"
      )
    )
  }

  it should "throw an Exception if a mandatory property is not present" in {
    a [IllegalArgumentException] should be thrownBy{
      PropertiesReader.read(basePath.concat(File.separator).concat("app-wrong.properties"))
    }
  }

}
