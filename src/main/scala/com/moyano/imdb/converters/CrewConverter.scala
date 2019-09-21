package com.moyano.imdb.converters

import com.moyano.imdb.model.schema.{CrewItem, MovieCrew}
import com.moyano.imdb.utils.AppConstants
import org.apache.spark.sql.Row

object CrewConverter {
  def itemizeMovieCrewRowToCrewItemRow(row:Row):Array[Row] =
    (itemize(row.getAs[String](MovieCrew.Directors)) ++ itemize(row.getAs[String](MovieCrew.Writers)))
      .map(idCrew => Row(row.getAs[String](MovieCrew.Id), idCrew, CrewItem.schema))

  private def itemize(itemsInString:String):Array[String] =
    if(itemsInString == AppConstants.NullMarker) Array.empty[String]
    else itemsInString.split(AppConstants.ItemSeparator)
}
