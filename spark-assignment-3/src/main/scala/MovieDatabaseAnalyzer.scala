import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{avg, col, corr, count, desc, expr, isnan, when, stddev}
import scala.util._


case class MovieDatabaseAnalyzer() {

  def parse(resource: String): Try[DataFrame] = Try {
    val spark = SparkSession
      .builder()
      .appName("Analyzing Movie Rating")
      .config("spark.master", "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val moviesdf = spark.read.option("header", "true").csv(resource)
    moviesdf
  }

  def calculateMovieRatingMean(moviedf: DataFrame): DataFrame = moviedf.agg(avg("imdb_score").alias("Mean"))

  def calculateMovieRatingStdDev(moviedf: DataFrame): DataFrame = moviedf.agg(stddev("imdb_score").alias("Std Deviation"))

}

object MovieDatabaseAnalyzer extends App {
  val ma: MovieDatabaseAnalyzer = new MovieDatabaseAnalyzer()
  val result = ma.parse("src/main/resources/movie_metadata.csv") // from the one in the repository: https://github.com/rchillyard/CSYE7200/blob/Spring2022/spark-csv/src/main/resources/movie_metadata.csv
  result match {
    case Success(dframe) =>
      // DataFrame was successfully loaded
      ma.calculateMovieRatingMean(dframe).show()
      ma.calculateMovieRatingStdDev(dframe).show()
    case Failure(exception) =>
      println(s"Failed to parse DataFrame: ${exception.getMessage}")
  }
}