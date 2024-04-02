import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


import scala.util.Try

class DatabaseTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("MovieDatabaseAnalyzer")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  behavior of "parseResource"
  it should "get movie_metadata.csv" in {

    val ma: MovieDatabaseAnalyzer = new MovieDatabaseAnalyzer()

    import spark.implicits._

    val mdf: Try[DataFrame] = ma.parse("src/main/resources/movie_metadata.csv")
    mdf.isSuccess shouldBe true
    mdf foreach {
      d =>
        d.count() shouldBe 1609
        d.show(10)
    }

  }

  behavior of "calculateMovieRatingMean and calculateMovieRatingStdDev"
  it should "get mean and stddev" in {

    val ma: MovieDatabaseAnalyzer = new MovieDatabaseAnalyzer()

    import spark.implicits._

    val mdf: Try[DataFrame] = ma.parse("src/main/resources/movie_metadata.csv")
    mdf.isSuccess shouldBe true
    mdf foreach {
      d =>
        ma.calculateMovieRatingMean(d).first().getDouble(0) shouldBe 6.45 +- 0.01
        ma.calculateMovieRatingStdDev(d).first().getDouble(0) shouldBe 0.99 +- 0.01

    }

  }

}