package typedudf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import ParamEncoder._

class TypedUdfSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  import TypedUdfSpec._

  var _spark: SparkSession = _
  lazy val spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    _spark = SparkSession.builder
      .appName("typedudf-test")
      .master("local")
      .getOrCreate()
    _spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop

  "it" should "execute the typed udf" in {
    import spark.implicits._
    val ds = spark.createDataset(Seq(Foo(true, "asd"))).select(struct("*").as("foo"))
    val fooToBar = TypedUdf((foo: Foo) => Bar(foo.y, Some(foo.y.length)))
    val bar = ds.withColumn("bar", fooToBar($"foo")).select("bar.*").as[Bar].head
    bar shouldEqual Bar("asd", Some(3))
  }
}

object TypedUdfSpec {
  case class Foo(x: Boolean, y: String)
  case class Bar(a: String, b: Option[Int])
}
