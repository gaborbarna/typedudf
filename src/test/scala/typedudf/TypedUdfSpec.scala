package typedudf

import scala.collection.mutable.WrappedArray
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

  "it" should "work with non-product types" in {
    import spark.implicits._
    val ds = spark.createDataset(Seq(1, 2)).toDF("x")
    val plusOneUdf = TypedUdf((x: Int) => x + 1)
    val result = ds.withColumn("y", plusOneUdf($"x")).select("y").as[Int].collect
    result shouldEqual Seq(2, 3)
  }

  "it" should "work with seq type" in {
    import spark.implicits._
    val ds = spark.createDataset(Seq(Seq(1, 2), Seq(3, 4))).toDF("x")
    val seqUdf = TypedUdf((x: Seq[Int]) => x.map(_ * 2))
    val result = ds.withColumn("y", seqUdf($"x")).select("y").as[Seq[Int]].collect
    result shouldEqual Seq(Seq(2, 4), Seq(6, 8))
  }

  "it" should "work with Array[Byte] type" in {
    import spark.implicits._
    val ds = spark.createDataset(Seq(Array(1, 2).map(_.toByte))).toDF("x")
    val arrUdf = TypedUdf((x: Array[Byte]) => x.reverse)
    val result = ds.withColumn("y", arrUdf($"x")).select("y").as[Array[Byte]].collect
    result shouldEqual Array(Array(2, 1).map(_.toByte))
  }

  "it" should "work with WrappedArray type" in {
    import spark.implicits._
    val ds = spark.createDataset(Seq(Seq(1, 1, 2))).toDF("x")
    val arrUdf = TypedUdf((x: WrappedArray[Int]) => x.distinct)
    val result = ds.withColumn("y", arrUdf($"x")).select("y").as[Seq[Int]].collect
    result shouldEqual Seq(Seq(1, 2))
  }

  "it" should "work with option type" in {
    import spark.implicits._
    val ds = spark.createDataset(Seq(Some(1 -> 2) -> 3, None -> 3)).select(struct("*").as("x"))
    val optUdf = TypedUdf((x: (Option[(Int, Int)], Int)) => x._1.map(_._1 * x._2))
    val result = ds.select(optUdf($"x")).as[Option[Int]].collect
    result shouldEqual Seq(Some(3), None)
  }

}

object TypedUdfSpec {
  case class Foo(x: Boolean, y: String)
  case class Bar(a: String, b: Option[Int])
}
