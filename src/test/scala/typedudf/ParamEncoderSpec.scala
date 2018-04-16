package typedudf

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest._
import ParamEncoder._

class ParamEncoderSpec extends FlatSpec with Matchers {
  "A ParamEncoder" should "encode product type" in {
    case class Foo(x: Int, y: Set[Int])
    val schema = StructType(
      StructField("x", IntegerType, false) ::
      StructField("y", ArrayType(IntegerType, false), false) ::
      Nil
    )
    val row = new GenericRowWithSchema(Array(1, Set(2, 3)), schema)
    ParamEncoder[Foo].apply(row) shouldEqual Foo(1, Set(2, 3))
  }

  "A ParamEncoder" should "encode deep nested product type" in {
    ParamEncoder[(Int, (Int, (Int, Int)))]
    ParamEncoder[(Int, (Int, Int, Int))]
  }
}
