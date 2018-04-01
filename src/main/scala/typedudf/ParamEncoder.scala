package typedudf

import java.sql.{ Date, Timestamp }
import org.apache.spark.sql.Row
import scala.collection.generic.{ CanBuildFrom, IsTraversableLike }
import shapeless._
import shapeless.labelled._

@annotation.implicitNotFound("""
  Type ${T} does not have a ParamEncoder defined in the library.
  You need to define one yourself.
  """)
trait ParamEncoder[T] {
  type In
  def apply(v: In): T
}

trait ParamEncoderImpl {
  type Aux[T, In0] = ParamEncoder[T] { type In = In0 }

  def apply[T](implicit paramEncoder: ParamEncoder[T]): Aux[T, paramEncoder.In] = paramEncoder

  def identityEncoder[T]: Aux[T, T] = new ParamEncoder[T] {
    type In = T
    def apply(v: In) = v
  }

  implicit val hnilEncoder: Aux[HNil, Row] = new ParamEncoder[HNil] {
    type In = Row
    def apply(row: In): HNil = HNil
  }

  implicit def hconsEncoder[K <: Symbol, V, VIn, T <: HList](
    implicit
      w: Witness.Aux[K],
      vEncoder: ParamEncoder.Aux[V, VIn],
      tEncoder: ParamEncoder.Aux[T, Row]
  ): Aux[FieldType[K, V] :: T, Row] = new ParamEncoder[FieldType[K, V] :: T] {
    type In = Row
    def apply(row: In) = {
      val key = w.value
      val value = row.getAs[VIn](key.name)
      field[K](vEncoder(value)) :: tEncoder(row)
    }
  }

  implicit def productEncoder[P, L <: HList](
    implicit
      lgen: LabelledGeneric.Aux[P, L],
      encoder: ParamEncoder.Aux[L, Row]
  ): Aux[P, Row] = new ParamEncoder[P] {
    type In = Row
    def apply(row: In) = lgen.from(encoder(row))
  }

  implicit def binaryEncoder = identityEncoder[Array[Byte]]
  implicit def booleanEncoder = identityEncoder[Boolean]
  implicit def byteEncoder = identityEncoder[Byte]
  implicit def bigDecimalEncoder = identityEncoder[BigDecimal]
  implicit def doubleEncoder = identityEncoder[Double]
  implicit def floatEncoder = identityEncoder[Float]
  implicit def intEncoder = identityEncoder[Int]
  implicit def longEncoder = identityEncoder[Long]
  implicit def unitEncoder = identityEncoder[Unit]
  implicit def shortEncoder = identityEncoder[Short]
  implicit def stringEncoder = identityEncoder[String]
  implicit def timestampEncoder = identityEncoder[Timestamp]
  implicit def dateEncoder = identityEncoder[Date]

  implicit def traversableLikeEncoder[V, VIn, C[_]](
    implicit
      encoder: ParamEncoder.Aux[V, VIn],
      is: IsTraversableLike[C[VIn]] { type A = VIn },
      bf: CanBuildFrom[C[VIn], V, C[V]]): Aux[C[V], C[VIn]] = new ParamEncoder[C[V]] {
    type In = C[VIn]
    def apply(s: In) = is.conversion(s).map(encoder.apply)
  }

  implicit def optionEncoder[V, VIn](
    implicit
      encoder: ParamEncoder.Aux[V, VIn]): Aux[Option[V], VIn] = new ParamEncoder[Option[V]] {
    type In = VIn
    def apply(s: In): Option[V] = Option(encoder(s))
  }

  implicit def mapEncoder[K, KIn, V, VIn](
    implicit
      kEncoder: ParamEncoder.Aux[K, KIn],
      vEncoder: ParamEncoder.Aux[V, VIn]): Aux[Map[K, V], Map[KIn, VIn]] = new ParamEncoder[Map[K, V]] {
    type In = Map[KIn, VIn]
    def apply(m: In): Map[K, V] = m.map { case (k, v) => (kEncoder(k), vEncoder(v)) }
  }
}

object ParamEncoder extends ParamEncoderImpl
