package typedudf

import org.apache.spark.sql.functions.udf
import scala.reflect.runtime.universe.TypeTag

object TypedUdf {
  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag](f: A0 => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In]) =
    udf((a0: A0In) => f(a0Enc(a0)))

  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag,
    A1, A1In: TypeTag](f: (A0, A1) => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In],
      a1Enc: ParamEncoder.Aux[A1, A1In]) =
    udf((a0: A0In, a1: A1In) => f(a0Enc(a0), a1Enc(a1)))

  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag,
    A1, A1In: TypeTag,
    A2, A2In: TypeTag](f: (A0, A1, A2) => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In],
      a1Enc: ParamEncoder.Aux[A1, A1In],
      a2Enc: ParamEncoder.Aux[A2, A2In]) =
    udf((a0: A0In, a1: A1In, a2: A2In) => f(a0Enc(a0), a1Enc(a1), a2Enc(a2)))

  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag,
    A1, A1In: TypeTag,
    A2, A2In: TypeTag,
    A3, A3In: TypeTag](f: (A0, A1, A2, A3) => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In],
      a1Enc: ParamEncoder.Aux[A1, A1In],
      a2Enc: ParamEncoder.Aux[A2, A2In],
      a3Enc: ParamEncoder.Aux[A3, A3In]) =
    udf((a0: A0In, a1: A1In, a2: A2In, a3: A3In) => f(a0Enc(a0), a1Enc(a1), a2Enc(a2), a3Enc(a3)))

  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag,
    A1, A1In: TypeTag,
    A2, A2In: TypeTag,
    A3, A3In: TypeTag,
    A4, A4In: TypeTag](f: (A0, A1, A2, A3, A4) => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In],
      a1Enc: ParamEncoder.Aux[A1, A1In],
      a2Enc: ParamEncoder.Aux[A2, A2In],
      a3Enc: ParamEncoder.Aux[A3, A3In],
      a4Enc: ParamEncoder.Aux[A4, A4In]) =
    udf((a0:A0In, a1: A1In, a2: A2In, a3: A3In, a4: A4In) => f(a0Enc(a0), a1Enc(a1), a2Enc(a2), a3Enc(a3), a4Enc(a4)))

  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag,
    A1, A1In: TypeTag,
    A2, A2In: TypeTag,
    A3, A3In: TypeTag,
    A4, A4In: TypeTag,
    A5, A5In: TypeTag](f: (A0, A1, A2, A3, A4, A5) => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In],
      a1Enc: ParamEncoder.Aux[A1, A1In],
      a2Enc: ParamEncoder.Aux[A2, A2In],
      a3Enc: ParamEncoder.Aux[A3, A3In],
      a4Enc: ParamEncoder.Aux[A4, A4In],
      a5Enc: ParamEncoder.Aux[A5, A5In]) =
    udf((a0: A0In, a1: A1In, a2: A2In, a3: A3In, a4: A4In, a5: A5In) =>
      f(a0Enc(a0), a1Enc(a1), a2Enc(a2), a3Enc(a3), a4Enc(a4), a5Enc(a5)))

  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag,
    A1, A1In: TypeTag,
    A2, A2In: TypeTag,
    A3, A3In: TypeTag,
    A4, A4In: TypeTag,
    A5, A5In: TypeTag,
    A6, A6In: TypeTag](f: (A0, A1, A2, A3, A4, A5, A6) => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In],
      a1Enc: ParamEncoder.Aux[A1, A1In],
      a2Enc: ParamEncoder.Aux[A2, A2In],
      a3Enc: ParamEncoder.Aux[A3, A3In],
      a4Enc: ParamEncoder.Aux[A4, A4In],
      a5Enc: ParamEncoder.Aux[A5, A5In],
      a6Enc: ParamEncoder.Aux[A6, A6In]) =
    udf((a0: A0In, a1: A1In, a2: A2In, a3: A3In, a4: A4In, a5: A5In, a6: A6In) =>
      f(a0Enc(a0), a1Enc(a1), a2Enc(a2), a3Enc(a3), a4Enc(a4), a5Enc(a5), a6Enc(a6)))

  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag,
    A1, A1In: TypeTag,
    A2, A2In: TypeTag,
    A3, A3In: TypeTag,
    A4, A4In: TypeTag,
    A5, A5In: TypeTag,
    A6, A6In: TypeTag,
    A7, A7In: TypeTag](f: (A0, A1, A2, A3, A4, A5, A6, A7) => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In],
      a1Enc: ParamEncoder.Aux[A1, A1In],
      a2Enc: ParamEncoder.Aux[A2, A2In],
      a3Enc: ParamEncoder.Aux[A3, A3In],
      a4Enc: ParamEncoder.Aux[A4, A4In],
      a5Enc: ParamEncoder.Aux[A5, A5In],
      a6Enc: ParamEncoder.Aux[A6, A6In],
      a7Enc: ParamEncoder.Aux[A7, A7In]) =
    udf((a0: A0In, a1: A1In, a2: A2In, a3: A3In, a4: A4In, a5: A5In, a6: A6In, a7: A7In) =>
      f(a0Enc(a0), a1Enc(a1), a2Enc(a2), a3Enc(a3), a4Enc(a4), a5Enc(a5), a6Enc(a6), a7Enc(a7)))

  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag,
    A1, A1In: TypeTag,
    A2, A2In: TypeTag,
    A3, A3In: TypeTag,
    A4, A4In: TypeTag,
    A5, A5In: TypeTag,
    A6, A6In: TypeTag,
    A7, A7In: TypeTag,
    A8, A8In: TypeTag](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In],
      a1Enc: ParamEncoder.Aux[A1, A1In],
      a2Enc: ParamEncoder.Aux[A2, A2In],
      a3Enc: ParamEncoder.Aux[A3, A3In],
      a4Enc: ParamEncoder.Aux[A4, A4In],
      a5Enc: ParamEncoder.Aux[A5, A5In],
      a6Enc: ParamEncoder.Aux[A6, A6In],
      a7Enc: ParamEncoder.Aux[A7, A7In],
      a8Enc: ParamEncoder.Aux[A8, A8In]) =
    udf((a0: A0In, a1: A1In, a2: A2In, a3: A3In, a4: A4In, a5: A5In, a6: A6In, a7: A7In, a8: A8In) =>
      f(a0Enc(a0), a1Enc(a1), a2Enc(a2), a3Enc(a3), a4Enc(a4), a5Enc(a5), a6Enc(a6), a7Enc(a7), a8Enc(a8)))

  def apply[
    RT: TypeTag,
    A0, A0In: TypeTag,
    A1, A1In: TypeTag,
    A2, A2In: TypeTag,
    A3, A3In: TypeTag,
    A4, A4In: TypeTag,
    A5, A5In: TypeTag,
    A6, A6In: TypeTag,
    A7, A7In: TypeTag,
    A8, A8In: TypeTag,
    A9, A9In: TypeTag](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => RT)(
    implicit
      a0Enc: ParamEncoder.Aux[A0, A0In],
      a1Enc: ParamEncoder.Aux[A1, A1In],
      a2Enc: ParamEncoder.Aux[A2, A2In],
      a3Enc: ParamEncoder.Aux[A3, A3In],
      a4Enc: ParamEncoder.Aux[A4, A4In],
      a5Enc: ParamEncoder.Aux[A5, A5In],
      a6Enc: ParamEncoder.Aux[A6, A6In],
      a7Enc: ParamEncoder.Aux[A7, A7In],
      a8Enc: ParamEncoder.Aux[A8, A8In],
      a9Enc: ParamEncoder.Aux[A9, A9In]) =
    udf((a0: A0In, a1: A1In, a2: A2In, a3: A3In, a4: A4In, a5: A5In, a6: A6In, a7: A7In, a8: A8In, a9: A9In) =>
      f(a0Enc(a0), a1Enc(a1), a2Enc(a2), a3Enc(a3), a4Enc(a4), a5Enc(a5), a6Enc(a6), a7Enc(a7), a8Enc(a8), a9Enc(a9)))
}
