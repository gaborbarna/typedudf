# typedudf

## Motivation

Spark sql user defined function doesn't support deserializing struct types into Product types (case classes, tuples etc.), therefore you have to manually access the fields of a Row:

```scala
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import spark.implicits._

case class Foo(x: Int, y: String)
val df = spark.createDataFrame(Seq(Foo(1, "asd"), Foo(2, "qwe"))).select(struct("*").as("foo"))
val fooUdf = udf((row: Row) => row.getAs[Int]("x") + row.getAs[String]("y").length)
df.withColumn("sum", fooUdf($"foo"))
```

https://issues.apache.org/jira/browse/SPARK-12823?page=com.atlassian.jira.plugin.system.issuetabpanels%3Aall-tabpanel

## Installation

In build.sbt add to `libraryDependencies`:

```scala
"com.github.lesbroot" %% "typedudf" % "1.1.0"
```

## Usage

typedudf derives the necessary boilerplate to construct Product types based on the input type parameters of a given function:

```scala
import typedudf.TypedUdf
import typedudf.ParamEncoder._

val fooUdf = TypedUdf((foo: Foo) => foo.x + foo.y.length)
df.withColumn("sum", fooUdf($"foo"))
```

## TODO
- add more tests
- benchmark
- CI
