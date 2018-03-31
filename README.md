# typedudf

## Installation

TODO

## Usage

```scala
import typedudf.TypedUdf
import typedudf.ParamEncoder._

case class Foo(x: Boolean, y: String)

TypedUdf((foo: Foo) => foo.y.length)
```
