package io.getquill.anyval

import io.getquill._
import io.getquill.context.ExecutionType

case class Blah(value: String, value2: Int)
case class Rec(value: Blah, otherValue: String)
case class Name(value: String) extends AnyVal

class AnyValEncodingSpec extends Spec {

  val ctx = new MirrorContext(MirrorSqlDialect, Literal)
  import ctx._

  case class Person(name: Name, age: Int)

  // AnyVal types are automatically handled by anyValEncoder/anyValDecoder
  // No explicit MappedEncoding needed

  "simple anyval should encode and decode" in {
    val name = Name("Joe")
    val mirror = ctx.run(query[Person].filter(p => p.name == lift(name)))
    mirror.triple mustEqual (
      (
        "SELECT p.name, p.age FROM Person p WHERE p.name = ?",
        List("Joe"),
        ExecutionType.Static
      )
    )
  }
}
