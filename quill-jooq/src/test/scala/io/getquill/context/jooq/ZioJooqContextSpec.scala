package io.getquill.context.jooq

import io.getquill.*
import io.getquill.ast.{Entity, Filter}
import io.getquill.quat.Quat
import org.jooq.SQLDialect
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

class ZioJooqContextSpec extends AnyFreeSpec with Matchers {

  case class Person(id: Int, name: String, age: Int)
  case class Address(personId: Int, street: String, city: String)

  // Test context with H2 for unit testing
  val ctx = new ZioJooqContext[Literal](SQLDialect.H2, Literal)
  import ctx.*

  "ZioJooqContext" - {

    "should compile simple query" in {
      val q = quote {
        query[Person]
      }
      q.ast mustBe a[Entity]
    }

    "should compile filtered query" in {
      val q = quote {
        query[Person].filter(p => p.age > 25)
      }
      q.ast mustBe a[Filter]
    }

    "should compile mapped query" in {
      val q = quote {
        query[Person].map(p => p.name)
      }
      q.ast mustBe a[io.getquill.ast.Map]
    }

    // TODO: lift functionality requires encoder implementation
    // Uncomment after implementing JooqEncoder for basic types
    // "should compile query with lift" in {
    //   val minAge = 25
    //   val q = quote {
    //     query[Person].filter(p => p.age > lift(minAge))
    //   }
    //   q.ast mustBe a[Filter]
    //   q.lifts.size mustBe 1
    // }

    "should compile sorted query" in {
      val q = quote {
        query[Person].sortBy(p => p.name)
      }
      q.ast mustBe a[io.getquill.ast.SortBy]
    }

    "should compile query with take" in {
      val q = quote {
        query[Person].take(10)
      }
      q.ast mustBe a[io.getquill.ast.Take]
    }

    "should compile query with drop" in {
      val q = quote {
        query[Person].drop(5)
      }
      q.ast mustBe a[io.getquill.ast.Drop]
    }

  }

  "JooqAstTranslator" - {

    "should translate Entity to jOOQ table" in {
      val personQuat = Quat.Product("Person", "id" -> Quat.Value, "name" -> Quat.Value, "age" -> Quat.Value)
      val entity = Entity("Person", Nil, personQuat)
      val translationCtx = JooqAstTranslator.TranslationContext(Literal)

      val table = JooqAstTranslator.translateEntity(entity, translationCtx)
      table.getName mustBe "Person"
    }

    "should apply naming strategy" in {
      val personQuat = Quat.Product("PersonDetails", "id" -> Quat.Value, "name" -> Quat.Value, "age" -> Quat.Value)
      val entity = Entity("PersonDetails", Nil, personQuat)
      val translationCtx = JooqAstTranslator.TranslationContext(SnakeCase)

      val table = JooqAstTranslator.translateEntity(entity, translationCtx)
      table.getName mustBe "person_details"
    }

    "should resolve ScalarTag from bindings" in {
      import io.getquill.ast.{ScalarTag, External}

      val translationCtx = JooqAstTranslator.TranslationContext(Literal)
      translationCtx.addBinding("uid_123", 42)

      val scalarTag = ScalarTag("uid_123", External.Source.Parser)
      val field = JooqAstTranslator.translateField(scalarTag, "", translationCtx)

      // The field should be an inline value of 42
      field.toString must include("42")
    }

    "should resolve ScalarTag in field value for INSERT/UPDATE" in {
      import io.getquill.ast.{ScalarTag, External}

      val translationCtx = JooqAstTranslator.TranslationContext(Literal)
      translationCtx.addBinding("uid_456", "test_value")

      val scalarTag = ScalarTag("uid_456", External.Source.Parser)
      val value = JooqAstTranslator.translateFieldValue(scalarTag, "", translationCtx)

      value mustBe "test_value"
    }

  }

}
