package io.getquill.context.jooq

import io.getquill.NamingStrategy
import scala.quoted.*

/**
 * Compile-time macro for extracting schema information from case classes.
 *
 * Usage:
 * ```scala
 * import io.getquill.context.jooq.SchemaMacro.*
 *
 * case class Person(id: Int, name: String, age: Int)
 *
 * val schema = extractSchema[Person](Literal)
 * // TableSchema("Person", List(ColumnSchema("id", INTEGER, false), ...))
 * ```
 */
object SchemaMacro {

  import SchemaMigration.*

  /**
   * Extract schema from a case class at compile time
   *
   * @tparam T The case class type
   * @param naming Naming strategy to apply
   * @return TableSchema with columns derived from case class fields
   */
  inline def extractSchema[T](naming: NamingStrategy): TableSchema =
    ${ extractSchemaImpl[T]('naming) }

  /**
   * Extract schema with explicit table name
   */
  inline def extractSchema[T](tableName: String, naming: NamingStrategy): TableSchema =
    ${ extractSchemaWithNameImpl[T]('tableName, 'naming) }

  private def extractSchemaImpl[T: Type](naming: Expr[NamingStrategy])(using Quotes): Expr[TableSchema] = {
    import quotes.reflect.*

    val tpe = TypeRepr.of[T]
    val sym = tpe.typeSymbol

    // Verify it's a case class
    if (!sym.flags.is(Flags.Case)) {
      report.errorAndAbort(s"${sym.name} is not a case class")
    }

    val className = sym.name
    val fields = sym.caseFields

    // Generate column expressions
    val columnExprs = fields.map { field =>
      val fieldName = field.name
      val fieldType = tpe.memberType(field)
      val (sqlType, nullable) = analyzeType(fieldType)

      '{
        val colName = $naming.column(${Expr(fieldName)})
        ColumnSchema(colName, ${Expr(sqlType)}, ${Expr(nullable)})
      }
    }

    val columnsExpr = Expr.ofList(columnExprs)

    '{
      val tableName = $naming.table(${Expr(className)})
      TableSchema(tableName, $columnsExpr)
    }
  }

  private def extractSchemaWithNameImpl[T: Type](
    tableName: Expr[String],
    naming: Expr[NamingStrategy]
  )(using Quotes): Expr[TableSchema] = {
    import quotes.reflect.*

    val tpe = TypeRepr.of[T]
    val sym = tpe.typeSymbol

    // Verify it's a case class
    if (!sym.flags.is(Flags.Case)) {
      report.errorAndAbort(s"${sym.name} is not a case class")
    }

    val fields = sym.caseFields

    // Generate column expressions
    val columnExprs = fields.map { field =>
      val fieldName = field.name
      val fieldType = tpe.memberType(field)
      val (sqlType, nullable) = analyzeType(fieldType)

      '{
        val colName = $naming.column(${Expr(fieldName)})
        ColumnSchema(colName, ${Expr(sqlType)}, ${Expr(nullable)})
      }
    }

    val columnsExpr = Expr.ofList(columnExprs)

    '{
      TableSchema($naming.table($tableName), $columnsExpr)
    }
  }

  /**
   * Analyze a Scala type and return (SQL type code, nullable)
   */
  private def analyzeType(using Quotes)(tpe: quotes.reflect.TypeRepr): (Int, Boolean) = {
    import quotes.reflect.*
    import java.sql.Types

    // Check if it's Option[T]
    tpe.asType match {
      case '[Option[inner]] =>
        val (sqlType, _) = analyzeType(TypeRepr.of[inner])
        (sqlType, true)
      case '[Int] => (Types.INTEGER, false)
      case '[Long] => (Types.BIGINT, false)
      case '[Short] => (Types.SMALLINT, false)
      case '[Byte] => (Types.TINYINT, false)
      case '[Float] => (Types.FLOAT, false)
      case '[Double] => (Types.DOUBLE, false)
      case '[Boolean] => (Types.BOOLEAN, false)
      case '[String] => (Types.VARCHAR, false)
      case '[BigDecimal] => (Types.DECIMAL, false)
      case '[java.math.BigDecimal] => (Types.DECIMAL, false)
      case '[java.time.LocalDate] => (Types.DATE, false)
      case '[java.time.LocalTime] => (Types.TIME, false)
      case '[java.time.LocalDateTime] => (Types.TIMESTAMP, false)
      case '[java.time.Instant] => (Types.TIMESTAMP, false)
      case '[java.time.OffsetDateTime] => (Types.TIMESTAMP, false)
      case '[java.time.ZonedDateTime] => (Types.TIMESTAMP, false)
      case '[Array[Byte]] => (Types.BLOB, false)
      case _ => (Types.VARCHAR, false) // Default fallback
    }
  }
}
