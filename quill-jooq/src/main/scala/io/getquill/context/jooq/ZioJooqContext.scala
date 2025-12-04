package io.getquill.context.jooq

import io.getquill.{Quoted, Query as QuillQuery, Action, ActionReturning, BatchAction, QAC, NamingStrategy, Literal, SnakeCase, Planter, EagerPlanter}
import io.getquill.context.{LiftMacro, LiftQueryMacro}
import io.getquill.generic.GenericEncoder
import io.getquill.ast.{Ast, Entity, Filter, Insert, Update, Delete, Map as AstMap, SortBy, Take, Drop, Distinct, Join as AstJoin, FlatMap, Returning, Property, Ident as AstIdent, Constant, BinaryOperation, UnaryOperation, Tuple, CaseClass, ScalarTag, NullValue, InnerJoin, LeftJoin, RightJoin, FullJoin, Asc, Desc, AscNullsFirst, DescNullsFirst, AscNullsLast, DescNullsLast, TupleOrdering, BooleanOperator, EqualityOperator, NumericOperator, StringOperator, SetOperator, PrefixUnaryOperator, OptionIsDefined, OptionIsEmpty, JoinType, Ordering as AstOrdering}
import io.getquill.context.RowContext
import org.jooq.{DSLContext, Record, SQLDialect, Field, Condition, Table, SelectSelectStep, SelectJoinStep, SelectConditionStep, ResultQuery, InsertSetStep, InsertSetMoreStep, UpdateSetFirstStep, UpdateSetMoreStep, UpdateConditionStep, DeleteConditionStep, SortField}
import org.jooq.impl.DSL
import zio.{ZIO, ZLayer, Unsafe, Runtime}

import java.sql.{Connection, ResultSet, SQLException}
import javax.sql.DataSource
import scala.annotation.targetName
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/**
 * ZIO-based jOOQ Context that translates Quill DSL directly to jOOQ DSL.
 *
 * Unlike traditional Quill contexts that generate SQL strings,
 * this context translates the AST to jOOQ's type-safe DSL objects,
 * leveraging jOOQ's SQL generation and execution capabilities.
 *
 * @param dialect jOOQ SQLDialect to use (e.g., SQLDialect.POSTGRES, SQLDialect.MYSQL)
 * @param naming Quill naming strategy for table/column names
 */
class ZioJooqContext[+N <: NamingStrategy](
    val dialect: SQLDialect,
    val naming: N
) extends RowContext {

  type Result[T] = ZIO[DataSource, SQLException, T]
  type PrepareRow = Connection
  type ResultRow = org.jooq.Record
  type Session = Connection
  type Runner = Unit

  // Encoders and Decoders
  type Encoder[T] = JooqEncoder[T]
  type Decoder[T] = JooqDecoder[T]

  // GenericEncoder instances for lift() macro support
  // These are pass-through encoders since jOOQ handles actual encoding
  // The value is captured by EagerPlanter and used during jOOQ translation
  implicit val intEncoder: GenericEncoder[Int, Connection, Connection] = (_, v, row, _) => row
  implicit val longEncoder: GenericEncoder[Long, Connection, Connection] = (_, v, row, _) => row
  implicit val shortEncoder: GenericEncoder[Short, Connection, Connection] = (_, v, row, _) => row
  implicit val byteEncoder: GenericEncoder[Byte, Connection, Connection] = (_, v, row, _) => row
  implicit val floatEncoder: GenericEncoder[Float, Connection, Connection] = (_, v, row, _) => row
  implicit val doubleEncoder: GenericEncoder[Double, Connection, Connection] = (_, v, row, _) => row
  implicit val booleanEncoder: GenericEncoder[Boolean, Connection, Connection] = (_, v, row, _) => row
  implicit val stringEncoder: GenericEncoder[String, Connection, Connection] = (_, v, row, _) => row
  implicit val bigDecimalEncoder: GenericEncoder[BigDecimal, Connection, Connection] = (_, v, row, _) => row
  implicit val javaBigDecimalEncoder: GenericEncoder[java.math.BigDecimal, Connection, Connection] = (_, v, row, _) => row
  implicit val byteArrayEncoder: GenericEncoder[Array[Byte], Connection, Connection] = (_, v, row, _) => row

  // Date/Time encoders
  implicit val localDateEncoder: GenericEncoder[java.time.LocalDate, Connection, Connection] = (_, v, row, _) => row
  implicit val localTimeEncoder: GenericEncoder[java.time.LocalTime, Connection, Connection] = (_, v, row, _) => row
  implicit val localDateTimeEncoder: GenericEncoder[java.time.LocalDateTime, Connection, Connection] = (_, v, row, _) => row
  implicit val instantEncoder: GenericEncoder[java.time.Instant, Connection, Connection] = (_, v, row, _) => row
  implicit val offsetDateTimeEncoder: GenericEncoder[java.time.OffsetDateTime, Connection, Connection] = (_, v, row, _) => row
  implicit val zonedDateTimeEncoder: GenericEncoder[java.time.ZonedDateTime, Connection, Connection] = (_, v, row, _) => row

  // Option encoders
  implicit val optionIntEncoder: GenericEncoder[Option[Int], Connection, Connection] = (_, v, row, _) => row
  implicit val optionLongEncoder: GenericEncoder[Option[Long], Connection, Connection] = (_, v, row, _) => row
  implicit val optionDoubleEncoder: GenericEncoder[Option[Double], Connection, Connection] = (_, v, row, _) => row
  implicit val optionBooleanEncoder: GenericEncoder[Option[Boolean], Connection, Connection] = (_, v, row, _) => row
  implicit val optionStringEncoder: GenericEncoder[Option[String], Connection, Connection] = (_, v, row, _) => row

  // Result types
  type RunQueryResult[T] = List[T]
  type RunQuerySingleResult[T] = T
  type RunActionResult = Long
  type RunActionReturningResult[T] = T
  type RunBatchActionResult = List[Long]
  type RunBatchActionReturningResult[T] = List[T]

  // Implicit decoder summoning
  implicit inline def autoDecoder[T]: Decoder[T] = ${ JooqDecoderMacro.summon[T] }

  // ========== Run methods ==========

  @targetName("runQueryDefault")
  inline def run[T](inline quoted: Quoted[QuillQuery[T]]): Result[List[T]] =
    ${ ZioJooqContextMacro.runQuery[T, N]('this, 'quoted) }

  @targetName("runQuerySingle")
  inline def run[T](inline quoted: Quoted[T]): Result[T] =
    ${ ZioJooqContextMacro.runQuerySingle[T, N]('this, 'quoted) }

  @targetName("runAction")
  inline def run[E](inline quoted: Quoted[Action[E]]): Result[Long] =
    ${ ZioJooqContextMacro.runAction[E, N]('this, 'quoted) }

  @targetName("runActionReturning")
  inline def run[E, T](inline quoted: Quoted[ActionReturning[E, T]]): Result[T] =
    ${ ZioJooqContextMacro.runActionReturning[E, T, N]('this, 'quoted) }

  @targetName("runBatchAction")
  inline def run[I, A <: Action[I] & QAC[I, Nothing]](inline quoted: Quoted[BatchAction[A]]): Result[List[Long]] =
    ${ ZioJooqContextMacro.runBatchAction[I, A, N]('this, 'quoted) }

  // ========== Lift methods ==========

  inline def lift[T](inline runtimeValue: T): T =
    ${ LiftMacro[T, PrepareRow, Session]('runtimeValue) }

  inline def liftQuery[U[_] <: Iterable[_], T](inline runtimeValue: U[T]): QuillQuery[T] =
    ${ LiftQueryMacro[T, U, PrepareRow, Session]('runtimeValue) }

  // ========== Internal execution methods ==========

  def executeQuery[T](
      ast: Ast,
      lifts: List[Planter[?, ?, ?]],
      extractor: (Record, Int) => T
  ): Result[List[T]] = {
    ZIO.serviceWithZIO[DataSource] { ds =>
      ZIO.attemptBlocking {
        val conn = ds.getConnection()
        try {
          val dslCtx = DSL.using(conn, dialect)
          val translationCtx = JooqAstTranslator.TranslationContext(naming)

          // Bind lift values
          lifts.foreach {
            case p: EagerPlanter[?, ?, ?] =>
              translationCtx.addBinding(p.uid, p.value)
            case _ => // Other planters handled differently
          }

          val query = JooqAstTranslator.translateQuery(ast, translationCtx, dslCtx)

          // Bind parameters
          val boundQuery = bindParameters(query, translationCtx.bindings.toList)

          // Execute and extract results
          val result = boundQuery.fetch()
          result.asScala.toList.zipWithIndex.map { case (record, idx) =>
            extractor(record, idx)
          }
        } finally {
          conn.close()
        }
      }.refineToOrDie[SQLException]
    }
  }

  def executeQuerySingle[T](
      ast: Ast,
      lifts: List[Planter[?, ?, ?]],
      extractor: (Record, Int) => T
  ): Result[T] = {
    executeQuery(ast, lifts, extractor).flatMap { results =>
      results match {
        case head :: Nil => ZIO.succeed(head)
        case Nil => ZIO.fail(new SQLException("Expected single result but got none"))
        case _ => ZIO.fail(new SQLException(s"Expected single result but got ${results.size}"))
      }
    }
  }

  def executeAction(
      ast: Ast,
      lifts: List[Planter[?, ?, ?]]
  ): Result[Long] = {
    ZIO.serviceWithZIO[DataSource] { ds =>
      ZIO.attemptBlocking {
        val conn = ds.getConnection()
        try {
          val dslCtx = DSL.using(conn, dialect)
          val translationCtx = JooqAstTranslator.TranslationContext(naming)

          lifts.foreach {
            case p: EagerPlanter[?, ?, ?] =>
              translationCtx.addBinding(p.uid, p.value)
            case _ =>
          }

          val rowsAffected = ast match {
            case insert: Insert =>
              val query = JooqAstTranslator.translateInsert(insert, translationCtx, dslCtx)
              bindParametersInsert(query, translationCtx.bindings.toList).execute()

            case update: Update =>
              val query = JooqAstTranslator.translateUpdate(update, translationCtx, dslCtx)
              bindParametersUpdate(query, translationCtx.bindings.toList).execute()

            case delete: Delete =>
              val query = JooqAstTranslator.translateDelete(delete, translationCtx, dslCtx)
              bindParametersDelete(query, translationCtx.bindings.toList).execute()

            case _ =>
              throw new UnsupportedOperationException(s"Unsupported action: ${ast.getClass.getSimpleName}")
          }

          rowsAffected.toLong
        } finally {
          conn.close()
        }
      }.refineToOrDie[SQLException]
    }
  }

  def executeActionReturning[T](
      ast: Ast,
      lifts: List[Planter[?, ?, ?]],
      extractor: (Record, Int) => T,
      returningColumn: String
  ): Result[T] = {
    ZIO.serviceWithZIO[DataSource] { ds =>
      ZIO.attemptBlocking {
        val conn = ds.getConnection()
        try {
          val dslCtx = DSL.using(conn, dialect)
          val translationCtx = JooqAstTranslator.TranslationContext(naming)

          lifts.foreach {
            case p: EagerPlanter[?, ?, ?] =>
              translationCtx.addBinding(p.uid, p.value)
            case _ =>
          }

          ast match {
            case insert: Insert =>
              val query = JooqAstTranslator.translateInsert(insert, translationCtx, dslCtx)
              val boundQuery = bindParametersInsert(query, translationCtx.bindings.toList)
              val result = boundQuery.returning(DSL.field(returningColumn)).fetchOne()
              extractor(result, 0)

            case _ =>
              throw new UnsupportedOperationException(s"Returning not supported for: ${ast.getClass.getSimpleName}")
          }
        } finally {
          conn.close()
        }
      }.refineToOrDie[SQLException]
    }
  }

  def executeBatchAction(
      ast: Ast,
      liftsBatch: List[List[Planter[?, ?, ?]]]
  ): Result[List[Long]] = {
    ZIO.serviceWithZIO[DataSource] { ds =>
      ZIO.attemptBlocking {
        val conn = ds.getConnection()
        try {
          val dslCtx = DSL.using(conn, dialect)

          liftsBatch.map { lifts =>
            val translationCtx = JooqAstTranslator.TranslationContext(naming)
            lifts.foreach {
              case p: EagerPlanter[?, ?, ?] =>
                translationCtx.addBinding(p.uid, p.value)
              case _ =>
            }

            ast match {
              case insert: Insert =>
                val query = JooqAstTranslator.translateInsert(insert, translationCtx, dslCtx)
                bindParametersInsert(query, translationCtx.bindings.toList).execute().toLong

              case update: Update =>
                val query = JooqAstTranslator.translateUpdate(update, translationCtx, dslCtx)
                bindParametersUpdate(query, translationCtx.bindings.toList).execute().toLong

              case delete: Delete =>
                val query = JooqAstTranslator.translateDelete(delete, translationCtx, dslCtx)
                bindParametersDelete(query, translationCtx.bindings.toList).execute().toLong

              case _ =>
                throw new UnsupportedOperationException(s"Unsupported batch action: ${ast.getClass.getSimpleName}")
            }
          }
        } finally {
          conn.close()
        }
      }.refineToOrDie[SQLException]
    }
  }

  // ========== Parameter binding helpers ==========

  private def bindParameters(query: ResultQuery[Record], bindings: List[(String, Any)]): ResultQuery[Record] = {
    // For now, return the query as-is. Parameter binding will be implemented with jOOQ's param API
    query
  }

  private def bindParametersInsert(query: InsertSetMoreStep[Record], bindings: List[(String, Any)]): InsertSetMoreStep[Record] = {
    query
  }

  private def bindParametersUpdate(query: UpdateConditionStep[Record], bindings: List[(String, Any)]): UpdateConditionStep[Record] = {
    query
  }

  private def bindParametersDelete(query: DeleteConditionStep[Record], bindings: List[(String, Any)]): DeleteConditionStep[Record] = {
    query
  }

  // ========== Resource management ==========

  def close(): Unit = ()
}

// ========== Encoder/Decoder types ==========

trait JooqEncoder[T] {
  def encode(value: T): Any
}

trait JooqDecoder[T] {
  def decode(record: Record, index: Int): T
}

object JooqDecoder {
  given intDecoder: JooqDecoder[Int] = (record, index) => record.get(index, classOf[java.lang.Integer]).intValue()
  given longDecoder: JooqDecoder[Long] = (record, index) => record.get(index, classOf[java.lang.Long]).longValue()
  given stringDecoder: JooqDecoder[String] = (record, index) => record.get(index, classOf[String])
  given doubleDecoder: JooqDecoder[Double] = (record, index) => record.get(index, classOf[java.lang.Double]).doubleValue()
  given booleanDecoder: JooqDecoder[Boolean] = (record, index) => record.get(index, classOf[java.lang.Boolean]).booleanValue()
  given optionIntDecoder: JooqDecoder[Option[Int]] = (record, index) =>
    Option(record.get(index, classOf[java.lang.Integer])).map(_.intValue())
  given optionStringDecoder: JooqDecoder[Option[String]] = (record, index) =>
    Option(record.get(index, classOf[String]))
}

// ========== Macro implementations ==========

object JooqDecoderMacro {
  import scala.quoted.*

  def summon[T: scala.quoted.Type](using Quotes): Expr[JooqDecoder[T]] = {
    import quotes.reflect.*

    Expr.summon[JooqDecoder[T]] match {
      case Some(decoder) => decoder
      case None =>
        // For case classes, generate a decoder
        TypeRepr.of[T].classSymbol match {
          case Some(sym) if sym.flags.is(Flags.Case) =>
            generateCaseClassDecoder[T]
          case _ =>
            report.errorAndAbort(s"Cannot find or generate JooqDecoder for type ${scala.quoted.Type.show[T]}")
        }
    }
  }

  private def generateCaseClassDecoder[T: scala.quoted.Type](using Quotes): Expr[JooqDecoder[T]] = {
    import quotes.reflect.*

    val tpe = TypeRepr.of[T]
    val sym = tpe.typeSymbol
    val fields = sym.caseFields

    val companion = sym.companionModule
    val applyMethod = companion.methodMember("apply").head

    '{
      new JooqDecoder[T] {
        def decode(record: org.jooq.Record, startIndex: Int): T = {
          ${
            val fieldExprs = fields.zipWithIndex.map { case (field, idx) =>
              val fieldType = tpe.memberType(field)
              fieldType.asType match {
                case '[ft] =>
                  val decoder = Expr.summon[JooqDecoder[ft]].getOrElse {
                    report.errorAndAbort(s"Cannot find JooqDecoder for field ${field.name} of type ${scala.quoted.Type.show[ft]}")
                  }
                  '{ $decoder.decode(record, startIndex + ${Expr(idx)}) }
              }
            }

            val args = Expr.ofList(fieldExprs)
            Apply(
              Select(Ref(companion), applyMethod),
              fieldExprs.map(_.asTerm)
            ).asExprOf[T]
          }
        }
      }
    }
  }
}

object ZioJooqContextMacro {
  import scala.quoted.*
  import io.getquill.parser.Unlifter
  import io.getquill.metaprog.QuotedExpr

  def runQuery[T: scala.quoted.Type, N <: NamingStrategy: scala.quoted.Type](
      ctx: Expr[ZioJooqContext[N]],
      quoted: Expr[Quoted[QuillQuery[T]]]
  )(using Quotes): Expr[ZIO[DataSource, SQLException, List[T]]] = {
    import quotes.reflect.*

    val ast = extractAst(quoted)
    val lifts = extractLifts(quoted)
    val decoder = Expr.summon[JooqDecoder[T]].getOrElse {
      JooqDecoderMacro.summon[T]
    }

    '{
      $ctx.executeQuery[T](
        $ast,
        $lifts,
        (record, idx) => $decoder.decode(record, 0)
      )
    }
  }

  def runQuerySingle[T: scala.quoted.Type, N <: NamingStrategy: scala.quoted.Type](
      ctx: Expr[ZioJooqContext[N]],
      quoted: Expr[Quoted[T]]
  )(using Quotes): Expr[ZIO[DataSource, SQLException, T]] = {
    import quotes.reflect.*

    val ast = extractAst(quoted.asInstanceOf[Expr[Quoted[QuillQuery[T]]]])
    val lifts = extractLifts(quoted.asInstanceOf[Expr[Quoted[QuillQuery[T]]]])
    val decoder = Expr.summon[JooqDecoder[T]].getOrElse {
      JooqDecoderMacro.summon[T]
    }

    '{
      $ctx.executeQuerySingle[T](
        $ast,
        $lifts,
        (record, idx) => $decoder.decode(record, 0)
      )
    }
  }

  def runAction[E: scala.quoted.Type, N <: NamingStrategy: scala.quoted.Type](
      ctx: Expr[ZioJooqContext[N]],
      quoted: Expr[Quoted[Action[E]]]
  )(using Quotes): Expr[ZIO[DataSource, SQLException, Long]] = {
    val ast = extractAst(quoted.asInstanceOf[Expr[Quoted[Action[E]]]])
    val lifts = extractLifts(quoted.asInstanceOf[Expr[Quoted[Action[E]]]])

    '{
      $ctx.executeAction($ast, $lifts)
    }
  }

  def runActionReturning[E: scala.quoted.Type, T: scala.quoted.Type, N <: NamingStrategy: scala.quoted.Type](
      ctx: Expr[ZioJooqContext[N]],
      quoted: Expr[Quoted[ActionReturning[E, T]]]
  )(using Quotes): Expr[ZIO[DataSource, SQLException, T]] = {
    import quotes.reflect.*

    val ast = extractAst(quoted.asInstanceOf[Expr[Quoted[ActionReturning[E, T]]]])
    val lifts = extractLifts(quoted.asInstanceOf[Expr[Quoted[ActionReturning[E, T]]]])
    val decoder = Expr.summon[JooqDecoder[T]].getOrElse {
      JooqDecoderMacro.summon[T]
    }

    // Extract returning column name from AST - simplified for now
    val returningColumn = '{ "id" }

    '{
      $ctx.executeActionReturning[T](
        $ast,
        $lifts,
        (record, idx) => $decoder.decode(record, 0),
        $returningColumn
      )
    }
  }

  def runBatchAction[I: scala.quoted.Type, A <: Action[I] & QAC[I, Nothing]: scala.quoted.Type, N <: NamingStrategy: scala.quoted.Type](
      ctx: Expr[ZioJooqContext[N]],
      quoted: Expr[Quoted[BatchAction[A]]]
  )(using Quotes): Expr[ZIO[DataSource, SQLException, List[Long]]] = {
    import quotes.reflect.*

    // For batch actions, we need to extract the inner action AST
    // This is a simplified implementation
    '{
      ZIO.succeed(List.empty[Long]) // Placeholder - batch implementation needed
    }
  }

  private def extractAst[T: scala.quoted.Type](quoted: Expr[Quoted[T]])(using Quotes): Expr[Ast] = {
    '{ $quoted.ast }
  }

  private def extractLifts[T: scala.quoted.Type](quoted: Expr[Quoted[T]])(using Quotes): Expr[List[Planter[?, ?, ?]]] = {
    '{ $quoted.lifts }
  }
}

// ========== Convenience type aliases ==========

object ZioJooqContext {
  type Postgres[N <: NamingStrategy] = ZioJooqContext[N]
  type MySQL[N <: NamingStrategy] = ZioJooqContext[N]
  type H2[N <: NamingStrategy] = ZioJooqContext[N]
}
