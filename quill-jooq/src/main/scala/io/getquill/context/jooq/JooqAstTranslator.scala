package io.getquill.context.jooq

import io.getquill.ast.{Ast, Entity, Filter, Insert, Update, Delete, Map, SortBy, Take, Drop, Distinct, Join, FlatMap, Returning, ReturningGenerated, Property, Ident, Constant, BinaryOperation, UnaryOperation, Tuple, CaseClass, ScalarTag, NullValue, InnerJoin, LeftJoin, RightJoin, FullJoin, Asc, Desc, AscNullsFirst, DescNullsFirst, AscNullsLast, DescNullsLast, TupleOrdering, BooleanOperator, EqualityOperator, NumericOperator, StringOperator, SetOperator, OptionIsDefined, OptionIsEmpty, JoinType, Ordering, Aggregation, AggregationOperator, GroupByMap, SetContains, ListContains, Infix}
import io.getquill.NamingStrategy
import org.jooq.{DSLContext, Record, Field, Condition, Table, SelectSelectStep, SelectJoinStep, SelectConditionStep, ResultQuery, InsertSetStep, InsertSetMoreStep, UpdateSetFirstStep, UpdateSetMoreStep, UpdateConditionStep, DeleteConditionStep, SortField, SelectFieldOrAsterisk, GroupField, AggregateFunction}
import org.jooq.impl.DSL
import scala.collection.mutable

/**
 * Translates Quill AST nodes to jOOQ DSL objects.
 * This is the core component that bridges Quill's compile-time DSL with jOOQ's runtime SQL building.
 */
object JooqAstTranslator {

  case class TranslationContext(
    naming: NamingStrategy,
    bindings: mutable.ListBuffer[(String, Any)] = mutable.ListBuffer.empty,
    listBindings: mutable.Map[String, List[Any]] = mutable.Map.empty,
    aliases: mutable.Map[String, Table[?]] = mutable.Map.empty
  ) {
    def addBinding(uid: String, value: Any): Unit = {
      bindings += ((uid, value))
    }

    def addListBinding(uid: String, values: List[Any]): Unit = {
      listBindings(uid) = values
    }

    def registerAlias(name: String, table: Table[?]): Unit = {
      aliases(name) = table
    }

    def lookupAlias(name: String): Option[Table[?]] = {
      aliases.get(name)
    }

    def applyNaming(name: String): String = {
      naming.table(name)
    }

    def applyColumnNaming(name: String): String = {
      naming.column(name)
    }
  }

  /**
   * Translate a Query AST to a jOOQ SelectQuery
   */
  def translateQuery(ast: Ast, ctx: TranslationContext, dslCtx: DSLContext): ResultQuery[Record] = {
    ast match {
      case Map(query, alias, body) =>
        val fields = translateProjection(body, alias.name, ctx)
        translateQueryWithProjection(query, fields, ctx, dslCtx)

      case Filter(query, alias, predicate) =>
        query match {
          case entity: Entity =>
            val table = translateEntity(entity, ctx)
            val condition = translateCondition(predicate, alias.name, ctx)
            dslCtx.selectFrom(table).where(condition)
          case _ =>
            throw new UnsupportedOperationException("Nested filter queries not yet supported")
        }

      case entity: Entity =>
        val table = translateEntity(entity, ctx)
        dslCtx.selectFrom(table)

      case SortBy(query, alias, sortBy, ordering) =>
        query match {
          case entity: Entity =>
            val table = translateEntity(entity, ctx)
            val orderFields = translateOrdering(sortBy, ordering, alias.name, ctx)
            dslCtx.selectFrom(table).orderBy(orderFields*)
          case Filter(innerEntity: Entity, filterAlias, predicate) =>
            val table = translateEntity(innerEntity, ctx)
            val condition = translateCondition(predicate, filterAlias.name, ctx)
            val orderFields = translateOrdering(sortBy, ordering, alias.name, ctx)
            dslCtx.selectFrom(table).where(condition).orderBy(orderFields*)
          case _ =>
            throw new UnsupportedOperationException("Complex sorted queries not yet supported")
        }

      case Take(query, n) =>
        val limit = translateConstantInt(n)
        query match {
          case entity: Entity =>
            val table = translateEntity(entity, ctx)
            dslCtx.selectFrom(table).limit(limit)
          case SortBy(innerQuery, alias, sortBy, ordering) =>
            innerQuery match {
              case e: Entity =>
                val table = translateEntity(e, ctx)
                val orderFields = translateOrdering(sortBy, ordering, alias.name, ctx)
                dslCtx.selectFrom(table).orderBy(orderFields*).limit(limit)
              case Filter(fe: Entity, fAlias, predicate) =>
                val table = translateEntity(fe, ctx)
                val condition = translateCondition(predicate, fAlias.name, ctx)
                val orderFields = translateOrdering(sortBy, ordering, alias.name, ctx)
                dslCtx.selectFrom(table).where(condition).orderBy(orderFields*).limit(limit)
              case _ =>
                throw new UnsupportedOperationException("Complex sorted limited queries not yet supported")
            }
          case Filter(fe: Entity, fAlias, predicate) =>
            val table = translateEntity(fe, ctx)
            val condition = translateCondition(predicate, fAlias.name, ctx)
            dslCtx.selectFrom(table).where(condition).limit(limit)
          case _ =>
            throw new UnsupportedOperationException("Complex limited queries not yet supported")
        }

      case Drop(query, n) =>
        query match {
          case entity: Entity =>
            val table = translateEntity(entity, ctx)
            val offset = translateConstantInt(n)
            dslCtx.selectFrom(table).offset(offset)
          case _ =>
            throw new UnsupportedOperationException("Complex offset queries not yet supported")
        }

      case Distinct(query) =>
        query match {
          case entity: Entity =>
            val table = translateEntity(entity, ctx)
            dslCtx.selectDistinct().from(table)
          case _ =>
            throw new UnsupportedOperationException("Complex distinct queries not yet supported")
        }

      case GroupByMap(query, byAlias, byBody, mapAlias, mapBody) =>
        translateGroupByMap(query, byAlias, byBody, mapAlias, mapBody, ctx, dslCtx)

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported query AST node: ${ast.getClass.getSimpleName}")
    }
  }

  private def translateQueryWithProjection(
    query: Ast,
    fields: Seq[SelectFieldOrAsterisk],
    ctx: TranslationContext,
    dslCtx: DSLContext
  ): ResultQuery[Record] = {
    query match {
      case entity: Entity =>
        val table = translateEntity(entity, ctx)
        dslCtx.select(fields*).from(table)

      case Filter(innerQuery, alias, predicate) =>
        val table = innerQuery match {
          case e: Entity => translateEntity(e, ctx)
          case _ => throw new UnsupportedOperationException("Complex nested queries not yet supported")
        }
        val condition = translateCondition(predicate, alias.name, ctx)
        dslCtx.select(fields*).from(table).where(condition)

      case _ =>
        throw new UnsupportedOperationException(s"Complex query projections not yet supported: ${query.getClass.getSimpleName}")
    }
  }

  /**
   * Translate entity (table) reference
   */
  def translateEntity(entity: Entity, ctx: TranslationContext): Table[Record] = {
    val tableName = ctx.applyNaming(entity.name)
    DSL.table(DSL.name(tableName)).asInstanceOf[Table[Record]]
  }

  /**
   * Translate projection (SELECT fields)
   */
  def translateProjection(body: Ast, alias: String, ctx: TranslationContext): Seq[SelectFieldOrAsterisk] = {
    body match {
      case Ident(name, _) if name == alias =>
        Seq(DSL.asterisk())

      case Property(Ident(identName, _), propName) =>
        val colName = ctx.applyColumnNaming(propName)
        Seq(DSL.field(DSL.name(colName)))

      case Tuple(values) =>
        values.flatMap(v => translateProjection(v, alias, ctx))

      case CaseClass(_, fields) =>
        fields.flatMap { case (name, ast) =>
          translateProjection(ast, alias, ctx)
        }

      case _ =>
        Seq(translateField(body, alias, ctx))
    }
  }

  /**
   * Translate a field expression
   */
  def translateField(ast: Ast, alias: String, ctx: TranslationContext): Field[?] = {
    ast match {
      case Property(Ident(identName, _), propName) =>
        val colName = ctx.applyColumnNaming(propName)
        DSL.field(DSL.name(colName))

      case Constant(v, _) =>
        DSL.inline(v)

      case Ident(name, _) =>
        DSL.field(DSL.name(name))

      case ScalarTag(uid, _) =>
        // Look up the binding value and inline it
        ctx.bindings.find(_._1 == uid) match {
          case Some((_, value)) => DSL.inline(value)
          case None => DSL.param(uid, classOf[Object]).asInstanceOf[Field[?]]
        }

      case BinaryOperation(a, op, b) =>
        translateBinaryField(a, op, b, alias, ctx)

      case Aggregation(op, inner) =>
        translateAggregation(op, inner, alias, ctx)

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported field AST: ${ast.getClass.getSimpleName}")
    }
  }

  /**
   * Translate aggregation function (COUNT, SUM, AVG, MAX, MIN)
   */
  def translateAggregation(op: AggregationOperator, inner: Ast, alias: String, ctx: TranslationContext): Field[?] = {
    val innerField = inner match {
      // For query[T].map(p => max(p.name)), inner is Map(Entity, alias, Property)
      case Map(_, _, body) => translateField(body, alias, ctx)
      // For direct field reference
      case _ => translateField(inner, alias, ctx)
    }

    op match {
      case AggregationOperator.`min` => DSL.min(innerField.asInstanceOf[Field[Comparable[?]]])
      case AggregationOperator.`max` => DSL.max(innerField.asInstanceOf[Field[Comparable[?]]])
      case AggregationOperator.`avg` => DSL.avg(innerField.asInstanceOf[Field[Number]])
      case AggregationOperator.`sum` => DSL.sum(innerField.asInstanceOf[Field[Number]])
      case AggregationOperator.`size` => DSL.count(innerField)
    }
  }

  /**
   * Translate binary operation as field (for arithmetic operations)
   */
  def translateBinaryField(a: Ast, op: io.getquill.ast.BinaryOperator, b: Ast, alias: String, ctx: TranslationContext): Field[?] = {
    val left = translateField(a, alias, ctx)
    val right = translateField(b, alias, ctx)

    op match {
      case NumericOperator.`+` => left.asInstanceOf[Field[Number]].add(right.asInstanceOf[Field[Number]])
      case NumericOperator.`-` => left.asInstanceOf[Field[Number]].sub(right.asInstanceOf[Field[Number]])
      case NumericOperator.`*` => left.asInstanceOf[Field[Number]].mul(right.asInstanceOf[Field[Number]])
      case NumericOperator.`/` => left.asInstanceOf[Field[Number]].div(right.asInstanceOf[Field[Number]])
      case NumericOperator.`%` => left.asInstanceOf[Field[Number]].mod(right.asInstanceOf[Field[Number]])
      case StringOperator.`+` => DSL.concat(left.asInstanceOf[Field[String]], right.asInstanceOf[Field[String]])
      case _ => throw new UnsupportedOperationException(s"Unsupported binary field operator: $op")
    }
  }

  /**
   * Translate condition (WHERE clause)
   */
  def translateCondition(ast: Ast, alias: String, ctx: TranslationContext): Condition = {
    ast match {
      case BinaryOperation(a, op, b) =>
        translateBinaryCondition(a, op, b, alias, ctx)

      case UnaryOperation(op, operand) if op.toString == "!" =>
        DSL.not(translateCondition(operand, alias, ctx))

      case Constant(true, _) =>
        DSL.trueCondition()

      case Constant(false, _) =>
        DSL.falseCondition()

      case OptionIsDefined(inner) =>
        translateField(inner, alias, ctx).isNotNull

      case OptionIsEmpty(inner) =>
        translateField(inner, alias, ctx).isNull

      case SetContains(set, element) =>
        translateInCondition(set, element, alias, ctx)

      case ListContains(list, element) =>
        translateInCondition(list, element, alias, ctx)

      case Infix(parts, params, _, _, _) =>
        translateInfixCondition(parts, params, alias, ctx)

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported condition AST: ${ast.getClass.getSimpleName}")
    }
  }

  /**
   * Translate Infix expression as condition
   * Handles LIKE: Infix(List("", " like ", ""), List(field, pattern), ...)
   */
  def translateInfixCondition(parts: List[String], params: List[Ast], alias: String, ctx: TranslationContext): Condition = {
    // Check for LIKE pattern: parts = ["", " like ", ""]
    if (parts.size == 3 && parts(1).trim.toLowerCase == "like") {
      val field = translateField(params(0), alias, ctx).asInstanceOf[Field[String]]
      val pattern = translateField(params(1), alias, ctx).asInstanceOf[Field[String]]
      field.like(pattern)
    } else {
      // Generic infix - build raw SQL condition
      val translatedParams = params.map(p => translateField(p, alias, ctx))
      val sql = parts.zip(translatedParams.map(_.toString) :+ "").map { case (part, param) => part + param }.mkString
      DSL.condition(sql)
    }
  }

  /**
   * Translate IN condition (for SetContains/ListContains)
   * liftQuery(Set(1,2,3)).contains(p.id) => p.id IN (1, 2, 3)
   */
  def translateInCondition(collection: Ast, element: Ast, alias: String, ctx: TranslationContext): Condition = {
    val field = translateField(element, alias, ctx)

    collection match {
      case ScalarTag(uid, _) =>
        // Look up the list values from list bindings
        ctx.listBindings.get(uid) match {
          case Some(values) if values.isEmpty =>
            // Empty set - always false
            DSL.falseCondition()
          case Some(values) =>
            // Non-empty set - create IN condition
            field.in(values.map(DSL.inline(_))*)
          case None =>
            throw new IllegalStateException(s"No list binding found for uid: $uid")
        }

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported collection type for IN: ${collection.getClass.getSimpleName}")
    }
  }

  /**
   * Translate binary operation as condition
   */
  def translateBinaryCondition(a: Ast, op: io.getquill.ast.BinaryOperator, b: Ast, alias: String, ctx: TranslationContext): Condition = {
    op match {
      case BooleanOperator.`&&` =>
        translateCondition(a, alias, ctx).and(translateCondition(b, alias, ctx))

      case BooleanOperator.`||` =>
        translateCondition(a, alias, ctx).or(translateCondition(b, alias, ctx))

      case SetOperator.`contains` =>
        // a.contains(b) => b IN a
        // For liftQuery(Set(1,2)).contains(p.id): a=ScalarTag (collection), b=Property (element)
        (a, b) match {
          case (ScalarTag(uid, _), element) =>
            // Collection is on the left (a), element on the right (b)
            val field = translateField(element, alias, ctx)
            ctx.listBindings.get(uid) match {
              case Some(values) if values.isEmpty => DSL.falseCondition()
              case Some(values) => field.in(values.map(DSL.inline(_))*)
              case None => throw new IllegalStateException(s"No list binding for uid: $uid")
            }
          case (element, ScalarTag(uid, _)) =>
            // Element is on the left (a), collection on the right (b)
            val field = translateField(element, alias, ctx)
            ctx.listBindings.get(uid) match {
              case Some(values) if values.isEmpty => DSL.falseCondition()
              case Some(values) => field.in(values.map(DSL.inline(_))*)
              case None => throw new IllegalStateException(s"No list binding for uid: $uid")
            }
          case _ =>
            // Fallback - try standard field.in(field)
            val left = translateField(a, alias, ctx).asInstanceOf[Field[Object]]
            val right = translateField(b, alias, ctx).asInstanceOf[Field[Object]]
            left.in(right)
        }

      case _ =>
        val left = translateField(a, alias, ctx).asInstanceOf[Field[Object]]
        val right = translateField(b, alias, ctx).asInstanceOf[Field[Object]]

        op match {
          case EqualityOperator.`_==` => left.equal(right)
          case EqualityOperator.`_!=` => left.notEqual(right)
          case NumericOperator.`>` => left.asInstanceOf[Field[Comparable[Object]]].gt(right.asInstanceOf[Field[Comparable[Object]]])
          case NumericOperator.`>=` => left.asInstanceOf[Field[Comparable[Object]]].ge(right.asInstanceOf[Field[Comparable[Object]]])
          case NumericOperator.`<` => left.asInstanceOf[Field[Comparable[Object]]].lt(right.asInstanceOf[Field[Comparable[Object]]])
          case NumericOperator.`<=` => left.asInstanceOf[Field[Comparable[Object]]].le(right.asInstanceOf[Field[Comparable[Object]]])
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported binary condition operator: $op")
        }
    }
  }

  /**
   * Translate ordering (ORDER BY)
   */
  def translateOrdering(sortBy: Ast, ordering: Ast, alias: String, ctx: TranslationContext): Seq[SortField[?]] = {
    val fields = sortBy match {
      case Tuple(values) => values.map(v => translateField(v, alias, ctx))
      case _ => Seq(translateField(sortBy, alias, ctx))
    }

    val orderings = ordering match {
      case TupleOrdering(values) => values
      case o => Seq(o)
    }

    fields.zip(orderings).map { case (field, ord) =>
      ord match {
        case Asc | AscNullsFirst => field.asc()
        case Desc | DescNullsFirst => field.desc()
        case AscNullsLast => field.asc().nullsLast()
        case DescNullsLast => field.desc().nullsLast()
        case _ => field.asc()
      }
    }
  }

  /**
   * Translate GroupByMap (groupByMap in Quill DSL)
   * query[Person].groupByMap(p => p.id)(p => (p.name, max(p.age)))
   * => SELECT name, MAX(age) FROM Person GROUP BY id
   */
  def translateGroupByMap(
    query: Ast,
    byAlias: Ident,
    byBody: Ast,
    mapAlias: Ident,
    mapBody: Ast,
    ctx: TranslationContext,
    dslCtx: DSLContext
  ): ResultQuery[Record] = {
    // Extract table and optional WHERE condition
    val (entity, whereCondition) = query match {
      case e: Entity => (e, None)
      case Filter(e: Entity, filterAlias, predicate) =>
        (e, Some(translateCondition(predicate, filterAlias.name, ctx)))
      case _ =>
        throw new UnsupportedOperationException("Complex GroupByMap source queries not yet supported")
    }

    val table = translateEntity(entity, ctx)

    // Translate SELECT fields (mapBody)
    val selectFields = translateProjection(mapBody, mapAlias.name, ctx)

    // Translate GROUP BY fields (byBody)
    val groupByFields = byBody match {
      case Tuple(values) => values.map(v => translateField(v, byAlias.name, ctx).asInstanceOf[GroupField])
      case _ => Seq(translateField(byBody, byAlias.name, ctx).asInstanceOf[GroupField])
    }

    // Build query
    val selectStep = dslCtx.select(selectFields*).from(table)
    val withWhere = whereCondition.fold(selectStep)(cond => selectStep.where(cond))
    withWhere.groupBy(groupByFields*)
  }

  /**
   * Extract integer from Constant AST
   */
  def translateConstantInt(ast: Ast): Int = {
    ast match {
      case Constant(v: Int, _) => v
      case Constant(v: Long, _) => v.toInt
      case _ => throw new IllegalArgumentException(s"Expected integer constant, got: ${ast.getClass.getSimpleName}")
    }
  }

  // ========== Action Translation ==========

  /**
   * Translate INSERT action
   */
  def translateInsert(ast: Insert, ctx: TranslationContext, dslCtx: DSLContext): InsertSetMoreStep[Record] = {
    val entity = ast.query match {
      case e: Entity => e
      case _ => throw new UnsupportedOperationException("Insert must target an entity")
    }
    val table = translateEntity(entity, ctx)
    var insertStep = dslCtx.insertInto(table).asInstanceOf[InsertSetStep[Record]]

    ast.assignments.foreach { assignment =>
      val propName = assignment.property match {
        case Property(_, name) => name
        case _ => throw new UnsupportedOperationException("Assignment must have a property")
      }
      val colName = ctx.applyColumnNaming(propName)
      val field = DSL.field(DSL.name(colName))
      val value = translateFieldValue(assignment.value, "", ctx)
      insertStep = insertStep.set(field.asInstanceOf[Field[Any]], value).asInstanceOf[InsertSetStep[Record]]
    }

    insertStep.asInstanceOf[InsertSetMoreStep[Record]]
  }

  /**
   * Translate UPDATE action
   */
  def translateUpdate(ast: Update, ctx: TranslationContext, dslCtx: DSLContext): UpdateConditionStep[Record] = {
    val (entity, whereCondition) = ast.query match {
      case Filter(e: Entity, alias, predicate) =>
        (e, Some(translateCondition(predicate, alias.name, ctx)))
      case e: Entity =>
        (e, None)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported update query")
    }

    val table = translateEntity(entity, ctx)
    var updateStep = dslCtx.update(table).asInstanceOf[UpdateSetFirstStep[Record]]

    ast.assignments.foreach { assignment =>
      val propName = assignment.property match {
        case Property(_, name) => name
        case _ => throw new UnsupportedOperationException("Assignment must have a property")
      }
      val colName = ctx.applyColumnNaming(propName)
      val field = DSL.field(DSL.name(colName))
      val value = translateFieldValue(assignment.value, "", ctx)
      updateStep = updateStep.set(field.asInstanceOf[Field[Any]], value).asInstanceOf[UpdateSetFirstStep[Record]]
    }

    whereCondition match {
      case Some(cond) => updateStep.asInstanceOf[UpdateSetMoreStep[Record]].where(cond)
      case None => updateStep.asInstanceOf[UpdateSetMoreStep[Record]].where(DSL.trueCondition())
    }
  }

  /**
   * Translate DELETE action
   */
  def translateDelete(ast: Delete, ctx: TranslationContext, dslCtx: DSLContext): DeleteConditionStep[Record] = {
    val (entity, whereCondition) = ast.query match {
      case Filter(e: Entity, alias, predicate) =>
        (e, Some(translateCondition(predicate, alias.name, ctx)))
      case e: Entity =>
        (e, None)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported delete query")
    }

    val table = translateEntity(entity, ctx)
    val deleteStep = dslCtx.deleteFrom(table)

    whereCondition match {
      case Some(cond) => deleteStep.where(cond)
      case None => deleteStep.where(DSL.trueCondition())
    }
  }

  /**
   * Translate field value for INSERT/UPDATE
   */
  def translateFieldValue(ast: Ast, alias: String, ctx: TranslationContext): Any = {
    ast match {
      case Constant(v, _) => v
      case ScalarTag(uid, _) =>
        // Look up the binding value directly for INSERT/UPDATE
        ctx.bindings.find(_._1 == uid) match {
          case Some((_, value)) => value
          case None => DSL.param(uid, classOf[Object])
        }
      case NullValue => null
      case _ => translateField(ast, alias, ctx)
    }
  }
}
