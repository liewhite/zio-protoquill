package io.getquill.context.jooq

import io.getquill.NamingStrategy
import org.jooq.{DSLContext, SQLDialect}
import org.jooq.impl.DSL
import zio.{ZIO, ZLayer}

import java.sql.{Connection, SQLException, Types}
import javax.sql.DataSource
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/**
 * Schema migration module for comparing code-defined schemas with database schemas
 * and applying the differences.
 */
object SchemaMigration {

  // ========== Schema Data Types ==========

  /**
   * Represents a column in a table schema
   *
   * @param name Column name
   * @param sqlType SQL type (java.sql.Types)
   * @param nullable Whether the column allows NULL
   * @param defaultValue Optional default value
   */
  case class ColumnSchema(
    name: String,
    sqlType: Int,
    nullable: Boolean,
    defaultValue: Option[String] = None
  ) {
    def typeName: String = sqlTypeToName(sqlType)
  }

  /**
   * Represents a table schema
   *
   * @param name Table name
   * @param columns List of columns
   */
  case class TableSchema(
    name: String,
    columns: List[ColumnSchema]
  ) {
    def columnMap: Map[String, ColumnSchema] = columns.map(c => c.name -> c).toMap
    def columnNames: Set[String] = columns.map(_.name).toSet
  }

  /**
   * Represents a diff operation for a column
   */
  sealed trait ColumnDiff
  object ColumnDiff {
    case class Add(column: ColumnSchema) extends ColumnDiff
    case class Drop(columnName: String) extends ColumnDiff
    case class Rename(oldName: String, newName: String) extends ColumnDiff
    case class Modify(oldColumn: ColumnSchema, newColumn: ColumnSchema) extends ColumnDiff
  }

  /**
   * Represents the complete diff between two table schemas
   *
   * @param tableName Table name
   * @param diffs List of column differences
   */
  case class SchemaDiff(
    tableName: String,
    diffs: List[ColumnDiff]
  ) {
    def isEmpty: Boolean = diffs.isEmpty
    def hasChanges: Boolean = diffs.nonEmpty
  }

  /**
   * Error indicating ambiguous column changes (could be rename or drop+add)
   */
  case class AmbiguousColumnChange(
    tableName: String,
    removedColumns: Set[String],
    addedColumns: Set[String]
  ) extends Exception(
    s"Ambiguous column changes in table '$tableName': " +
    s"removed columns [${removedColumns.mkString(", ")}], " +
    s"added columns [${addedColumns.mkString(", ")}]. " +
    s"Please specify a rename mapping or confirm delete+add with empty mapping."
  )

  // ========== Schema Comparison ==========

  /**
   * Compare two table schemas and compute the diff
   *
   * @param codeSchema Schema defined in code (source of truth)
   * @param dbSchema Schema from database
   * @param renameMapping Optional mapping for column renames:
   *                      - None: Throw error if ambiguous changes detected
   *                      - Some(Map.empty): Treat as delete + add (no renames)
   *                      - Some(Map(old -> new, ...)): Apply rename mappings
   * @return Either an error or the schema diff
   */
  def compareSchemas(
    codeSchema: TableSchema,
    dbSchema: TableSchema,
    renameMapping: Option[Map[String, String]]
  ): Either[AmbiguousColumnChange, SchemaDiff] = {
    val codeColumns = codeSchema.columnMap
    val dbColumns = dbSchema.columnMap

    val codeColumnNames = codeSchema.columnNames
    val dbColumnNames = dbSchema.columnNames

    // Columns to add (in code but not in DB)
    val toAdd = codeColumnNames -- dbColumnNames
    // Columns to remove (in DB but not in code)
    val toRemove = dbColumnNames -- codeColumnNames
    // Columns in both (check for modifications)
    val inBoth = codeColumnNames.intersect(dbColumnNames)

    val diffs = mutable.ListBuffer.empty[ColumnDiff]

    // Handle columns that exist in both - check for modifications
    for (colName <- inBoth) {
      val codeCol = codeColumns(colName)
      val dbCol = dbColumns(colName)
      if (codeCol.sqlType != dbCol.sqlType || codeCol.nullable != dbCol.nullable) {
        diffs += ColumnDiff.Modify(dbCol, codeCol)
      }
    }

    // Handle renamed/added/removed columns
    renameMapping match {
      case None =>
        // No mapping provided - check for ambiguity
        if (toAdd.nonEmpty && toRemove.nonEmpty) {
          // Could be renames - return error
          return Left(AmbiguousColumnChange(codeSchema.name, toRemove, toAdd))
        }
        // No ambiguity - proceed with simple add/remove
        for (colName <- toRemove) {
          diffs += ColumnDiff.Drop(colName)
        }
        for (colName <- toAdd) {
          diffs += ColumnDiff.Add(codeColumns(colName))
        }

      case Some(mapping) if mapping.isEmpty =>
        // Empty mapping - treat as delete + add (no renames)
        for (colName <- toRemove) {
          diffs += ColumnDiff.Drop(colName)
        }
        for (colName <- toAdd) {
          diffs += ColumnDiff.Add(codeColumns(colName))
        }

      case Some(mapping) =>
        // Apply rename mappings
        val renamedOld = mapping.keySet
        val renamedNew = mapping.values.toSet

        // Validate mapping
        val invalidOld = renamedOld -- toRemove
        val invalidNew = renamedNew -- toAdd
        if (invalidOld.nonEmpty) {
          throw new IllegalArgumentException(
            s"Rename mapping references non-existent old columns: ${invalidOld.mkString(", ")}"
          )
        }
        if (invalidNew.nonEmpty) {
          throw new IllegalArgumentException(
            s"Rename mapping references non-existent new columns: ${invalidNew.mkString(", ")}"
          )
        }

        // Apply renames
        for ((oldName, newName) <- mapping) {
          diffs += ColumnDiff.Rename(oldName, newName)
        }

        // Remaining removes (not renamed)
        for (colName <- toRemove -- renamedOld) {
          diffs += ColumnDiff.Drop(colName)
        }

        // Remaining adds (not renamed)
        for (colName <- toAdd -- renamedNew) {
          diffs += ColumnDiff.Add(codeColumns(colName))
        }
    }

    Right(SchemaDiff(codeSchema.name, diffs.toList))
  }

  // ========== Database Schema Extraction ==========

  /**
   * Extract table schema from database using JDBC metadata.
   * Note: tableName should already have naming strategy applied.
   *
   * @param tableName Table name to query (already with naming applied)
   * @param naming Naming strategy (not used for table name, kept for consistency)
   * @param conn Database connection
   * @return Table schema from database
   */
  def extractDbSchema(
    tableName: String,
    naming: NamingStrategy,
    conn: Connection
  ): TableSchema = {
    val meta = conn.getMetaData
    val columns = mutable.ListBuffer.empty[ColumnSchema]

    // Try both the exact name and case variations for cross-database compatibility
    val rs = meta.getColumns(null, null, tableName, null)
    try {
      while (rs.next()) {
        val colName = rs.getString("COLUMN_NAME")
        val sqlType = rs.getInt("DATA_TYPE")
        val nullable = rs.getInt("NULLABLE") == java.sql.DatabaseMetaData.columnNullable
        val defaultValue = Option(rs.getString("COLUMN_DEF"))

        columns += ColumnSchema(colName, sqlType, nullable, defaultValue)
      }
    } finally {
      rs.close()
    }

    // If no columns found, try uppercase (some databases like H2 uppercase table names)
    if (columns.isEmpty && tableName != tableName.toUpperCase) {
      val rsUpper = meta.getColumns(null, null, tableName.toUpperCase, null)
      try {
        while (rsUpper.next()) {
          val colName = rsUpper.getString("COLUMN_NAME")
          val sqlType = rsUpper.getInt("DATA_TYPE")
          val nullable = rsUpper.getInt("NULLABLE") == java.sql.DatabaseMetaData.columnNullable
          val defaultValue = Option(rsUpper.getString("COLUMN_DEF"))

          columns += ColumnSchema(colName, sqlType, nullable, defaultValue)
        }
      } finally {
        rsUpper.close()
      }
    }

    TableSchema(tableName, columns.toList)
  }

  /**
   * Check if a table exists in the database.
   * Note: tableName should already have naming strategy applied.
   */
  def tableExists(
    tableName: String,
    naming: NamingStrategy,
    conn: Connection
  ): Boolean = {
    val meta = conn.getMetaData
    // Try exact name first
    val rs = meta.getTables(null, null, tableName, Array("TABLE"))
    try {
      if (rs.next()) return true
    } finally {
      rs.close()
    }

    // Try uppercase for databases that uppercase names (like H2)
    if (tableName != tableName.toUpperCase) {
      val rsUpper = meta.getTables(null, null, tableName.toUpperCase, Array("TABLE"))
      try {
        rsUpper.next()
      } finally {
        rsUpper.close()
      }
    } else {
      false
    }
  }

  // ========== DDL Execution (using jOOQ DSL) ==========

  /**
   * Execute DDL for a schema diff using jOOQ DSL.
   * jOOQ handles dialect-specific SQL generation and execution.
   *
   * @param diff Schema diff to apply
   * @param dslCtx jOOQ DSLContext with connection and dialect
   * @return List of executed SQL statements (for reporting)
   */
  def executeDDL(
    diff: SchemaDiff,
    dslCtx: DSLContext
  ): List[String] = {
    val table = DSL.table(DSL.name(diff.tableName))
    diff.diffs.flatMap { d =>
      executeColumnDDL(table, d, dslCtx)
    }
  }

  /**
   * Generate DDL statements without executing (for preview/dry-run)
   */
  def generateDDL(
    diff: SchemaDiff,
    dslCtx: DSLContext
  ): List[String] = {
    val table = DSL.table(DSL.name(diff.tableName))
    diff.diffs.flatMap { d =>
      generateColumnDDL(table, d, dslCtx)
    }
  }

  /**
   * Overload for backward compatibility - creates DSLContext from dialect
   */
  def generateDDL(
    diff: SchemaDiff,
    dialect: SQLDialect
  ): List[String] = {
    val dslCtx = DSL.using(dialect)
    generateDDL(diff, dslCtx)
  }

  private def executeColumnDDL(
    table: org.jooq.Table[?],
    diff: ColumnDiff,
    dslCtx: DSLContext
  ): List[String] = {
    diff match {
      case ColumnDiff.Add(column) =>
        val dataType = sqlTypeToJooqDataType(column.sqlType)
        val finalType = if (column.nullable) dataType.nullable(true) else dataType.nullable(false)
        val query = dslCtx.alterTable(table).addColumn(DSL.field(DSL.name(column.name), finalType))
        query.execute()
        List(query.getSQL)

      case ColumnDiff.Drop(colName) =>
        val query = dslCtx.alterTable(table).dropColumn(DSL.field(DSL.name(colName)))
        query.execute()
        List(query.getSQL)

      case ColumnDiff.Rename(oldName, newName) =>
        val query = dslCtx.alterTable(table)
          .renameColumn(DSL.field(DSL.name(oldName)))
          .to(DSL.field(DSL.name(newName)))
        query.execute()
        List(query.getSQL)

      case ColumnDiff.Modify(oldCol, newCol) =>
        val stmts = mutable.ListBuffer.empty[String]
        val field = DSL.field(DSL.name(newCol.name))

        if (oldCol.sqlType != newCol.sqlType) {
          val newType = sqlTypeToJooqDataType(newCol.sqlType)
          val query = dslCtx.alterTable(table).alterColumn(field).set(newType)
          query.execute()
          stmts += query.getSQL
        }

        if (oldCol.nullable != newCol.nullable) {
          val query = if (newCol.nullable) {
            dslCtx.alterTable(table).alterColumn(field).dropNotNull()
          } else {
            dslCtx.alterTable(table).alterColumn(field).setNotNull()
          }
          query.execute()
          stmts += query.getSQL
        }

        stmts.toList
    }
  }

  private def generateColumnDDL(
    table: org.jooq.Table[?],
    diff: ColumnDiff,
    dslCtx: DSLContext
  ): List[String] = {
    diff match {
      case ColumnDiff.Add(column) =>
        val dataType = sqlTypeToJooqDataType(column.sqlType)
        val finalType = if (column.nullable) dataType.nullable(true) else dataType.nullable(false)
        List(dslCtx.alterTable(table).addColumn(DSL.field(DSL.name(column.name), finalType)).getSQL)

      case ColumnDiff.Drop(colName) =>
        List(dslCtx.alterTable(table).dropColumn(DSL.field(DSL.name(colName))).getSQL)

      case ColumnDiff.Rename(oldName, newName) =>
        List(dslCtx.alterTable(table).renameColumn(DSL.field(DSL.name(oldName))).to(DSL.field(DSL.name(newName))).getSQL)

      case ColumnDiff.Modify(oldCol, newCol) =>
        val stmts = mutable.ListBuffer.empty[String]
        val field = DSL.field(DSL.name(newCol.name))

        if (oldCol.sqlType != newCol.sqlType) {
          val newType = sqlTypeToJooqDataType(newCol.sqlType)
          stmts += dslCtx.alterTable(table).alterColumn(field).set(newType).getSQL
        }

        if (oldCol.nullable != newCol.nullable) {
          stmts += (if (newCol.nullable) {
            dslCtx.alterTable(table).alterColumn(field).dropNotNull().getSQL
          } else {
            dslCtx.alterTable(table).alterColumn(field).setNotNull().getSQL
          })
        }

        stmts.toList
    }
  }

  /**
   * Execute CREATE TABLE using jOOQ DSL
   */
  def executeCreateTable(
    schema: TableSchema,
    dslCtx: DSLContext
  ): String = {
    var createStep = dslCtx.createTable(DSL.table(DSL.name(schema.name)))

    schema.columns.foreach { col =>
      val dataType = sqlTypeToJooqDataType(col.sqlType)
      val finalType = if (col.nullable) dataType.nullable(true) else dataType.nullable(false)
      createStep = createStep.column(DSL.field(DSL.name(col.name), finalType))
    }

    createStep.execute()
    createStep.getSQL
  }

  /**
   * Generate CREATE TABLE SQL without executing (for preview)
   */
  def generateCreateTable(
    schema: TableSchema,
    dslCtx: DSLContext
  ): String = {
    var createStep = dslCtx.createTable(DSL.table(DSL.name(schema.name)))

    schema.columns.foreach { col =>
      val dataType = sqlTypeToJooqDataType(col.sqlType)
      val finalType = if (col.nullable) dataType.nullable(true) else dataType.nullable(false)
      createStep = createStep.column(DSL.field(DSL.name(col.name), finalType))
    }

    createStep.getSQL
  }

  /**
   * Overload for backward compatibility
   */
  def generateCreateTable(
    schema: TableSchema,
    dialect: SQLDialect
  ): String = {
    val dslCtx = DSL.using(dialect)
    generateCreateTable(schema, dslCtx)
  }

  /**
   * Convert java.sql.Types to jOOQ DataType
   */
  private def sqlTypeToJooqDataType(sqlType: Int): org.jooq.DataType[?] = {
    import org.jooq.impl.SQLDataType
    sqlType match {
      case Types.INTEGER => SQLDataType.INTEGER
      case Types.BIGINT => SQLDataType.BIGINT
      case Types.SMALLINT => SQLDataType.SMALLINT
      case Types.TINYINT => SQLDataType.TINYINT
      case Types.FLOAT => SQLDataType.FLOAT
      case Types.DOUBLE => SQLDataType.DOUBLE
      case Types.REAL => SQLDataType.REAL
      case Types.DECIMAL | Types.NUMERIC => SQLDataType.DECIMAL
      case Types.VARCHAR => SQLDataType.VARCHAR(255)
      case Types.CHAR => SQLDataType.CHAR(1)
      case Types.LONGVARCHAR | Types.CLOB => SQLDataType.CLOB
      case Types.BOOLEAN | Types.BIT => SQLDataType.BOOLEAN
      case Types.DATE => SQLDataType.DATE
      case Types.TIME => SQLDataType.TIME
      case Types.TIMESTAMP => SQLDataType.TIMESTAMP
      case Types.BLOB | Types.BINARY | Types.VARBINARY => SQLDataType.BLOB
      case _ => SQLDataType.VARCHAR(255)
    }
  }

  // ========== SQL Type Helpers ==========

  def sqlTypeToName(sqlType: Int): String = sqlType match {
    case Types.INTEGER => "INTEGER"
    case Types.BIGINT => "BIGINT"
    case Types.SMALLINT => "SMALLINT"
    case Types.TINYINT => "TINYINT"
    case Types.FLOAT => "FLOAT"
    case Types.DOUBLE => "DOUBLE"
    case Types.REAL => "REAL"
    case Types.DECIMAL | Types.NUMERIC => "DECIMAL"
    case Types.VARCHAR => "VARCHAR(255)"
    case Types.CHAR => "CHAR(1)"
    case Types.LONGVARCHAR | Types.CLOB => "TEXT"
    case Types.BOOLEAN | Types.BIT => "BOOLEAN"
    case Types.DATE => "DATE"
    case Types.TIME => "TIME"
    case Types.TIMESTAMP => "TIMESTAMP"
    case Types.BLOB | Types.BINARY | Types.VARBINARY => "BLOB"
    case _ => "VARCHAR(255)"
  }

  def scalaTypeToSqlType(typeName: String): Int = typeName match {
    case "Int" | "Integer" | "java.lang.Integer" => Types.INTEGER
    case "Long" | "java.lang.Long" => Types.BIGINT
    case "Short" | "java.lang.Short" => Types.SMALLINT
    case "Byte" | "java.lang.Byte" => Types.TINYINT
    case "Float" | "java.lang.Float" => Types.FLOAT
    case "Double" | "java.lang.Double" => Types.DOUBLE
    case "BigDecimal" | "java.math.BigDecimal" | "scala.math.BigDecimal" => Types.DECIMAL
    case "String" | "java.lang.String" => Types.VARCHAR
    case "Boolean" | "java.lang.Boolean" => Types.BOOLEAN
    case "java.time.LocalDate" => Types.DATE
    case "java.time.LocalTime" => Types.TIME
    case "java.time.LocalDateTime" | "java.time.Instant" | "java.time.OffsetDateTime" | "java.time.ZonedDateTime" => Types.TIMESTAMP
    case "Array[Byte]" => Types.BLOB
    case t if t.startsWith("Option[") => scalaTypeToSqlType(t.drop(7).dropRight(1))
    case _ => Types.VARCHAR
  }
}
