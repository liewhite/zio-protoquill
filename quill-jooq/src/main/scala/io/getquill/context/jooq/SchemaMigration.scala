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

  // ========== DDL Generation ==========

  /**
   * Generate DDL statements for a schema diff
   *
   * @param diff Schema diff to generate DDL for
   * @param dialect SQL dialect to use
   * @return List of DDL statements
   */
  def generateDDL(
    diff: SchemaDiff,
    dialect: SQLDialect
  ): List[String] = {
    diff.diffs.flatMap { d =>
      generateColumnDDL(diff.tableName, d, dialect)
    }
  }

  private def generateColumnDDL(
    tableName: String,
    diff: ColumnDiff,
    dialect: SQLDialect
  ): List[String] = {
    diff match {
      case ColumnDiff.Add(column) =>
        val nullStr = if (column.nullable) "" else " NOT NULL"
        val defaultStr = column.defaultValue.map(v => s" DEFAULT $v").getOrElse("")
        List(s"ALTER TABLE $tableName ADD COLUMN ${column.name} ${column.typeName}$nullStr$defaultStr")

      case ColumnDiff.Drop(colName) =>
        List(s"ALTER TABLE $tableName DROP COLUMN $colName")

      case ColumnDiff.Rename(oldName, newName) =>
        dialect match {
          case SQLDialect.MYSQL | SQLDialect.MARIADB =>
            // MySQL uses RENAME COLUMN in 8.0+
            List(s"ALTER TABLE $tableName RENAME COLUMN $oldName TO $newName")
          case SQLDialect.POSTGRES =>
            List(s"ALTER TABLE $tableName RENAME COLUMN $oldName TO $newName")
          case SQLDialect.SQLITE =>
            List(s"ALTER TABLE $tableName RENAME COLUMN $oldName TO $newName")
          case SQLDialect.H2 =>
            List(s"ALTER TABLE $tableName ALTER COLUMN $oldName RENAME TO $newName")
          case _ =>
            List(s"ALTER TABLE $tableName RENAME COLUMN $oldName TO $newName")
        }

      case ColumnDiff.Modify(oldCol, newCol) =>
        val nullStr = if (newCol.nullable) " NULL" else " NOT NULL"
        dialect match {
          case SQLDialect.MYSQL | SQLDialect.MARIADB =>
            List(s"ALTER TABLE $tableName MODIFY COLUMN ${newCol.name} ${newCol.typeName}$nullStr")
          case SQLDialect.POSTGRES =>
            // PostgreSQL requires separate statements for type and nullable
            val stmts = mutable.ListBuffer.empty[String]
            if (oldCol.sqlType != newCol.sqlType) {
              stmts += s"ALTER TABLE $tableName ALTER COLUMN ${newCol.name} TYPE ${newCol.typeName}"
            }
            if (oldCol.nullable != newCol.nullable) {
              if (newCol.nullable) {
                stmts += s"ALTER TABLE $tableName ALTER COLUMN ${newCol.name} DROP NOT NULL"
              } else {
                stmts += s"ALTER TABLE $tableName ALTER COLUMN ${newCol.name} SET NOT NULL"
              }
            }
            stmts.toList
          case _ =>
            List(s"ALTER TABLE $tableName ALTER COLUMN ${newCol.name} ${newCol.typeName}$nullStr")
        }
    }
  }

  /**
   * Generate CREATE TABLE statement for a new table
   */
  def generateCreateTable(
    schema: TableSchema,
    dialect: SQLDialect
  ): String = {
    val columns = schema.columns.map { col =>
      val nullStr = if (col.nullable) "" else " NOT NULL"
      val defaultStr = col.defaultValue.map(v => s" DEFAULT $v").getOrElse("")
      s"  ${col.name} ${col.typeName}$nullStr$defaultStr"
    }
    s"CREATE TABLE ${schema.name} (\n${columns.mkString(",\n")}\n)"
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
