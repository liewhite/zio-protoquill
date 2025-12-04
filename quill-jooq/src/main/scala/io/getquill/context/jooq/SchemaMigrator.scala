package io.getquill.context.jooq

import io.getquill.NamingStrategy
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import zio.{ZIO, ZLayer}

import java.sql.{Connection, SQLException}
import javax.sql.DataSource

/**
 * Schema migrator that compares code-defined schemas with database schemas
 * and applies the differences automatically.
 *
 * Usage:
 * ```scala
 * case class Person(id: Int, name: String, age: Int)
 * case class Address(id: Int, personId: Int, street: String, city: Option[String])
 *
 * val migrator = SchemaMigrator(SQLDialect.POSTGRES, Literal)
 *
 * // Auto-migrate a single table
 * val effect = migrator.migrate[Person](
 *   renameMapping = None  // Will error on ambiguous changes
 * )
 *
 * // Auto-migrate with rename mapping for renamed column
 * val effectWithRename = migrator.migrate[Person](
 *   renameMapping = Some(Map("old_name" -> "new_name"))
 * )
 *
 * // Auto-migrate with explicit delete+add (no renames)
 * val effectWithDeleteAdd = migrator.migrate[Person](
 *   renameMapping = Some(Map.empty)
 * )
 * ```
 *
 * @param dialect SQL dialect for generating DDL
 * @param naming Naming strategy for table/column names
 */
class SchemaMigrator[+N <: NamingStrategy](
  val dialect: SQLDialect,
  val naming: N
) {

  import SchemaMigration.*

  type MigrationResult = ZIO[DataSource, SQLException, MigrationReport]

  /**
   * Migrate a single table schema
   *
   * @tparam T The case class representing the table
   * @param renameMapping Optional mapping for column renames:
   *                      - None: Throw error if ambiguous changes detected
   *                      - Some(Map.empty): Treat as delete + add (no renames)
   *                      - Some(Map(old -> new, ...)): Apply rename mappings
   * @return MigrationReport with details of changes applied
   */
  inline def migrate[T](
    renameMapping: Option[Map[String, String]] = None
  ): MigrationResult = {
    val codeSchema = SchemaMacro.extractSchema[T](naming)
    migrateTable(codeSchema, renameMapping)
  }

  /**
   * Migrate a single table schema with explicit table name
   */
  inline def migrate[T](
    tableName: String,
    renameMapping: Option[Map[String, String]]
  ): MigrationResult = {
    val codeSchema = SchemaMacro.extractSchema[T](tableName, naming)
    migrateTable(codeSchema, renameMapping)
  }

  /**
   * Migrate multiple tables at once
   * Returns a combined report of all migrations
   */
  def migrateAll(
    schemas: List[(TableSchema, Option[Map[String, String]])]
  ): ZIO[DataSource, SQLException, List[MigrationReport]] = {
    ZIO.foreach(schemas) { case (schema, mapping) =>
      migrateTable(schema, mapping)
    }
  }

  /**
   * Preview migration without executing (dry-run)
   * Returns the DDL statements that would be executed
   */
  inline def preview[T](
    renameMapping: Option[Map[String, String]] = None
  ): ZIO[DataSource, SQLException, MigrationPreview] = {
    val codeSchema = SchemaMacro.extractSchema[T](naming)
    previewMigration(codeSchema, renameMapping)
  }

  /**
   * Check if migration is needed for a table
   */
  inline def needsMigration[T]: ZIO[DataSource, SQLException, Boolean] = {
    val codeSchema = SchemaMacro.extractSchema[T](naming)
    checkMigrationNeeded(codeSchema)
  }

  // ========== Internal Implementation ==========

  private def migrateTable(
    codeSchema: TableSchema,
    renameMapping: Option[Map[String, String]]
  ): MigrationResult = {
    ZIO.serviceWithZIO[DataSource] { ds =>
      ZIO.attemptBlocking {
        val conn = ds.getConnection()
        try {
          migrateTableInternal(codeSchema, renameMapping, conn)
        } finally {
          conn.close()
        }
      }.refineToOrDie[SQLException]
    }
  }

  private def migrateTableInternal(
    codeSchema: TableSchema,
    renameMapping: Option[Map[String, String]],
    conn: Connection
  ): MigrationReport = {
    // Create jOOQ DSLContext with connection - jOOQ handles execution
    val dslCtx = DSL.using(conn, dialect)

    // Check if table exists
    if (!tableExists(codeSchema.name, naming, conn)) {
      // Create new table using jOOQ
      val ddl = executeCreateTable(codeSchema, dslCtx)
      return MigrationReport(
        tableName = codeSchema.name,
        action = MigrationAction.Created,
        statements = List(ddl),
        changes = codeSchema.columns.map(c => ColumnChange.Added(c.name, c.typeName))
      )
    }

    // Extract current schema from database
    val dbSchema = extractDbSchema(codeSchema.name, naming, conn)

    // Compare schemas
    compareSchemas(codeSchema, dbSchema, renameMapping) match {
      case Left(ambiguity) =>
        throw new SQLException(ambiguity.getMessage)

      case Right(diff) if diff.isEmpty =>
        MigrationReport(
          tableName = codeSchema.name,
          action = MigrationAction.NoChanges,
          statements = List.empty,
          changes = List.empty
        )

      case Right(diff) =>
        // Execute DDL using jOOQ (returns executed SQL for reporting)
        val ddlStatements = executeDDL(diff, dslCtx)

        val changes = diff.diffs.map {
          case ColumnDiff.Add(col) => ColumnChange.Added(col.name, col.typeName)
          case ColumnDiff.Drop(name) => ColumnChange.Dropped(name)
          case ColumnDiff.Rename(oldName, newName) => ColumnChange.Renamed(oldName, newName)
          case ColumnDiff.Modify(oldCol, newCol) => ColumnChange.Modified(oldCol.name, oldCol.typeName, newCol.typeName)
        }

        MigrationReport(
          tableName = codeSchema.name,
          action = MigrationAction.Altered,
          statements = ddlStatements,
          changes = changes
        )
    }
  }

  private def previewMigration(
    codeSchema: TableSchema,
    renameMapping: Option[Map[String, String]]
  ): ZIO[DataSource, SQLException, MigrationPreview] = {
    ZIO.serviceWithZIO[DataSource] { ds =>
      ZIO.attemptBlocking {
        val conn = ds.getConnection()
        try {
          previewMigrationInternal(codeSchema, renameMapping, conn)
        } finally {
          conn.close()
        }
      }.refineToOrDie[SQLException]
    }
  }

  private def previewMigrationInternal(
    codeSchema: TableSchema,
    renameMapping: Option[Map[String, String]],
    conn: Connection
  ): MigrationPreview = {
    // Use jOOQ DSLContext for generating DDL (without connection for preview-only)
    val dslCtx = DSL.using(dialect)

    if (!tableExists(codeSchema.name, naming, conn)) {
      val ddl = generateCreateTable(codeSchema, dslCtx)
      MigrationPreview(
        tableName = codeSchema.name,
        tableExists = false,
        statements = List(ddl),
        diff = None
      )
    } else {
      val dbSchema = extractDbSchema(codeSchema.name, naming, conn)
      compareSchemas(codeSchema, dbSchema, renameMapping) match {
        case Left(ambiguity) =>
          throw new SQLException(ambiguity.getMessage)
        case Right(diff) =>
          val ddlStatements = if (diff.isEmpty) List.empty else generateDDL(diff, dslCtx)
          MigrationPreview(
            tableName = codeSchema.name,
            tableExists = true,
            statements = ddlStatements,
            diff = Some(diff)
          )
      }
    }
  }

  private def checkMigrationNeeded(
    codeSchema: TableSchema
  ): ZIO[DataSource, SQLException, Boolean] = {
    ZIO.serviceWithZIO[DataSource] { ds =>
      ZIO.attemptBlocking {
        val conn = ds.getConnection()
        try {
          if (!tableExists(codeSchema.name, naming, conn)) {
            true
          } else {
            val dbSchema = extractDbSchema(codeSchema.name, naming, conn)
            compareSchemas(codeSchema, dbSchema, Some(Map.empty)) match {
              case Left(_) => true // Ambiguous = needs migration
              case Right(diff) => diff.hasChanges
            }
          }
        } finally {
          conn.close()
        }
      }.refineToOrDie[SQLException]
    }
  }
}

// ========== Report Types ==========

/**
 * Result of a migration operation
 */
case class MigrationReport(
  tableName: String,
  action: MigrationAction,
  statements: List[String],
  changes: List[ColumnChange]
) {
  def summary: String = {
    val changesSummary = changes.map {
      case ColumnChange.Added(name, typeName) => s"  + $name ($typeName)"
      case ColumnChange.Dropped(name) => s"  - $name"
      case ColumnChange.Renamed(oldName, newName) => s"  ~ $oldName -> $newName"
      case ColumnChange.Modified(name, oldType, newType) => s"  ! $name: $oldType -> $newType"
    }.mkString("\n")

    s"""Migration Report for '$tableName':
       |Action: $action
       |Changes:
       |$changesSummary
       |Statements executed:
       |${statements.mkString("\n")}""".stripMargin
  }
}

sealed trait MigrationAction
object MigrationAction {
  case object Created extends MigrationAction
  case object Altered extends MigrationAction
  case object NoChanges extends MigrationAction
}

sealed trait ColumnChange
object ColumnChange {
  case class Added(name: String, typeName: String) extends ColumnChange
  case class Dropped(name: String) extends ColumnChange
  case class Renamed(oldName: String, newName: String) extends ColumnChange
  case class Modified(name: String, oldType: String, newType: String) extends ColumnChange
}

/**
 * Preview of migration without execution
 */
case class MigrationPreview(
  tableName: String,
  tableExists: Boolean,
  statements: List[String],
  diff: Option[SchemaMigration.SchemaDiff]
) {
  def summary: String = {
    val existsStr = if (tableExists) "exists" else "will be created"
    val stmtsStr = if (statements.isEmpty) "No changes needed" else statements.mkString("\n")
    s"""Migration Preview for '$tableName' (table $existsStr):
       |$stmtsStr""".stripMargin
  }
}

// ========== Companion Object ==========

object SchemaMigrator {
  def apply[N <: NamingStrategy](dialect: SQLDialect, naming: N): SchemaMigrator[N] =
    new SchemaMigrator(dialect, naming)
}
