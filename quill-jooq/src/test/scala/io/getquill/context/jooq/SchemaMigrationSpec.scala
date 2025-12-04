package io.getquill.context.jooq

import io.getquill.*
import org.jooq.SQLDialect
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfterAll
import zio.*

import java.sql.{Connection, DriverManager, Types}
import javax.sql.DataSource
import java.io.File

/**
 * Unit tests for schema migration functionality.
 */
class SchemaMigrationSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  import SchemaMigration.*

  // ========== Schema Comparison Tests ==========

  "Schema comparison" - {

    "should detect no changes when schemas are identical" in {
      val schema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false),
        ColumnSchema("age", Types.INTEGER, true)
      ))

      val result = compareSchemas(schema, schema, None)
      result mustBe Right(SchemaDiff("users", List.empty))
    }

    "should detect added columns" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false),
        ColumnSchema("email", Types.VARCHAR, true) // new column
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false)
      ))

      val result = compareSchemas(codeSchema, dbSchema, None)
      result mustBe Right(SchemaDiff("users", List(
        ColumnDiff.Add(ColumnSchema("email", Types.VARCHAR, true))
      )))
    }

    "should detect dropped columns" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false)
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("legacy_field", Types.VARCHAR, true)
      ))

      val result = compareSchemas(codeSchema, dbSchema, None)
      result mustBe Right(SchemaDiff("users", List(
        ColumnDiff.Drop("legacy_field")
      )))
    }

    "should detect modified columns (type change)" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("age", Types.BIGINT, false) // changed from INTEGER to BIGINT
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("age", Types.INTEGER, false)
      ))

      val result = compareSchemas(codeSchema, dbSchema, None)
      result mustBe Right(SchemaDiff("users", List(
        ColumnDiff.Modify(
          ColumnSchema("age", Types.INTEGER, false),
          ColumnSchema("age", Types.BIGINT, false)
        )
      )))
    }

    "should detect modified columns (nullable change)" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false) // changed to NOT NULL
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, true)
      ))

      val result = compareSchemas(codeSchema, dbSchema, None)
      result mustBe Right(SchemaDiff("users", List(
        ColumnDiff.Modify(
          ColumnSchema("name", Types.VARCHAR, true),
          ColumnSchema("name", Types.VARCHAR, false)
        )
      )))
    }

    "should return error for ambiguous changes (None mapping)" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("full_name", Types.VARCHAR, false) // renamed from 'name'?
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false)
      ))

      val result = compareSchemas(codeSchema, dbSchema, None)
      result mustBe a[Left[?, ?]]
      result.left.get mustBe a[AmbiguousColumnChange]
      result.left.get.removedColumns mustBe Set("name")
      result.left.get.addedColumns mustBe Set("full_name")
    }

    "should treat as delete+add with empty mapping" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("full_name", Types.VARCHAR, false)
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false)
      ))

      val result = compareSchemas(codeSchema, dbSchema, Some(Map.empty))
      result mustBe Right(SchemaDiff("users", List(
        ColumnDiff.Drop("name"),
        ColumnDiff.Add(ColumnSchema("full_name", Types.VARCHAR, false))
      )))
    }

    "should apply rename mapping" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("full_name", Types.VARCHAR, false)
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false)
      ))

      val result = compareSchemas(codeSchema, dbSchema, Some(Map("name" -> "full_name")))
      result mustBe Right(SchemaDiff("users", List(
        ColumnDiff.Rename("name", "full_name")
      )))
    }

    "should handle mixed rename and add/drop" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("full_name", Types.VARCHAR, false), // renamed from name
        ColumnSchema("email", Types.VARCHAR, true)       // new
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false),
        ColumnSchema("phone", Types.VARCHAR, true)       // to be dropped
      ))

      val result = compareSchemas(codeSchema, dbSchema, Some(Map("name" -> "full_name")))
      result.isRight mustBe true
      val diff = result.toOption.get
      diff.diffs must contain(ColumnDiff.Rename("name", "full_name"))
      diff.diffs must contain(ColumnDiff.Drop("phone"))
      diff.diffs must contain(ColumnDiff.Add(ColumnSchema("email", Types.VARCHAR, true)))
    }

    "should throw error for invalid rename mapping (non-existent old column)" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("full_name", Types.VARCHAR, false)
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false)
      ))

      assertThrows[IllegalArgumentException] {
        compareSchemas(codeSchema, dbSchema, Some(Map("nonexistent" -> "full_name")))
      }
    }

    "should throw error for invalid rename mapping (non-existent new column)" in {
      val codeSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("full_name", Types.VARCHAR, false)
      ))
      val dbSchema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false)
      ))

      assertThrows[IllegalArgumentException] {
        compareSchemas(codeSchema, dbSchema, Some(Map("name" -> "nonexistent")))
      }
    }
  }

  // ========== DDL Generation Tests (using jOOQ) ==========

  "DDL generation" - {

    "should generate ADD COLUMN statement" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Add(ColumnSchema("email", Types.VARCHAR, true))
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.size mustBe 1
      // jOOQ generates lowercase SQL with quoted identifiers
      ddl.head.toLowerCase must include("alter table")
      ddl.head.toLowerCase must include("add")
      ddl.head must include("email")
      ddl.head.toLowerCase must include("varchar")
    }

    "should generate ADD COLUMN with NOT NULL" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Add(ColumnSchema("email", Types.VARCHAR, false))
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.head.toLowerCase must include("not null")
    }

    "should generate DROP COLUMN statement" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Drop("legacy_field")
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.size mustBe 1
      ddl.head.toLowerCase must include("alter table")
      ddl.head.toLowerCase must include("drop")
      ddl.head must include("legacy_field")
    }

    "should generate RENAME COLUMN statement (Postgres)" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Rename("name", "full_name")
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.size mustBe 1
      ddl.head.toLowerCase must include("rename")
      ddl.head must include("name")
      ddl.head must include("full_name")
    }

    "should generate RENAME COLUMN statement (H2)" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Rename("name", "full_name")
      ))

      val ddl = generateDDL(diff, SQLDialect.H2)
      ddl.size mustBe 1
      // H2 uses "alter column ... rename to"
      ddl.head.toLowerCase must include("rename")
      ddl.head must include("name")
      ddl.head must include("full_name")
    }

    "should generate MODIFY COLUMN for type change (Postgres)" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Modify(
          ColumnSchema("age", Types.INTEGER, false),
          ColumnSchema("age", Types.BIGINT, false)
        )
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.size mustBe 1
      ddl.head.toLowerCase must include("alter")
      ddl.head must include("age")
      ddl.head.toLowerCase must include("bigint")
    }

    "should generate separate statements for type and nullable change (Postgres)" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Modify(
          ColumnSchema("age", Types.INTEGER, true),
          ColumnSchema("age", Types.BIGINT, false)
        )
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.size mustBe 2
      // One for type change, one for nullable change
      ddl.exists(_.toLowerCase.contains("bigint")) mustBe true
      ddl.exists(_.toLowerCase.contains("not null")) mustBe true
    }

    "should generate CREATE TABLE statement" in {
      val schema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false),
        ColumnSchema("email", Types.VARCHAR, true)
      ))

      val ddl = generateCreateTable(schema, SQLDialect.POSTGRES)
      ddl.toLowerCase must include("create table")
      ddl must include("id")
      ddl must include("name")
      ddl must include("email")
    }

    "should generate dialect-specific SQL for MySQL" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Add(ColumnSchema("email", Types.VARCHAR, true))
      ))

      val ddlPostgres = generateDDL(diff, SQLDialect.POSTGRES)
      val ddlMySQL = generateDDL(diff, SQLDialect.MYSQL)

      // Both should generate valid SQL, potentially with dialect differences
      ddlPostgres.size mustBe 1
      ddlMySQL.size mustBe 1
    }
  }

  // ========== SQL Type Helpers Tests ==========

  "SQL type helpers" - {

    "should convert SQL types to names" in {
      sqlTypeToName(Types.INTEGER) mustBe "INTEGER"
      sqlTypeToName(Types.BIGINT) mustBe "BIGINT"
      sqlTypeToName(Types.VARCHAR) mustBe "VARCHAR(255)"
      sqlTypeToName(Types.BOOLEAN) mustBe "BOOLEAN"
      sqlTypeToName(Types.TIMESTAMP) mustBe "TIMESTAMP"
      sqlTypeToName(Types.DATE) mustBe "DATE"
    }

    "should convert Scala types to SQL types" in {
      scalaTypeToSqlType("Int") mustBe Types.INTEGER
      scalaTypeToSqlType("Long") mustBe Types.BIGINT
      scalaTypeToSqlType("String") mustBe Types.VARCHAR
      scalaTypeToSqlType("Boolean") mustBe Types.BOOLEAN
      scalaTypeToSqlType("java.time.LocalDateTime") mustBe Types.TIMESTAMP
      scalaTypeToSqlType("Option[Int]") mustBe Types.INTEGER
    }
  }
}
