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

  // ========== DDL Generation Tests ==========

  "DDL generation" - {

    "should generate ADD COLUMN statement" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Add(ColumnSchema("email", Types.VARCHAR, true))
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.size mustBe 1
      ddl.head mustBe "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
    }

    "should generate ADD COLUMN with NOT NULL" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Add(ColumnSchema("email", Types.VARCHAR, false))
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.head mustBe "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL"
    }

    "should generate DROP COLUMN statement" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Drop("legacy_field")
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.size mustBe 1
      ddl.head mustBe "ALTER TABLE users DROP COLUMN legacy_field"
    }

    "should generate RENAME COLUMN statement (Postgres)" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Rename("name", "full_name")
      ))

      val ddl = generateDDL(diff, SQLDialect.POSTGRES)
      ddl.size mustBe 1
      ddl.head mustBe "ALTER TABLE users RENAME COLUMN name TO full_name"
    }

    "should generate RENAME COLUMN statement (H2)" in {
      val diff = SchemaDiff("users", List(
        ColumnDiff.Rename("name", "full_name")
      ))

      val ddl = generateDDL(diff, SQLDialect.H2)
      ddl.size mustBe 1
      ddl.head mustBe "ALTER TABLE users ALTER COLUMN name RENAME TO full_name"
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
      ddl.head mustBe "ALTER TABLE users ALTER COLUMN age TYPE BIGINT"
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
      ddl must contain("ALTER TABLE users ALTER COLUMN age TYPE BIGINT")
      ddl must contain("ALTER TABLE users ALTER COLUMN age SET NOT NULL")
    }

    "should generate CREATE TABLE statement" in {
      val schema = TableSchema("users", List(
        ColumnSchema("id", Types.INTEGER, false),
        ColumnSchema("name", Types.VARCHAR, false),
        ColumnSchema("email", Types.VARCHAR, true)
      ))

      val ddl = generateCreateTable(schema, SQLDialect.POSTGRES)
      ddl must include("CREATE TABLE users")
      ddl must include("id INTEGER NOT NULL")
      ddl must include("name VARCHAR(255) NOT NULL")
      ddl must include("email VARCHAR(255)")
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
