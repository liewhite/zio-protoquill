package io.getquill.context.jooq

import io.getquill.*
import org.jooq.SQLDialect
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfterEach
import zio.*

import java.sql.{Connection, DriverManager, Types}
import javax.sql.DataSource
import java.io.File

/**
 * Integration tests for SchemaMigrator.
 */
class SchemaMigratorSpec extends AnyFreeSpec with Matchers with BeforeAndAfterEach {

  // Test entities
  case class User(id: Int, name: String, age: Int)
  case class UserV2(id: Int, name: String, age: Int, email: Option[String])
  case class UserV3(id: Int, fullName: String, age: Int, email: Option[String])

  // Use a temp file for the SQLite database
  val dbFile = new File(java.lang.System.getProperty("java.io.tmpdir"), "quill_migration_test.db")
  val dbUrl = s"jdbc:sqlite:${dbFile.getAbsolutePath}"

  // Simple DataSource wrapper
  class SimpleDataSource(url: String) extends DataSource {
    override def getConnection() = DriverManager.getConnection(url)
    override def getConnection(username: String, password: String) = getConnection()
    override def getLogWriter() = null
    override def setLogWriter(out: java.io.PrintWriter): Unit = ()
    override def setLoginTimeout(seconds: Int): Unit = ()
    override def getLoginTimeout() = 0
    override def getParentLogger() = null
    override def unwrap[T](iface: Class[T]): T = null.asInstanceOf[T]
    override def isWrapperFor(iface: Class[?]) = false
  }

  val dataSource = new SimpleDataSource(dbUrl)
  val migrator = SchemaMigrator(SQLDialect.SQLITE, Literal)

  // Helper to run ZIO effects
  def runZIO[A](effect: ZIO[DataSource, Throwable, A]): A = {
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(
        effect.provideLayer(ZLayer.succeed(dataSource))
      ).getOrThrowFiberFailure()
    }
  }

  // Helper to execute raw SQL
  def executeSQL(sql: String): Unit = {
    val conn = dataSource.getConnection()
    try {
      conn.createStatement().execute(sql)
    } finally {
      conn.close()
    }
  }

  // Helper to check if table exists
  def tableExists(tableName: String): Boolean = {
    val conn = dataSource.getConnection()
    try {
      val rs = conn.getMetaData.getTables(null, null, tableName, Array("TABLE"))
      try { rs.next() } finally { rs.close() }
    } finally {
      conn.close()
    }
  }

  // Helper to get column names
  def getColumnNames(tableName: String): Set[String] = {
    getColumnInfo(tableName).map(_._1).toSet
  }

  // Helper to get full column info: (name, sqlType, nullable)
  case class ColumnInfo(name: String, sqlType: Int, nullable: Boolean)

  def getColumnInfo(tableName: String): List[ColumnInfo] = {
    val conn = dataSource.getConnection()
    try {
      val rs = conn.getMetaData.getColumns(null, null, tableName, null)
      try {
        var cols = List.empty[ColumnInfo]
        while (rs.next()) {
          cols = cols :+ ColumnInfo(
            rs.getString("COLUMN_NAME"),
            rs.getInt("DATA_TYPE"),
            rs.getInt("NULLABLE") == java.sql.DatabaseMetaData.columnNullable
          )
        }
        cols
      } finally {
        rs.close()
      }
    } finally {
      conn.close()
    }
  }

  // Helper to verify we can actually use the table (insert and query)
  def verifyTableUsable(tableName: String, insertSql: String, selectSql: String, expectedCount: Int): Unit = {
    val conn = dataSource.getConnection()
    try {
      val stmt = conn.createStatement()
      stmt.execute(insertSql)
      val rs = stmt.executeQuery(selectSql)
      var count = 0
      while (rs.next()) count += 1
      rs.close()
      count mustBe expectedCount
    } finally {
      conn.close()
    }
  }

  override def beforeEach(): Unit = {
    // Clean up database file before each test
    if (dbFile.exists()) dbFile.delete()
  }

  override def afterEach(): Unit = {
    // Clean up database file after each test
    if (dbFile.exists()) dbFile.delete()
  }

  "SchemaMigrator" - {

    "should create new table when it doesn't exist" in {
      tableExists("User") mustBe false

      val report = runZIO(migrator.migrate[User]())

      // Verify report
      report.action mustBe MigrationAction.Created
      report.tableName mustBe "User"
      report.changes.size mustBe 3

      // Verify table exists with correct structure
      tableExists("User") mustBe true
      val columns = getColumnInfo("User")
      columns.map(_.name).toSet mustBe Set("id", "name", "age")

      // Verify nullable settings
      columns.find(_.name == "id").get.nullable mustBe false
      columns.find(_.name == "name").get.nullable mustBe false
      columns.find(_.name == "age").get.nullable mustBe false

      // Verify table is usable - insert and query data
      verifyTableUsable(
        "User",
        "INSERT INTO User (id, name, age) VALUES (1, 'Alice', 30)",
        "SELECT * FROM User WHERE id = 1",
        expectedCount = 1
      )
    }

    "should report no changes when schema matches" in {
      // Create table manually
      executeSQL("""
        CREATE TABLE User (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL
        )
      """)
      val columnsBefore = getColumnInfo("User")

      val report = runZIO(migrator.migrate[User]())

      // Verify report
      report.action mustBe MigrationAction.NoChanges
      report.statements mustBe empty
      report.changes mustBe empty

      // Verify table structure unchanged
      val columnsAfter = getColumnInfo("User")
      columnsAfter mustBe columnsBefore
    }

    "should add new column" in {
      // Create original table and insert test data
      executeSQL("""
        CREATE TABLE UserV2 (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL
        )
      """)
      executeSQL("INSERT INTO UserV2 (id, name, age) VALUES (1, 'Alice', 30)")

      // Migrate to add email column
      val report = runZIO(migrator.migrate[UserV2]())

      // Verify report
      report.action mustBe MigrationAction.Altered
      report.changes must contain(ColumnChange.Added("email", "VARCHAR(255)"))

      // Verify column added with correct properties
      val columns = getColumnInfo("UserV2")
      columns.map(_.name).toSet mustBe Set("id", "name", "age", "email")
      val emailCol = columns.find(_.name == "email").get
      emailCol.nullable mustBe true  // Option[String] should be nullable

      // Verify existing data preserved and new column works
      verifyTableUsable(
        "UserV2",
        "UPDATE UserV2 SET email = 'alice@example.com' WHERE id = 1",
        "SELECT * FROM UserV2 WHERE email = 'alice@example.com'",
        expectedCount = 1
      )
    }

    "should drop column with empty mapping" in {
      // Create table with extra column and data
      executeSQL("""
        CREATE TABLE User (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL,
          legacy_field VARCHAR(255)
        )
      """)
      executeSQL("INSERT INTO User (id, name, age, legacy_field) VALUES (1, 'Alice', 30, 'old_value')")

      // Migrate - should drop legacy_field
      val report = runZIO(migrator.migrate[User](Some(Map.empty)))

      // Verify report
      report.action mustBe MigrationAction.Altered
      report.changes must contain(ColumnChange.Dropped("legacy_field"))

      // Verify column dropped
      val columns = getColumnInfo("User")
      columns.map(_.name).toSet mustBe Set("id", "name", "age")
      columns.find(_.name == "legacy_field") mustBe None

      // Verify remaining data preserved
      verifyTableUsable(
        "User",
        "SELECT 1",  // No insert needed
        "SELECT * FROM User WHERE name = 'Alice'",
        expectedCount = 1
      )
    }

    "should rename column with mapping" in {
      // Create table with old column name and data
      executeSQL("""
        CREATE TABLE UserV3 (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL,
          email VARCHAR(255)
        )
      """)
      executeSQL("INSERT INTO UserV3 (id, name, age, email) VALUES (1, 'Alice', 30, 'alice@test.com')")

      // Migrate with rename mapping
      val report = runZIO(migrator.migrate[UserV3](
        renameMapping = Some(Map("name" -> "fullName"))
      ))

      // Verify report
      report.action mustBe MigrationAction.Altered
      report.changes must contain(ColumnChange.Renamed("name", "fullName"))

      // Verify column renamed
      val columns = getColumnInfo("UserV3")
      columns.map(_.name).toSet mustBe Set("id", "fullName", "age", "email")
      columns.find(_.name == "name") mustBe None
      columns.find(_.name == "fullName") mustBe defined

      // Verify data preserved under new column name
      verifyTableUsable(
        "UserV3",
        "SELECT 1",  // No insert needed
        "SELECT * FROM UserV3 WHERE fullName = 'Alice'",
        expectedCount = 1
      )
    }

    "should throw error on ambiguous changes without mapping" in {
      // Create table with different column
      executeSQL("""
        CREATE TABLE UserV3 (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL,
          email VARCHAR(255)
        )
      """)
      val columnsBefore = getColumnInfo("UserV3")

      // Migrate without mapping - should fail
      val result = runZIO(migrator.migrate[UserV3]().either)

      // Verify error
      result.isLeft mustBe true
      result.left.get.getMessage must include("Ambiguous")

      // Verify table unchanged after failed migration
      val columnsAfter = getColumnInfo("UserV3")
      columnsAfter mustBe columnsBefore
    }

    "should preview migration without executing" in {
      // Create table
      executeSQL("""
        CREATE TABLE UserV2 (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL
        )
      """)
      val columnsBefore = getColumnInfo("UserV2")

      val preview = runZIO(migrator.preview[UserV2]())

      // Verify preview content
      preview.tableExists mustBe true
      preview.statements.size mustBe 1
      preview.statements.head.toLowerCase must include("add")
      preview.statements.head.toLowerCase must include("email")

      // Verify table structure unchanged (preview should NOT modify)
      val columnsAfter = getColumnInfo("UserV2")
      columnsAfter mustBe columnsBefore
      columnsAfter.map(_.name).toSet mustBe Set("id", "name", "age")
    }

    "should check if migration is needed" in {
      // Table doesn't exist
      val needed1 = runZIO(migrator.needsMigration[User])
      needed1 mustBe true

      // Create matching table
      executeSQL("""
        CREATE TABLE User (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL
        )
      """)

      val needed2 = runZIO(migrator.needsMigration[User])
      needed2 mustBe false

      // Add extra column - should need migration again
      executeSQL("ALTER TABLE User ADD COLUMN extra VARCHAR(255)")
      val needed3 = runZIO(migrator.needsMigration[User])
      needed3 mustBe true
    }

    "should handle multiple migrations in sequence" in {
      // Start with no table
      tableExists("User") mustBe false

      // First migration: create table
      val report1 = runZIO(migrator.migrate[User]())
      report1.action mustBe MigrationAction.Created
      getColumnNames("User") mustBe Set("id", "name", "age")

      // Second migration: no changes
      val report2 = runZIO(migrator.migrate[User]())
      report2.action mustBe MigrationAction.NoChanges

      // Manually add column to simulate schema drift
      executeSQL("ALTER TABLE User ADD COLUMN extra VARCHAR(255)")
      getColumnNames("User") mustBe Set("id", "name", "age", "extra")

      // Third migration: should drop extra column
      val report3 = runZIO(migrator.migrate[User](Some(Map.empty)))
      report3.action mustBe MigrationAction.Altered
      report3.changes must contain(ColumnChange.Dropped("extra"))
      getColumnNames("User") mustBe Set("id", "name", "age")
    }

    "should generate readable summary" in {
      val report = MigrationReport(
        tableName = "User",
        action = MigrationAction.Altered,
        statements = List("ALTER TABLE User ADD COLUMN email VARCHAR(255)"),
        changes = List(
          ColumnChange.Added("email", "VARCHAR(255)"),
          ColumnChange.Renamed("name", "fullName"),
          ColumnChange.Dropped("legacy")
        )
      )

      val summary = report.summary
      summary must include("User")
      summary must include("Altered")
      summary must include("+ email")
      summary must include("~ name -> fullName")
      summary must include("- legacy")
    }
  }

  "SchemaMacro" - {

    "should extract schema from case class" in {
      val schema = SchemaMacro.extractSchema[User](Literal)

      schema.name mustBe "User"
      schema.columns.size mustBe 3
      schema.columnNames mustBe Set("id", "name", "age")

      val idCol = schema.columnMap("id")
      idCol.sqlType mustBe Types.INTEGER
      idCol.nullable mustBe false

      val nameCol = schema.columnMap("name")
      nameCol.sqlType mustBe Types.VARCHAR
      nameCol.nullable mustBe false
    }

    "should apply naming strategy" in {
      val schema = SchemaMacro.extractSchema[User](SnakeCase)

      // Note: User doesn't have camelCase fields, so no change expected
      schema.name mustBe "user"  // SnakeCase lowercases table names
      schema.columnNames mustBe Set("id", "name", "age")
    }

    "should detect Option types as nullable" in {
      val schema = SchemaMacro.extractSchema[UserV2](Literal)

      val emailCol = schema.columnMap("email")
      emailCol.nullable mustBe true
      emailCol.sqlType mustBe Types.VARCHAR
    }

    "should extract schema with explicit table name" in {
      val schema = SchemaMacro.extractSchema[User]("custom_users", Literal)

      schema.name mustBe "custom_users"
      schema.columns.size mustBe 3
    }
  }
}
