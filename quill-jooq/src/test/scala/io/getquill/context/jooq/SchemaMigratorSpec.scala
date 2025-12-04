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
    val conn = dataSource.getConnection()
    try {
      val rs = conn.getMetaData.getColumns(null, null, tableName, null)
      try {
        var cols = Set.empty[String]
        while (rs.next()) {
          cols += rs.getString("COLUMN_NAME")
        }
        cols
      } finally {
        rs.close()
      }
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

      report.action mustBe MigrationAction.Created
      report.tableName mustBe "User"
      report.changes.size mustBe 3
      tableExists("User") mustBe true
      getColumnNames("User") mustBe Set("id", "name", "age")
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

      val report = runZIO(migrator.migrate[User]())

      report.action mustBe MigrationAction.NoChanges
      report.statements mustBe empty
      report.changes mustBe empty
    }

    "should add new column" in {
      // Create original table
      executeSQL("""
        CREATE TABLE UserV2 (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL
        )
      """)

      // Migrate to add email column
      val report = runZIO(migrator.migrate[UserV2]())

      report.action mustBe MigrationAction.Altered
      report.changes must contain(ColumnChange.Added("email", "VARCHAR(255)"))
      getColumnNames("UserV2") mustBe Set("id", "name", "age", "email")
    }

    "should drop column with empty mapping" in {
      // Create table with extra column
      executeSQL("""
        CREATE TABLE User (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL,
          legacy_field VARCHAR(255)
        )
      """)

      // Migrate - should drop legacy_field
      val report = runZIO(migrator.migrate[User](Some(Map.empty)))

      report.action mustBe MigrationAction.Altered
      report.changes must contain(ColumnChange.Dropped("legacy_field"))
      getColumnNames("User") mustBe Set("id", "name", "age")
    }

    "should rename column with mapping" in {
      // Create table with old column name
      executeSQL("""
        CREATE TABLE UserV3 (
          id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          age INTEGER NOT NULL,
          email VARCHAR(255)
        )
      """)

      // Migrate with rename mapping
      val report = runZIO(migrator.migrate[UserV3](
        renameMapping = Some(Map("name" -> "fullName"))
      ))

      report.action mustBe MigrationAction.Altered
      report.changes must contain(ColumnChange.Renamed("name", "fullName"))
      getColumnNames("UserV3") mustBe Set("id", "fullName", "age", "email")
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

      // Migrate without mapping - should fail
      val result = runZIO(migrator.migrate[UserV3]().either)

      result.isLeft mustBe true
      result.left.get.getMessage must include("Ambiguous")
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

      val preview = runZIO(migrator.preview[UserV2]())

      preview.tableExists mustBe true
      preview.statements.size mustBe 1
      // jOOQ generates "add" (lowercase) for SQLite dialect
      preview.statements.head.toLowerCase must include("add")
      preview.statements.head.toLowerCase must include("email")

      // Verify table wasn't changed
      getColumnNames("UserV2") mustBe Set("id", "name", "age")
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
