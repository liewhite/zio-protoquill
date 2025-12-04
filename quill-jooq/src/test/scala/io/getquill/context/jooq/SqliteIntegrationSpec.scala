package io.getquill.context.jooq

import io.getquill.*
import io.getquill.ast.Entity
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfterAll
import zio.*

import javax.sql.DataSource
import java.sql.{Connection, DriverManager}
import java.io.File

class SqliteIntegrationSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  case class Person(id: Int, name: String, age: Int)

  // Use a temp file for the SQLite database
  val dbFile = new File(java.lang.System.getProperty("java.io.tmpdir"), "quill_jooq_test.db")
  val dbUrl = s"jdbc:sqlite:${dbFile.getAbsolutePath}"

  // Create a simple DataSource wrapper for SQLite
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

  // Initialize the database
  override def beforeAll(): Unit = {
    // Delete any existing database file
    if (dbFile.exists()) dbFile.delete()

    val conn = dataSource.getConnection()
    try {
      val stmt = conn.createStatement()
      stmt.execute("""
        CREATE TABLE Person (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          age INTEGER NOT NULL
        )
      """)
      // Insert test data
      stmt.execute("INSERT INTO Person (id, name, age) VALUES (1, 'Alice', 30)")
      stmt.execute("INSERT INTO Person (id, name, age) VALUES (2, 'Bob', 25)")
      stmt.execute("INSERT INTO Person (id, name, age) VALUES (3, 'Charlie', 35)")
    } finally {
      conn.close()
    }
  }

  override def afterAll(): Unit = {
    // Clean up the database file
    if (dbFile.exists()) dbFile.delete()
  }

  def runZIO[A](effect: ZIO[DataSource, Throwable, A]): A = {
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(
        effect.provideLayer(ZLayer.succeed(dataSource))
      ).getOrThrowFiberFailure()
    }
  }

  "SQLite Integration" - {

    "should execute raw jOOQ query" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val result = dslCtx.selectFrom(DSL.table("Person")).fetch()
        result.size() mustBe 3
      } finally {
        conn.close()
      }
    }

    "should translate and execute simple select" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val translationCtx = JooqAstTranslator.TranslationContext(Literal)

        // Manually create Entity AST
        val personQuat = io.getquill.quat.Quat.Product("Person",
          "id" -> io.getquill.quat.Quat.Value,
          "name" -> io.getquill.quat.Quat.Value,
          "age" -> io.getquill.quat.Quat.Value
        )
        val entity = io.getquill.ast.Entity("Person", Nil, personQuat)

        val query = JooqAstTranslator.translateQuery(entity, translationCtx, dslCtx)
        val result = query.fetch()

        result.size() mustBe 3
      } finally {
        conn.close()
      }
    }

    "should translate and execute filtered query" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val translationCtx = JooqAstTranslator.TranslationContext(Literal)

        // Create Filter AST: query[Person].filter(p => p.age > 25)
        val personQuat = io.getquill.quat.Quat.Product("Person",
          "id" -> io.getquill.quat.Quat.Value,
          "name" -> io.getquill.quat.Quat.Value,
          "age" -> io.getquill.quat.Quat.Value
        )
        val entity = io.getquill.ast.Entity("Person", Nil, personQuat)
        val alias = io.getquill.ast.Ident("p", personQuat)
        val ageProperty = io.getquill.ast.Property(alias, "age")
        val constant25 = io.getquill.ast.Constant(25, io.getquill.quat.Quat.Value)
        val predicate = io.getquill.ast.BinaryOperation(
          ageProperty,
          io.getquill.ast.NumericOperator.`>`,
          constant25
        )
        val filter = io.getquill.ast.Filter(entity, alias, predicate)

        val query = JooqAstTranslator.translateQuery(filter, translationCtx, dslCtx)
        val result = query.fetch()

        // Should return Alice (30) and Charlie (35)
        result.size() mustBe 2
      } finally {
        conn.close()
      }
    }

    "should translate and execute equality filter" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val translationCtx = JooqAstTranslator.TranslationContext(Literal)

        // Create Filter AST: query[Person].filter(p => p.name == "Alice")
        val personQuat = io.getquill.quat.Quat.Product("Person",
          "id" -> io.getquill.quat.Quat.Value,
          "name" -> io.getquill.quat.Quat.Value,
          "age" -> io.getquill.quat.Quat.Value
        )
        val entity = io.getquill.ast.Entity("Person", Nil, personQuat)
        val alias = io.getquill.ast.Ident("p", personQuat)
        val nameProperty = io.getquill.ast.Property(alias, "name")
        val constantAlice = io.getquill.ast.Constant("Alice", io.getquill.quat.Quat.Value)
        val predicate = io.getquill.ast.BinaryOperation(
          nameProperty,
          io.getquill.ast.EqualityOperator.`_==`,
          constantAlice
        )
        val filter = io.getquill.ast.Filter(entity, alias, predicate)

        val query = JooqAstTranslator.translateQuery(filter, translationCtx, dslCtx)
        val result = query.fetch()

        result.size() mustBe 1
        result.get(0).get("name") mustBe "Alice"
      } finally {
        conn.close()
      }
    }

    "should translate and execute sorted query" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val translationCtx = JooqAstTranslator.TranslationContext(Literal)

        // Create SortBy AST: query[Person].sortBy(p => p.age)(Ord.desc)
        val personQuat = io.getquill.quat.Quat.Product("Person",
          "id" -> io.getquill.quat.Quat.Value,
          "name" -> io.getquill.quat.Quat.Value,
          "age" -> io.getquill.quat.Quat.Value
        )
        val entity = io.getquill.ast.Entity("Person", Nil, personQuat)
        val alias = io.getquill.ast.Ident("p", personQuat)
        val ageProperty = io.getquill.ast.Property(alias, "age")
        val sortBy = io.getquill.ast.SortBy(entity, alias, ageProperty, io.getquill.ast.Desc)

        val query = JooqAstTranslator.translateQuery(sortBy, translationCtx, dslCtx)
        val result = query.fetch()

        result.size() mustBe 3
        // Desc order: Charlie (35), Alice (30), Bob (25)
        result.get(0).get("name") mustBe "Charlie"
        result.get(1).get("name") mustBe "Alice"
        result.get(2).get("name") mustBe "Bob"
      } finally {
        conn.close()
      }
    }

    "should translate and execute query with limit" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val translationCtx = JooqAstTranslator.TranslationContext(Literal)

        // Create Take AST: query[Person].take(2)
        val personQuat = io.getquill.quat.Quat.Product("Person",
          "id" -> io.getquill.quat.Quat.Value,
          "name" -> io.getquill.quat.Quat.Value,
          "age" -> io.getquill.quat.Quat.Value
        )
        val entity = io.getquill.ast.Entity("Person", Nil, personQuat)
        val take = io.getquill.ast.Take(entity, io.getquill.ast.Constant(2, io.getquill.quat.Quat.Value))

        val query = JooqAstTranslator.translateQuery(take, translationCtx, dslCtx)
        val result = query.fetch()

        result.size() mustBe 2
      } finally {
        conn.close()
      }
    }

    "should translate and execute mapped query" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val translationCtx = JooqAstTranslator.TranslationContext(Literal)

        // Create Map AST: query[Person].map(p => p.name)
        val personQuat = io.getquill.quat.Quat.Product("Person",
          "id" -> io.getquill.quat.Quat.Value,
          "name" -> io.getquill.quat.Quat.Value,
          "age" -> io.getquill.quat.Quat.Value
        )
        val entity = io.getquill.ast.Entity("Person", Nil, personQuat)
        val alias = io.getquill.ast.Ident("p", personQuat)
        val nameProperty = io.getquill.ast.Property(alias, "name")
        val mapQuery = io.getquill.ast.Map(entity, alias, nameProperty)

        val query = JooqAstTranslator.translateQuery(mapQuery, translationCtx, dslCtx)
        val result = query.fetch()

        result.size() mustBe 3
        // Verify that only name column is selected
        result.get(0).size() mustBe 1
      } finally {
        conn.close()
      }
    }

    "should translate and execute INSERT" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val translationCtx = JooqAstTranslator.TranslationContext(Literal)

        // Create Insert AST
        val personQuat = io.getquill.quat.Quat.Product("Person",
          "id" -> io.getquill.quat.Quat.Value,
          "name" -> io.getquill.quat.Quat.Value,
          "age" -> io.getquill.quat.Quat.Value
        )
        val entity = io.getquill.ast.Entity("Person", Nil, personQuat)
        val alias = io.getquill.ast.Ident("p", personQuat)

        val assignments = List(
          io.getquill.ast.Assignment(alias, io.getquill.ast.Property(alias, "id"), io.getquill.ast.Constant(4, io.getquill.quat.Quat.Value)),
          io.getquill.ast.Assignment(alias, io.getquill.ast.Property(alias, "name"), io.getquill.ast.Constant("David", io.getquill.quat.Quat.Value)),
          io.getquill.ast.Assignment(alias, io.getquill.ast.Property(alias, "age"), io.getquill.ast.Constant(40, io.getquill.quat.Quat.Value))
        )
        val insert = io.getquill.ast.Insert(entity, assignments)

        val query = JooqAstTranslator.translateInsert(insert, translationCtx, dslCtx)
        val rowsAffected = query.execute()

        rowsAffected mustBe 1

        // Verify insertion
        val verifyResult = dslCtx.selectFrom(DSL.table("Person")).where(DSL.field("id").equal(4)).fetch()
        verifyResult.size() mustBe 1
        verifyResult.get(0).get("name") mustBe "David"
      } finally {
        conn.close()
      }
    }

    "should translate and execute UPDATE" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val translationCtx = JooqAstTranslator.TranslationContext(Literal)

        // First verify Bob's current age
        val beforeResult = dslCtx.selectFrom(DSL.table("Person")).where(DSL.field("name").equal("Bob")).fetch()
        beforeResult.get(0).get("age").asInstanceOf[Int] mustBe 25

        // Create Update AST: query[Person].filter(p => p.name == "Bob").update(_.age -> 26)
        val personQuat = io.getquill.quat.Quat.Product("Person",
          "id" -> io.getquill.quat.Quat.Value,
          "name" -> io.getquill.quat.Quat.Value,
          "age" -> io.getquill.quat.Quat.Value
        )
        val entity = io.getquill.ast.Entity("Person", Nil, personQuat)
        val alias = io.getquill.ast.Ident("p", personQuat)
        val nameProperty = io.getquill.ast.Property(alias, "name")
        val predicate = io.getquill.ast.BinaryOperation(
          nameProperty,
          io.getquill.ast.EqualityOperator.`_==`,
          io.getquill.ast.Constant("Bob", io.getquill.quat.Quat.Value)
        )
        val filter = io.getquill.ast.Filter(entity, alias, predicate)

        val assignments = List(
          io.getquill.ast.Assignment(alias, io.getquill.ast.Property(alias, "age"), io.getquill.ast.Constant(26, io.getquill.quat.Quat.Value))
        )
        val update = io.getquill.ast.Update(filter, assignments)

        val query = JooqAstTranslator.translateUpdate(update, translationCtx, dslCtx)
        val rowsAffected = query.execute()

        rowsAffected mustBe 1

        // Verify update
        val afterResult = dslCtx.selectFrom(DSL.table("Person")).where(DSL.field("name").equal("Bob")).fetch()
        afterResult.get(0).get("age").asInstanceOf[Int] mustBe 26
      } finally {
        conn.close()
      }
    }

    "should translate and execute DELETE" in {
      val conn = dataSource.getConnection()
      try {
        val dslCtx = DSL.using(conn, SQLDialect.SQLITE)
        val translationCtx = JooqAstTranslator.TranslationContext(Literal)

        // First insert a record to delete
        dslCtx.insertInto(DSL.table("Person"))
          .set(DSL.field("id"), 100)
          .set(DSL.field("name"), "ToDelete")
          .set(DSL.field("age"), 99)
          .execute()

        // Verify insertion
        val beforeResult = dslCtx.selectFrom(DSL.table("Person")).where(DSL.field("id").equal(100)).fetch()
        beforeResult.size() mustBe 1

        // Create Delete AST: query[Person].filter(p => p.id == 100).delete
        val personQuat = io.getquill.quat.Quat.Product("Person",
          "id" -> io.getquill.quat.Quat.Value,
          "name" -> io.getquill.quat.Quat.Value,
          "age" -> io.getquill.quat.Quat.Value
        )
        val entity = io.getquill.ast.Entity("Person", Nil, personQuat)
        val alias = io.getquill.ast.Ident("p", personQuat)
        val idProperty = io.getquill.ast.Property(alias, "id")
        val predicate = io.getquill.ast.BinaryOperation(
          idProperty,
          io.getquill.ast.EqualityOperator.`_==`,
          io.getquill.ast.Constant(100, io.getquill.quat.Quat.Value)
        )
        val filter = io.getquill.ast.Filter(entity, alias, predicate)
        val delete = io.getquill.ast.Delete(filter)

        val query = JooqAstTranslator.translateDelete(delete, translationCtx, dslCtx)
        val rowsAffected = query.execute()

        rowsAffected mustBe 1

        // Verify deletion
        val afterResult = dslCtx.selectFrom(DSL.table("Person")).where(DSL.field("id").equal(100)).fetch()
        afterResult.size() mustBe 0
      } finally {
        conn.close()
      }
    }

  }

}
