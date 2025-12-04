package io.getquill.context.jooq

import io.getquill.*
import org.jooq.SQLDialect
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfterAll
import zio.*

import javax.sql.DataSource
import java.sql.{Connection, DriverManager}
import java.io.File

/**
 * End-to-end integration tests for ZioJooqContext.
 * Tests the full flow from Quill DSL -> jOOQ -> Database
 */
class JooqIntegrationSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  case class Person(id: Int, name: String, age: Int)

  // Use a temp file for the SQLite database
  val dbFile = new File(java.lang.System.getProperty("java.io.tmpdir"), "quill_jooq_integration_test.db")
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

  // Create the jOOQ context
  val ctx = new ZioJooqContext(SQLDialect.SQLITE, Literal)
  import ctx.*

  // Helper to run ZIO effects
  def runZIO[A](effect: ZIO[DataSource, Throwable, A]): A = {
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(
        effect.provideLayer(ZLayer.succeed(dataSource))
      ).getOrThrowFiberFailure()
    }
  }

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
      // Insert initial test data
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

  "ZioJooqContext Integration" - {

    "SELECT - query all" in {
      inline def q = query[Person]
      val result = runZIO(ctx.run(q))
      result.size mustBe 3
      result.map(_.name) must contain allOf ("Alice", "Bob", "Charlie")
    }

    "SELECT - filter by id" in {
      val q = quote {
        query[Person].filter(p => p.id == 1)
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 1
      result.head.name mustBe "Alice"
    }

    "SELECT - filter with lift" in {
      val targetId = 2
      inline def q = query[Person].filter(p => p.id == lift(targetId))
      
      val result = runZIO(ctx.run(q))
      result.size mustBe 1
      result.head.name mustBe "Bob"
    }

    "SELECT - filter by age comparison" in {
      val q = quote {
        query[Person].filter(p => p.age > 25)
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 2
      result.map(_.name) must contain allOf ("Alice", "Charlie")
    }

    "SELECT - filter with lifted comparison" in {
      val minAge = 30
      val q = quote {
        query[Person].filter(p => p.age >= lift(minAge))
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 2
      result.map(_.name) must contain allOf ("Alice", "Charlie")
    }

    "SELECT - map to single column" in {
      val q = quote {
        query[Person].map(p => p.name)
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 3
      result must contain allOf ("Alice", "Bob", "Charlie")
    }

    "SELECT - sortBy ascending" in {
      val q = quote {
        query[Person].sortBy(p => p.age)
      }
      val result = runZIO(ctx.run(q))
      result.map(_.name) mustBe List("Bob", "Alice", "Charlie")
    }

    "SELECT - sortBy descending" in {
      val q = quote {
        query[Person].sortBy(p => p.age)(Ord.desc)
      }
      val result = runZIO(ctx.run(q))
      result.map(_.name) mustBe List("Charlie", "Alice", "Bob")
    }

    "SELECT - take limit" in {
      val q = quote {
        query[Person].sortBy(p => p.id).take(2)
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 2
    }

    "SELECT - combined filter and sort" in {
      val q = quote {
        query[Person].filter(p => p.age > 20).sortBy(p => p.name)
      }
      val result = runZIO(ctx.run(q))
      result.map(_.name) mustBe List("Alice", "Bob", "Charlie")
    }

    "INSERT - single row" in {
      val q = quote {
        query[Person].insert(
          _.id -> (100),
          _.name -> lift("David"),
          _.age -> lift(40)
        )
      }
      val rowsAffected = runZIO(ctx.run(q))
      rowsAffected mustBe 1

      // Verify insertion
      val verifyQ = quote {
        query[Person].filter(p => p.id == 100)
      }
      val result = runZIO(ctx.run(verifyQ))
      result.size mustBe 1
      result.head.name mustBe "David"
      result.head.age mustBe 40
    }

    "UPDATE - single row" in {
      // First insert a record to update via raw SQL
      val conn = dataSource.getConnection()
      try {
        conn.createStatement().execute("INSERT INTO Person (id, name, age) VALUES (200, 'Eve', 28)")
      } finally {
        conn.close()
      }

      // Update using Quill DSL
      val q = quote {
        query[Person].filter(p => p.id == 200).update(_.age -> lift(29))
      }
      val rowsAffected = runZIO(ctx.run(q))
      rowsAffected mustBe 1

      // Verify update
      val verifyQ = quote {
        query[Person].filter(p => p.id == 200)
      }
      val result = runZIO(ctx.run(verifyQ))
      result.head.age mustBe 29
    }

    "DELETE - single row" in {
      // First insert a record to delete via raw SQL
      val conn = dataSource.getConnection()
      try {
        conn.createStatement().execute("INSERT INTO Person (id, name, age) VALUES (300, 'ToDelete', 99)")
      } finally {
        conn.close()
      }

      // Delete using Quill DSL
      val q = quote {
        query[Person].filter(p => p.id == 300).delete
      }
      val rowsAffected = runZIO(ctx.run(q))
      rowsAffected mustBe 1

      // Verify deletion
      val verifyQ = quote {
        query[Person].filter(p => p.id == 300)
      }
      val result = runZIO(ctx.run(verifyQ))
      result.size mustBe 0
    }

    "Complex query - filter with multiple conditions" in {
      val q = quote {
        query[Person].filter(p => p.age > 25 && p.name != "Charlie")
      }
      val result = runZIO(ctx.run(q))
      // Verify AND conditions work - Alice (30) should be included, Bob (25) and Charlie excluded
      result.map(_.name) must contain ("Alice")
      result.map(_.name) must not contain ("Bob")      // age <= 25
      result.map(_.name) must not contain ("Charlie")  // excluded by name
      // All results must satisfy both conditions
      result.foreach { p =>
        p.age must be > 25
        p.name must not be "Charlie"
      }
    }

    "Complex query - filter with OR" in {
      val q = quote {
        query[Person].filter(p => p.name == "Alice" || p.name == "Bob")
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 2
      result.map(_.name) must contain allOf ("Alice", "Bob")
    }

    // ========== Aggregation Tests ==========

    "Aggregation - count (size)" in {
      val q = quote {
        query[Person].map(p => count(p.id))
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 1
      // At least 3 (Alice, Bob, Charlie)
      result.head must be >= 3
    }

    "Aggregation - max" in {
      val q = quote {
        query[Person].filter(p => p.id <= 3).map(p => max(p.age))
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 1
      // Charlie is 35 (max of original 3)
      result.head mustBe 35
    }

    "Aggregation - min" in {
      val q = quote {
        query[Person].filter(p => p.id <= 3).map(p => min(p.age))
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 1
      // Bob is 25 (min of original 3)
      result.head mustBe 25
    }

    "Aggregation - sum" in {
      // Sum of original 3: 30 + 25 + 35 = 90
      val q = quote {
        query[Person].filter(p => p.id <= 3).map(p => sum(p.age))
      }
      val result = runZIO(ctx.run(q))
      result.size mustBe 1
      result.head mustBe 90
    }

    // Note: avg returns BigDecimal which needs additional decoder support
    // GroupByMap returns tuples which need additional decoder support
    // These will be added in future iterations

    // ========== RETURNING Tests ==========

    "INSERT with RETURNING - single column" in {
      val q = quote {
        query[Person].insert(
          _.id -> lift(400),
          _.name -> lift("Frank"),
          _.age -> lift(45)
        ).returning(r => r.id)
      }
      val returnedId = runZIO(ctx.run(q))
      returnedId mustBe 400

      // Verify insertion
      val verifyQ = quote {
        query[Person].filter(p => p.id == 400)
      }
      val result = runZIO(ctx.run(verifyQ))
      result.size mustBe 1
      result.head.name mustBe "Frank"
    }

    "INSERT with RETURNING - different column" in {
      val q = quote {
        query[Person].insert(
          _.id -> lift(401),
          _.name -> lift("Grace"),
          _.age -> lift(33)
        ).returning(r => r.name)
      }
      val returnedName = runZIO(ctx.run(q))
      returnedName mustBe "Grace"
    }

    "UPDATE with RETURNING - single column" in {
      // First insert a record to update
      val conn = dataSource.getConnection()
      try {
        conn.createStatement().execute("INSERT INTO Person (id, name, age) VALUES (500, 'Henry', 50)")
      } finally {
        conn.close()
      }

      val q = quote {
        query[Person].filter(p => p.id == 500).update(_.age -> lift(51)).returning(r => r.age)
      }
      val returnedAge = runZIO(ctx.run(q))
      returnedAge mustBe 51
    }

  }

}
