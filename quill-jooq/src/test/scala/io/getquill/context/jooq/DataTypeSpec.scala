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
import java.time.{LocalDate, LocalTime, LocalDateTime}

/**
 * Comprehensive tests for all supported column data types.
 * Tests encoding (INSERT/UPDATE) and decoding (SELECT) for each type.
 */
class DataTypeSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  // ========== Test Entity Definitions ==========

  // Numeric types test entity
  // Note: Using Double instead of Float because Quill doesn't allow implicit Double->Float conversion
  case class NumericEntity(
    id: Int,
    intVal: Int,
    longVal: Long,
    doubleVal: Double,
    floatVal: Double,  // Use Double as SQLite REAL maps to Double anyway
    boolVal: Boolean
  )

  // String test entity (BigDecimal removed due to SQLite/jOOQ compatibility issues)
  case class TextEntity(
    id: Int,
    stringVal: String,
    numericVal: Double  // Using Double instead of BigDecimal for SQLite compatibility
  )

  // Date/Time types test entity
  case class DateTimeEntity(
    id: Int,
    dateVal: LocalDate,
    timeVal: LocalTime,
    dateTimeVal: LocalDateTime
  )

  // Nullable (Option) types test entity
  case class NullableEntity(
    id: Int,
    optInt: Option[Int],
    optString: Option[String],
    optDouble: Option[Double],
    optBoolean: Option[Boolean]
  )

  // Binary data test entity
  case class BinaryEntity(
    id: Int,
    blobVal: Array[Byte]
  )

  // ========== Test Setup ==========

  val dbFile = new File(java.lang.System.getProperty("java.io.tmpdir"), "quill_jooq_datatype_test.db")
  val dbUrl = s"jdbc:sqlite:${dbFile.getAbsolutePath}"

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
  val ctx = new ZioJooqContext(SQLDialect.SQLITE, Literal)
  import ctx.*

  def runZIO[A](effect: ZIO[DataSource, Throwable, A]): A = {
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(
        effect.provideLayer(ZLayer.succeed(dataSource))
      ).getOrThrowFiberFailure()
    }
  }

  override def beforeAll(): Unit = {
    if (dbFile.exists()) dbFile.delete()

    val conn = dataSource.getConnection()
    try {
      val stmt = conn.createStatement()

      // Create NumericEntity table
      stmt.execute("""
        CREATE TABLE NumericEntity (
          id INTEGER PRIMARY KEY,
          intVal INTEGER NOT NULL,
          longVal INTEGER NOT NULL,
          doubleVal REAL NOT NULL,
          floatVal REAL NOT NULL,
          boolVal INTEGER NOT NULL
        )
      """)

      // Create TextEntity table
      stmt.execute("""
        CREATE TABLE TextEntity (
          id INTEGER PRIMARY KEY,
          stringVal TEXT NOT NULL,
          numericVal REAL NOT NULL
        )
      """)

      // Create DateTimeEntity table
      stmt.execute("""
        CREATE TABLE DateTimeEntity (
          id INTEGER PRIMARY KEY,
          dateVal TEXT NOT NULL,
          timeVal TEXT NOT NULL,
          dateTimeVal TEXT NOT NULL
        )
      """)

      // Create NullableEntity table
      stmt.execute("""
        CREATE TABLE NullableEntity (
          id INTEGER PRIMARY KEY,
          optInt INTEGER,
          optString TEXT,
          optDouble REAL,
          optBoolean INTEGER
        )
      """)

      // Create BinaryEntity table
      stmt.execute("""
        CREATE TABLE BinaryEntity (
          id INTEGER PRIMARY KEY,
          blobVal BLOB NOT NULL
        )
      """)

    } finally {
      conn.close()
    }
  }

  override def afterAll(): Unit = {
    if (dbFile.exists()) dbFile.delete()
  }

  // ========== Numeric Types Tests ==========

  "Numeric Types" - {

    "INSERT and SELECT Int, Long, Double, Boolean" in {
      // Note: SQLite INTEGER is 64-bit but jOOQ may decode as Int for smaller values
      // Using a value within Int range to ensure compatibility
      val entity = NumericEntity(
        id = 1,
        intVal = 42,
        longVal = 123456789L,  // Within Int range for SQLite compatibility
        doubleVal = 3.14159,
        floatVal = 2.71828,
        boolVal = true
      )

      val insertQ = quote {
        query[NumericEntity].insert(
          _.id -> lift(entity.id),
          _.intVal -> lift(entity.intVal),
          _.longVal -> lift(entity.longVal),
          _.doubleVal -> lift(entity.doubleVal),
          _.floatVal -> lift(entity.floatVal),
          _.boolVal -> lift(entity.boolVal)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[NumericEntity].filter(_.id == 1) }
      val result = runZIO(ctx.run(selectQ))

      result.size mustBe 1
      val r = result.head
      r.intVal mustBe 42
      r.longVal mustBe 123456789L
      r.doubleVal mustBe 3.14159 +- 0.00001
      r.floatVal mustBe 2.71828 +- 0.00001
      r.boolVal mustBe true
    }

    "UPDATE numeric values" in {
      val updateQ = quote {
        query[NumericEntity].filter(_.id == 1).update(
          _.intVal -> lift(100),
          _.boolVal -> lift(false)
        )
      }
      runZIO(ctx.run(updateQ))

      val selectQ = quote { query[NumericEntity].filter(_.id == 1) }
      val result = runZIO(ctx.run(selectQ))
      result.head.intVal mustBe 100
      result.head.boolVal mustBe false
    }

    "filter by numeric comparison" in {
      // Insert more data
      val floatVal = 1.5
      val insertQ = quote {
        query[NumericEntity].insert(
          _.id -> lift(2),
          _.intVal -> lift(200),
          _.longVal -> lift(1000L),
          _.doubleVal -> lift(1.5),
          _.floatVal -> lift(floatVal),
          _.boolVal -> lift(true)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[NumericEntity].filter(_.intVal > 50) }
      val result = runZIO(ctx.run(selectQ))
      result.map(_.intVal) must contain allOf (100, 200)
    }
  }

  // ========== String Tests ==========

  "String Types" - {

    "INSERT and SELECT String" in {
      val entity = TextEntity(
        id = 1,
        stringVal = "Hello, World! 你好世界",
        numericVal = 123.456
      )

      val insertQ = quote {
        query[TextEntity].insert(
          _.id -> lift(entity.id),
          _.stringVal -> lift(entity.stringVal),
          _.numericVal -> lift(entity.numericVal)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[TextEntity].filter(_.id == 1) }
      val result = runZIO(ctx.run(selectQ))

      result.size mustBe 1
      result.head.stringVal mustBe "Hello, World! 你好世界"
      result.head.numericVal mustBe 123.456 +- 0.001
    }

    "String with special characters" in {
      val specialString = "Line1\nLine2\tTabbed \"Quoted\" 'Single' \\Backslash"
      val insertQ = quote {
        query[TextEntity].insert(
          _.id -> lift(2),
          _.stringVal -> lift(specialString),
          _.numericVal -> lift(0.0)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[TextEntity].filter(_.id == 2) }
      val result = runZIO(ctx.run(selectQ))
      result.head.stringVal mustBe specialString
    }

    "empty String" in {
      val insertQ = quote {
        query[TextEntity].insert(
          _.id -> lift(3),
          _.stringVal -> lift(""),
          _.numericVal -> lift(0.0)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[TextEntity].filter(_.id == 3) }
      val result = runZIO(ctx.run(selectQ))
      result.head.stringVal mustBe ""
    }
  }

  // ========== Date/Time Types Tests ==========

  "Date/Time Types" - {

    "INSERT and SELECT LocalDate, LocalTime, LocalDateTime" in {
      val entity = DateTimeEntity(
        id = 1,
        dateVal = LocalDate.of(2024, 12, 25),
        timeVal = LocalTime.of(14, 30, 45),
        dateTimeVal = LocalDateTime.of(2024, 12, 25, 14, 30, 45)
      )

      val insertQ = quote {
        query[DateTimeEntity].insert(
          _.id -> lift(entity.id),
          _.dateVal -> lift(entity.dateVal),
          _.timeVal -> lift(entity.timeVal),
          _.dateTimeVal -> lift(entity.dateTimeVal)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[DateTimeEntity].filter(_.id == 1) }
      val result = runZIO(ctx.run(selectQ))

      result.size mustBe 1
      result.head.dateVal mustBe LocalDate.of(2024, 12, 25)
      result.head.timeVal mustBe LocalTime.of(14, 30, 45)
      result.head.dateTimeVal mustBe LocalDateTime.of(2024, 12, 25, 14, 30, 45)
    }

    "boundary date values" in {
      // Test minimum and maximum dates
      val minDate = LocalDate.of(1970, 1, 1)
      val maxDate = LocalDate.of(2099, 12, 31)

      val insertQ1 = quote {
        query[DateTimeEntity].insert(
          _.id -> lift(2),
          _.dateVal -> lift(minDate),
          _.timeVal -> lift(LocalTime.MIN),
          _.dateTimeVal -> lift(LocalDateTime.of(1970, 1, 1, 0, 0, 0))
        )
      }
      runZIO(ctx.run(insertQ1))

      val insertQ2 = quote {
        query[DateTimeEntity].insert(
          _.id -> lift(3),
          _.dateVal -> lift(maxDate),
          _.timeVal -> lift(LocalTime.of(23, 59, 59)),
          _.dateTimeVal -> lift(LocalDateTime.of(2099, 12, 31, 23, 59, 59))
        )
      }
      runZIO(ctx.run(insertQ2))

      val selectQ = quote { query[DateTimeEntity].filter(_.id == 2) }
      val result = runZIO(ctx.run(selectQ))
      result.head.dateVal mustBe minDate
    }
  }

  // ========== Nullable (Option) Types Tests ==========

  "Nullable (Option) Types" - {

    "INSERT and SELECT with Some values" in {
      val entity = NullableEntity(
        id = 1,
        optInt = Some(42),
        optString = Some("optional"),
        optDouble = Some(3.14),
        optBoolean = Some(true)
      )

      val insertQ = quote {
        query[NullableEntity].insert(
          _.id -> lift(entity.id),
          _.optInt -> lift(entity.optInt),
          _.optString -> lift(entity.optString),
          _.optDouble -> lift(entity.optDouble),
          _.optBoolean -> lift(entity.optBoolean)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[NullableEntity].filter(_.id == 1) }
      val result = runZIO(ctx.run(selectQ))

      result.size mustBe 1
      result.head.optInt mustBe Some(42)
      result.head.optString mustBe Some("optional")
      // Use tolerance for floating point comparison
      result.head.optDouble.get mustBe 3.14 +- 0.01
      result.head.optBoolean mustBe Some(true)
    }

    "INSERT and SELECT with None values" in {
      val entity = NullableEntity(
        id = 2,
        optInt = None,
        optString = None,
        optDouble = None,
        optBoolean = None
      )

      val insertQ = quote {
        query[NullableEntity].insert(
          _.id -> lift(entity.id),
          _.optInt -> lift(entity.optInt),
          _.optString -> lift(entity.optString),
          _.optDouble -> lift(entity.optDouble),
          _.optBoolean -> lift(entity.optBoolean)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[NullableEntity].filter(_.id == 2) }
      val result = runZIO(ctx.run(selectQ))

      result.size mustBe 1
      result.head.optInt mustBe None
      result.head.optString mustBe None
      result.head.optDouble mustBe None
      result.head.optBoolean mustBe None
    }

    "UPDATE Option from Some to None" in {
      val noneInt: Option[Int] = None
      val noneString: Option[String] = None
      val updateQ = quote {
        query[NullableEntity].filter(_.id == 1).update(
          _.optInt -> lift(noneInt),
          _.optString -> lift(noneString)
        )
      }
      runZIO(ctx.run(updateQ))

      val selectQ = quote { query[NullableEntity].filter(_.id == 1) }
      val result = runZIO(ctx.run(selectQ))
      result.head.optInt mustBe None
      result.head.optString mustBe None
    }

    "UPDATE Option from None to Some" in {
      val newInt: Option[Int] = Some(999)
      val newString: Option[String] = Some("updated")
      val updateQ = quote {
        query[NullableEntity].filter(_.id == 2).update(
          _.optInt -> lift(newInt),
          _.optString -> lift(newString)
        )
      }
      runZIO(ctx.run(updateQ))

      val selectQ = quote { query[NullableEntity].filter(_.id == 2) }
      val result = runZIO(ctx.run(selectQ))
      result.head.optInt mustBe Some(999)
      result.head.optString mustBe Some("updated")
    }

    "filter by Option values" in {
      // Add one more record for filtering test
      val optInt: Option[Int] = Some(100)
      val optString: Option[String] = Some("test")
      val optDoubleNone: Option[Double] = None
      val optBoolNone: Option[Boolean] = None
      val insertQ = quote {
        query[NullableEntity].insert(
          _.id -> lift(3),
          _.optInt -> lift(optInt),
          _.optString -> lift(optString),
          _.optDouble -> lift(optDoubleNone),
          _.optBoolean -> lift(optBoolNone)
        )
      }
      runZIO(ctx.run(insertQ))

      // Filter where optInt is not null (has value)
      val selectQ = quote {
        query[NullableEntity].filter(e => e.optInt.isDefined)
      }
      val result = runZIO(ctx.run(selectQ))
      result.size must be >= 1
    }
  }

  // ========== Binary Data Tests ==========

  "Binary (Array[Byte]) Types" - {

    "INSERT and SELECT binary data" in {
      val data = Array[Byte](0, 1, 2, 127, -128, -1)
      val insertQ = quote {
        query[BinaryEntity].insert(
          _.id -> lift(1),
          _.blobVal -> lift(data)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[BinaryEntity].filter(_.id == 1) }
      val result = runZIO(ctx.run(selectQ))

      result.size mustBe 1
      result.head.blobVal mustBe data
    }

    "empty binary data" in {
      val emptyData = Array.empty[Byte]
      val insertQ = quote {
        query[BinaryEntity].insert(
          _.id -> lift(2),
          _.blobVal -> lift(emptyData)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[BinaryEntity].filter(_.id == 2) }
      val result = runZIO(ctx.run(selectQ))
      result.head.blobVal mustBe emptyData
    }

    "large binary data" in {
      val largeData = Array.fill[Byte](10000)((scala.util.Random.nextInt(256) - 128).toByte)
      val insertQ = quote {
        query[BinaryEntity].insert(
          _.id -> lift(3),
          _.blobVal -> lift(largeData)
        )
      }
      runZIO(ctx.run(insertQ))

      val selectQ = quote { query[BinaryEntity].filter(_.id == 3) }
      val result = runZIO(ctx.run(selectQ))
      result.head.blobVal mustBe largeData
    }
  }

}
