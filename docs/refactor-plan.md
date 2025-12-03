# zio-protoquill 重构计划：jOOQ Context 实现

## 背景

zio-protoquill 是一个 Scala 3 编译时 SQL DSL 库。当前架构：
- 使用 `Quoted[T]` DSL 定义查询
- 通过 `Idiom` 将 AST 翻译成 SQL 字符串
- 在不同的 `Context` 中执行（JdbcContext, ZioJdbcContext, CassandraContext）

### 重构目标

1. **第一步**：添加 jOOQ Context，将 Quoted DSL 直接翻译成 jOOQ DSL 调用
2. **第二步**：删除 jooq 以外的其他 ctx

### 重构优势

- jOOQ 是成熟的 SQL 构建库，支持多种数据库方言
- jOOQ 提供类型安全的 SQL 构建
- 减少自研代码维护成本
- jOOQ 的执行层经过充分测试

---

## 项目当前架构分析

### 核心模块

```
quill-sql                    # 核心 DSL 和 AST
quill-jdbc                   # 同步 JDBC Context
quill-jdbc-zio               # ZIO 异步 JDBC Context
quill-zio                    # ZIO 通用模块
quill-cassandra              # Cassandra Context
quill-cassandra-zio          # Cassandra ZIO Context
quill-doobie                 # Doobie 集成
quill-caliban                # Caliban GraphQL 集成
```

### Quoted DSL 执行流程

```
Scala DSL 代码
    ↓
[编译时] QuoteMacro → Parser → AST
    ↓
[编译时] Lifter → Serialized AST
    ↓
生成 Quoted[T](ast, lifts, runtimeQuotes)
    ↓
[运行时] run() → Idiom.translate() → SQL 字符串
    ↓
[运行时] JDBC 执行 → 结果
```

### AST 节点类型（来自 quill-engine）

**查询节点**：Entity, Filter, Map, FlatMap, SortBy, GroupBy, Join, Take, Drop, Union, Distinct

**操作节点**：Insert, Update, Delete, Returning, OnConflict

**值节点**：Constant, NullValue, Tuple, CaseClass, Ident, Property

---

## 第一步：jOOQ Context 实现

### 1.1 架构设计

```
Quoted[T](ast, lifts, runtimeQuotes)
    ↓
[运行时] JooqContext.run()
    ↓
[运行时] JooqAstTranslator.translate(ast) → jOOQ DSL 对象
    ↓
[运行时] DSLContext.execute() → 结果
```

### 1.2 新模块结构

```
quill-jooq/
├── build.sbt
└── src/main/scala/io/getquill/
    ├── ZioJooqContext.scala        # ZIO jOOQ Context 实现
    ├── JooqAstTranslator.scala     # AST → jOOQ DSL 翻译器
    ├── JooqLiftExtractor.scala     # lift 参数绑定
    └── JooqResultExtractor.scala   # 结果集解析
```

### 1.3 核心类设计

#### ZioJooqContext.scala

```scala
import org.jooq.SQLDialect
import zio._
import javax.sql.DataSource

class ZioJooqContext[+N <: NamingStrategy](
    val dialect: SQLDialect,
    val naming: N
) {
  type Result[T] = ZIO[DataSource, SQLException, T]

  // 查询执行
  inline def run[T](inline quoted: Quoted[Query[T]]): Result[List[T]]

  // 动作执行
  inline def run[E](inline quoted: Quoted[Action[E]]): Result[Long]
  inline def run[E, T](inline quoted: Quoted[ActionReturning[E, T]]): Result[T]

  // 批量操作
  inline def run[I, A <: Action[I]](inline quoted: Quoted[BatchAction[A]]): Result[List[Long]]
}
```

#### JooqAstTranslator.scala

```scala
object JooqAstTranslator {
  // AST → jOOQ Select
  def translateQuery(ast: Ast, ctx: DSLContext): SelectQuery[Record]

  // AST → jOOQ Insert/Update/Delete
  def translateAction(ast: Ast, ctx: DSLContext): Query

  // 递归翻译各种 AST 节点
  private def translateEntity(e: Entity): Table[_]
  private def translateFilter(f: Filter): Condition
  private def translateMap(m: Map): SelectFieldOrAsterisk
  private def translateJoin(j: Join): TableLike[_]
  private def translateSortBy(s: SortBy): SortField[_]
  // ... 其他节点翻译
}
```

### 1.4 AST → jOOQ 映射规则

| AST 节点 | jOOQ DSL |
|---------|----------|
| `Entity(name)` | `DSL.table(name)` |
| `Filter(query, pred)` | `query.where(pred)` |
| `Map(query, body)` | `query.select(body)` |
| `SortBy(query, by, ord)` | `query.orderBy(by)` |
| `Take(query, n)` | `query.limit(n)` |
| `Drop(query, n)` | `query.offset(n)` |
| `Join(a, b, on)` | `a.join(b).on(on)` |
| `GroupBy(query, by)` | `query.groupBy(by)` |
| `Distinct(query)` | `query.distinct()` |
| `Insert(entity, assignments)` | `ctx.insertInto(table).set(...)` |
| `Update(entity, assignments)` | `ctx.update(table).set(...).where(...)` |
| `Delete(entity)` | `ctx.deleteFrom(table).where(...)` |

### 1.5 表达式翻译

| AST 表达式 | jOOQ 表达式 |
|-----------|-------------|
| `BinaryOperation(a, ==, b)` | `a.eq(b)` |
| `BinaryOperation(a, !=, b)` | `a.ne(b)` |
| `BinaryOperation(a, >, b)` | `a.gt(b)` |
| `BinaryOperation(a, <, b)` | `a.lt(b)` |
| `BinaryOperation(a, &&, b)` | `a.and(b)` |
| `BinaryOperation(a, \|\|, b)` | `a.or(b)` |
| `BinaryOperation(a, +, b)` | `a.add(b)` |
| `Property(ident, name)` | `DSL.field(name)` |
| `Constant(v)` | `DSL.val(v)` |
| `ScalarTag(uid)` | 占位符绑定 |

### 1.6 实现步骤

1. **创建 quill-jooq 模块**
   - 添加 jOOQ 依赖
   - 创建基础目录结构

2. **实现 JooqAstTranslator**
   - 查询翻译（SELECT）
   - 条件翻译（WHERE）
   - 排序翻译（ORDER BY）
   - 分组翻译（GROUP BY）
   - 连接翻译（JOIN）
   - 分页翻译（LIMIT/OFFSET）

3. **实现 JooqContext**
   - run 方法（查询）
   - run 方法（动作）
   - 批量操作支持

4. **实现参数绑定**
   - lift 值绑定到 jOOQ 参数

5. **实现结果提取**
   - jOOQ Record → Scala Case Class

6. **测试**
   - 单元测试各种 AST 翻译
   - 集成测试实际数据库操作

---

## 第二步：删除其他 Context

### 2.1 待删除模块

- `quill-jdbc` - 同步 JDBC Context
- `quill-jdbc-zio` - ZIO JDBC Context
- `quill-zio` - ZIO 通用模块
- `quill-cassandra` - Cassandra Context
- `quill-cassandra-zio` - Cassandra ZIO Context
- `quill-doobie` - Doobie 集成

### 2.2 需要保留的模块

- `quill-sql` - 核心 DSL、AST、宏（需要精简）
- `quill-jooq` - 新的 jOOQ Context
- `quill-caliban` - 根据需要决定是否保留

### 2.3 精简 quill-sql

删除：
- 现有的 Idiom 实现（PostgresDialect, MysqlDialect 等）
- SQL 字符串生成相关代码
- 不再需要的 Context 基类

保留：
- Quoted DSL 定义
- AST 相关代码
- 宏处理代码
- 编码/解码器

---

## 确认的方案

### 方言策略
**选择：多方言支持**
- 通过 jOOQ SQLDialect 配置支持 PostgreSQL, MySQL, H2 等多种数据库
- JooqContext 构造时传入 SQLDialect 参数

### ZIO 集成
**选择：只实现 ZioJooqContext**
- 只实现 ZIO 异步版本
- 返回 `ZIO[DataSource, SQLException, T]` effect

### Caliban 模块
**选择：删除**
- 删除 quill-caliban 模块

---

## 依赖变更

### 新增依赖

```scala
// build.sbt
libraryDependencies ++= Seq(
  "org.jooq" % "jooq" % "3.19.x",
  "org.jooq" % "jooq-meta" % "3.19.x"
)
```

### 删除依赖

- HikariCP（如果 jOOQ 自带连接池管理）
- ZIO 相关依赖（如果不需要 ZIO 集成）
- Cassandra 驱动

---

## 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| AST 覆盖不完全 | 某些查询无法翻译 | 逐步添加 AST 节点支持 |
| jOOQ API 不熟悉 | 开发效率低 | 学习 jOOQ 文档 |
| 性能差异 | 可能比原实现慢 | 基准测试对比 |
| 功能回归 | 原有功能丢失 | 完善测试用例 |

---

## 下一步行动

确认上述待确认问题后，开始实施第一步：
1. 创建 quill-jooq 模块
2. 实现核心 AST 翻译器
3. 实现 JooqContext
4. 添加测试
