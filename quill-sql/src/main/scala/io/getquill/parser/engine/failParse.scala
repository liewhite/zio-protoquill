package io.getquill.parser.engine

import scala.quoted._
import io.getquill.util.Format

object failParse {
  enum ThrowInfo {
    case AstClass(astClass: Class[?])
    case Message(msg: String)
  }

  def apply(expr: Expr[?], astClass: Class[?])(using Quotes): Nothing =
    apply(expr, ThrowInfo.AstClass(astClass))

  def apply(expr: Expr[?], msg: String)(using Quotes): Nothing =
    apply(expr, ThrowInfo.Message(msg))

  def apply(expr: Expr[?], throwInfo: ThrowInfo)(using Quotes): Nothing = {
    import quotes.reflect._
    // When errors are printed, make sure to deserialize parts of the AST that may be serialized,
    // otherwise in the expression printout there will garbled base46 characters everywhere
    val term = io.getquill.metaprog.DeserializeAstInstances(expr).asTerm
    val message =
      throwInfo match {
        case ThrowInfo.Message(msg)       => msg
        case ThrowInfo.AstClass(astClass) => s"Tree cannot be parsed to '${astClass.getSimpleName}'"
      }

    val traces = Thread.currentThread.getStackTrace.take(50).map("  " + _.toString).mkString("\n")
    report.errorAndAbort(
      s"""|
      |s"==== ${message} ====
      |  ${Format(Printer.TreeShortCode.show(term)) /* Or Maybe just expr? */}
      |==== Extractors ===
      |  ${Format(Printer.TreeStructure.show(term))}
      |==== Stacktrace ===
      |${traces}""".stripMargin,
      expr
    )
  } // end apply
} // end failParse
