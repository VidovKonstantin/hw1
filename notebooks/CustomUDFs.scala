import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object CustomUDFs {
  def splitTags(s: String): Array[String] = s.split('|')
  val splitTagsUDF: UserDefinedFunction = udf(splitTags _)
}
