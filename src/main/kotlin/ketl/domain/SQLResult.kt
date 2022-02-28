package ketl.domain

sealed class SQLResult(open val sql: String) {
  data class Error(
    override val sql: String,
    val error: Exception,
  ) : SQLResult(sql = sql)

  data class Success(
    override val sql: String,
    val parameters: Map<String, Any>?,
  ) : SQLResult(sql = sql) {
    override fun toString(): String {
      val parameterSQL = if (parameters == null) {
        ""
      } else {
        "\nParameters:\n  " + parameters.map { entry ->
          "\n  ${entry.key}: ${entry.value}"
        }.joinToString("\n    ")
      }
      return "SQL:\n  ${sql.split("\n").joinToString("\n  ")}$parameterSQL"
    }
  }
}
