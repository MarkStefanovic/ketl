package ketl.domain

sealed class SQLResult(
  open val sql: String,
  open val parameters: Map<String, Any?>?,
) {
  data class Error(
    override val sql: String,
    override val parameters: Map<String, Any?>?,
    val error: Exception,
  ) : SQLResult(sql = sql, parameters = parameters) {
    override fun toString(): String {
      val parameterSQL = if (parameters == null) {
        ""
      } else {
        "\nParameters:\n  " + parameters.map { entry ->
          "\n  ${entry.key}: ${entry.value}"
        }.joinToString("\n    ")
      }

      return "Error:\n  ${error.stackTraceToString().split("\n").joinToString("\n  ")}\n" +
        "SQL:\n  ${sql.split("\n").joinToString("\n  ")}$parameterSQL"
    }
  }

  data class Success(
    override val sql: String,
    override val parameters: Map<String, Any?>?,
  ) : SQLResult(sql = sql, parameters = parameters) {
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

  operator fun plus(other: SQLResult): SQLResult =
    when (this) {
      is Error -> this
      is Success -> when (other) {
        is Error -> other
        is Success -> Success(
          sql = this.sql + ";\n" + other.sql,
          parameters = mapOf(
            "first" to this.parameters,
            "second" to other.parameters,
          ),
        )
      }
    }
}
