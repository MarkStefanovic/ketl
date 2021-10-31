package ketl

import com.zaxxer.hikari.HikariDataSource
import java.time.LocalDate
import java.time.format.DateTimeFormatter

@DslMarker annotation class RunSpDSL

enum class DbDialect {
  PostgreSQL,
}

fun runSp(
  ds: HikariDataSource,
  schemaName: String,
  spName: String,
  dialect: DbDialect,
  init: SpSQL.Builder.() -> Unit,
) {
  val sql =
    SpSQL.Builder(
      dialect = dialect,
      schemaName = schemaName,
      spName = spName,
    )
      .apply(init)
      .build()
      .sql

  ds.connection.use { con ->
    con.createStatement().use { stmt ->
      stmt.execute(sql)
    }
  }
}

data class SpSQL(val sql: String) {
  @RunSpDSL
  class Builder(
    val dialect: DbDialect,
    val schemaName: String,
    val spName: String,
  ) {
    private var parameters: SpParams = SpParams(listOf())

    fun build(): SpSQL {
      val sql =
        when (dialect) {
          DbDialect.PostgreSQL -> {
            val paramSQL =
              if (parameters.values.isEmpty()) {
                ""
              } else {
                parameters.values.joinToString(", ") { param ->
                  param.sql
                }
              }
            "CALL $schemaName.$spName($paramSQL)"
          }
        }
      return SpSQL(sql)
    }

    fun params(init: SpParams.Builder.() -> Unit) {
      parameters = SpParams.Builder(dialect).apply(init).build()
    }
  }
}

data class SpParams(val values: List<SpParam>) {
  @RunSpDSL
  class Builder(private val dialect: DbDialect) {
    private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    private val params = mutableListOf<SpParam>()

    fun build() = SpParams(params)

    fun boolParam(name: String, value: Boolean) {
      val strValue = when (dialect) {
        DbDialect.PostgreSQL -> if (value) "TRUE" else "FALSE"
      }
      val sql = when (dialect) {
        DbDialect.PostgreSQL -> "$name := $strValue"
      }
      params.add(SpParam(name = name, value = value, sql = sql))
    }

    fun dateParam(name: String, value: LocalDate) {
      val strValue = value.format(dateFormatter)
      val sql = when (dialect) {
        DbDialect.PostgreSQL -> "$name := '$strValue'::DATE"
      }
      params.add(SpParam(name = name, value = value, sql = sql))
    }

    fun intParam(name: String, value: String) {
      params.add(SpParam(name = name, value = value, sql = "$name := $value"))
    }

    fun textParam(name: String, value: String) {
      params.add(SpParam(name = name, value = value, sql = "$name := '$value'"))
    }
  }
}

data class SpParam(val name: String, val value: Any, val sql: String)
