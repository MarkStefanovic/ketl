package ketl

import com.zaxxer.hikari.HikariDataSource
import ketl.domain.ETLJob
import ketl.domain.JobContext
import ketl.domain.Schedule
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlin.system.measureTimeMillis
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@DslMarker annotation class RunSpDSL

enum class DbDialect {
  PostgreSQL,
}

@ExperimentalTime
fun <Ctx : JobContext> Ctx.execPgSp(
  ds: HikariDataSource,
  schemaName: String,
  spName: String,
  schedule: Schedule,
  dependencies: Set<String> = setOf(),
  timeout: Duration = Duration.seconds(3600),
  retries: Int = 0,
  init: SpSQL.Builder.() -> Unit = {},
): ETLJob<Ctx> =
  execSp(
    ds = ds,
    schemaName = schemaName,
    spName = spName,
    dialect = DbDialect.PostgreSQL,
    schedule = schedule,
    dependencies = dependencies,
    timeout = timeout,
    retries = retries,
    init = init,
  )

@ExperimentalTime
private fun <Ctx : JobContext> Ctx.execSp(
  ds: HikariDataSource,
  schemaName: String,
  spName: String,
  dialect: DbDialect,
  schedule: Schedule,
  timeout: Duration,
  retries: Int,
  dependencies: Set<String>,
  init: SpSQL.Builder.() -> Unit,
): ETLJob<Ctx> {
  val jobName = "$schemaName.$spName"
  val sql =
    SpSQL.Builder(
      dialect = dialect,
      schemaName = schemaName,
      spName = spName,
    )
      .apply(init)
      .build()
      .sql
  return ETLJob(
    name = jobName,
    schedule = schedule,
    timeout = timeout,
    retries = retries,
    dependencies = dependencies,
    ctx = this,
    onRun = { log ->
      log.info("Executing '$sql'.")
      ds.connection.use { con ->
        con.createStatement().use { stmt ->
          try {
            val executionMillis = measureTimeMillis { stmt.execute(sql) }
            log.info("Successfully ran '$sql.' in $executionMillis milliseconds.")
          } catch (e: Exception) {
            failure("An error occurred while running '$sql':\n  Error: ${e.message}")
          }
        }
      }
      success()
    }
  )
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
