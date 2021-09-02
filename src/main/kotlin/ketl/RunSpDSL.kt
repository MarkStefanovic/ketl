package ketl

import com.zaxxer.hikari.HikariDataSource
import ketl.domain.Job
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
fun <Ctx : JobContext> execPgSp(
  ctx: Ctx,
  ds: HikariDataSource,
  schemaName: String,
  spName: String,
  schedule: List<Schedule>,
  timeout: Duration = Duration.seconds(3600),
  retries: Int = 0,
  init: SpSQL.Builder.() -> Unit = {},
): Job<Ctx> =
  execSp(
    ctx = ctx,
    ds = ds,
    schemaName = schemaName,
    spName = spName,
    dialect = DbDialect.PostgreSQL,
    schedule = schedule,
    timeout = timeout,
    retries = retries,
    init = init,
  )

@ExperimentalTime
private fun <Ctx : JobContext> execSp(
  ctx: Ctx,
  ds: HikariDataSource,
  schemaName: String,
  spName: String,
  dialect: DbDialect,
  schedule: List<Schedule>,
  timeout: Duration,
  retries: Int,
  init: SpSQL.Builder.() -> Unit,
): Job<Ctx> {
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
  return Job(
    name = jobName,
    schedule = schedule,
    timeout = timeout,
    retries = retries,
    ctx = ctx,
    onRun = {
      log.info("Executing '$sql'.")
      ds.connection.use { con ->
        con.createStatement().use { stmt ->
          try {
            val executionMillis = measureTimeMillis { stmt.execute(sql) }
            log.info("Successfully ran '$sql.' in $executionMillis milliseconds.")
          } catch (e: Exception) {
            log.error("An error occurred while running '$sql':\n  Error: ${e.message}")
          }
        }
      }
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
              parameters.values.joinToString(", ") { param ->
                param.sql
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
