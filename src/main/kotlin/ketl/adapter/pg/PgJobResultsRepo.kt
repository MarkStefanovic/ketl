@file:Suppress("SqlResolve", "DuplicatedCode", "SqlNoDataSourceInspection")

package ketl.adapter.pg

import ketl.domain.DbJobResultsRepo
import ketl.domain.JobResult
import ketl.domain.KETLError
import ketl.domain.SQLResult
import java.sql.Connection
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import javax.sql.DataSource

data class PgJobResultsRepo(
  val schema: String,
  private val ds: DataSource,
) : DbJobResultsRepo {
  override fun createTables(): SQLResult =
    ds.connection.use { con ->
      createJobResultSnapshotTable(con = con, schema = schema) +
        createJobResultTable(con = con, schema = schema)
    }

  override fun add(jobResult: JobResult): SQLResult =
    ds.connection.use { con ->
      addResultToJobResultSnapshotTable(con = con, schema = schema, jobResult = jobResult) +
        addResultToJobResultTable(con = con, schema = schema, jobResult = jobResult)
    }

  override fun deleteBefore(ts: LocalDateTime): SQLResult =
    ds.connection.use { con ->
      deleteResultsOnJobResultSnapshotTableBefore(con = con, schema = schema, ts = ts) +
        deleteResultsOnJobResultTableBefore(con = con, schema = schema, ts = ts)
    }

  override fun getLatestResults(): Pair<SQLResult, Set<JobResult>> {
    // language=PostgreSQL
    val sql = """
      |SELECT
      |  jr.job_name
      |, jr.start_time
      |, jr.end_time
      |, jr.result
      |, jr.error_message
      |, jr.skip_reason
      |FROM $schema.job_result_snapshot AS jr
    """.trimMargin()

    return try {
      ds.connection.use { connection ->
        connection.createStatement().use { statement ->
          statement.queryTimeout = 60

          val result = statement.executeQuery(sql)

          val jobResults = mutableListOf<JobResult>()
          while (result.next()) {
            val jobName = result.getString("job_name")
            val startTime = result.getTimestamp("start_time").toLocalDateTime()
            val endTime = result.getTimestamp("end_time").toLocalDateTime()
            val resultTypeName = result.getString("result")
            val errorMessage = result.getObject("error_message") as String?
            val skipReason = result.getObject("skip_reason") as String?

            val jobResult: JobResult = when (resultTypeName) {
              "cancelled" -> JobResult.Cancelled(
                jobName = jobName,
                start = startTime,
                end = endTime,
              )
              "failed" -> JobResult.Failed(
                jobName = jobName,
                start = startTime,
                end = endTime,
                errorMessage = errorMessage ?: "No message was provided.",
              )
              "skipped" -> JobResult.Skipped(
                jobName = jobName,
                start = startTime,
                end = endTime,
                reason = skipReason ?: "No reason was provided.",
              )
              "successful" -> JobResult.Successful(
                jobName = jobName,
                start = startTime,
                end = endTime,
              )
              else -> throw KETLError.UnrecognizedResultTypeName(resultTypeName)
            }

            jobResults.add(jobResult)
          }

          SQLResult.Success(
            sql = sql,
            parameters = null,
          ) to jobResults.toSet()
        }
      }
    } catch (e: Exception) {
      SQLResult.Error(sql = sql, parameters = null, error = e) to emptySet()
    }
  }
}

private fun addResultToJobResultTable(
  con: Connection,
  schema: String,
  jobResult: JobResult,
): SQLResult {
  // language=PostgreSQL
  val sql = """
    |INSERT INTO $schema.job_result (
    |  job_name
    |, start_time
    |, end_time
    |, result
    |, error_message
    |, skip_reason
    |) VALUES (
    |  ?
    |, ?
    |, ?
    |, ?
    |, ?
    |, ?
    |)
  """.trimMargin()

  return try {
    con.prepareStatement(sql).use { preparedStatement ->
      preparedStatement.queryTimeout = 60

      val resultName: String = when (jobResult) {
        is JobResult.Cancelled -> "cancelled"
        is JobResult.Failed -> "failed"
        is JobResult.Skipped -> "skipped"
        is JobResult.Successful -> "successful"
      }

      val errorMessage: String? = if (jobResult is JobResult.Failed) {
        jobResult.errorMessage
      } else {
        null
      }

      val skipReason: String? = if (jobResult is JobResult.Skipped) {
        jobResult.reason
      } else {
        null
      }

      preparedStatement.setString(1, jobResult.jobName)
      preparedStatement.setTimestamp(2, Timestamp.valueOf(jobResult.start))
      preparedStatement.setTimestamp(3, Timestamp.valueOf(jobResult.end))
      preparedStatement.setString(4, resultName)
      if (errorMessage == null) {
        preparedStatement.setNull(5, Types.VARCHAR)
      } else {
        preparedStatement.setString(5, errorMessage)
      }
      if (skipReason == null) {
        preparedStatement.setNull(6, Types.VARCHAR)
      } else {
        preparedStatement.setString(6, skipReason)
      }

      preparedStatement.executeUpdate()

      SQLResult.Success(
        sql = sql,
        parameters = mapOf(
          "job_name" to jobResult.jobName,
          "start_time" to jobResult.start,
          "end_time" to jobResult.end,
          "result" to resultName,
          "error_message" to errorMessage,
          "skip_reason" to skipReason,
        ),
      )
    }
  } catch (e: Exception) {
    SQLResult.Error(
      sql = sql,
      parameters = mapOf(
        "schema" to schema,
        "jobResult" to jobResult,
      ),
      error = e,
    )
  }
}

private fun addResultToJobResultSnapshotTable(
  con: Connection,
  schema: String,
  jobResult: JobResult,
): SQLResult {
  // language=PostgreSQL
  val sql = """
    |INSERT INTO $schema.job_result_snapshot (
    |  job_name
    |, start_time
    |, end_time
    |, result
    |, error_message
    |, skip_reason
    |) VALUES (
    |  ?
    |, ?
    |, ?
    |, ?
    |, ?
    |, ?
    |) 
    |ON CONFLICT (job_name)
    |DO UPDATE 
    |SET
    |  start_time = EXCLUDED.start_time
    |, end_time = EXCLUDED.end_time 
    |, result = EXCLUDED.result
    |, error_message = EXCLUDED.error_message
    |, skip_reason = EXCLUDED.skip_reason
    |WHERE 
    |(
    |  $schema.job_result_snapshot.start_time
    |, $schema.job_result_snapshot.end_time
    |, $schema.job_result_snapshot.result
    |, $schema.job_result_snapshot.error_message
    |, $schema.job_result_snapshot.skip_reason
    |) IS DISTINCT FROM (
    |  EXCLUDED.start_time 
    |, EXCLUDED.end_time
    |, EXCLUDED.result
    |, EXCLUDED.error_message
    |, EXCLUDED.skip_reason
    |)
  """.trimMargin()

  return try {
    con.prepareStatement(sql.trimIndent()).use { preparedStatement ->
      preparedStatement.queryTimeout = 60

      val resultName: String = when (jobResult) {
        is JobResult.Cancelled -> "cancelled"
        is JobResult.Failed -> "failed"
        is JobResult.Skipped -> "skipped"
        is JobResult.Successful -> "successful"
      }

      val errorMessage: String? = if (jobResult is JobResult.Failed) {
        jobResult.errorMessage
      } else {
        null
      }

      val skipReason: String? = if (jobResult is JobResult.Skipped) {
        jobResult.reason
      } else {
        null
      }

      preparedStatement.setString(1, jobResult.jobName)
      preparedStatement.setTimestamp(2, Timestamp.valueOf(jobResult.start))
      preparedStatement.setTimestamp(3, Timestamp.valueOf(jobResult.end))
      preparedStatement.setString(4, resultName)
      if (errorMessage == null) {
        preparedStatement.setNull(5, Types.VARCHAR)
      } else {
        preparedStatement.setString(5, errorMessage)
      }
      if (skipReason == null) {
        preparedStatement.setNull(6, Types.VARCHAR)
      } else {
        preparedStatement.setString(6, skipReason)
      }

      preparedStatement.executeUpdate()

      SQLResult.Success(
        sql = sql,
        parameters = mapOf(
          "job_name" to jobResult.jobName,
          "start_time" to jobResult.start,
          "end_time" to jobResult.end,
          "result" to resultName,
          "error_message" to errorMessage,
          "skip_reason" to skipReason,
        ),
      )
    }
  } catch (e: Exception) {
    SQLResult.Error(
      sql = sql,
      parameters = mapOf(
        "schema" to schema,
        "jobResult" to jobResult,
      ),
      error = e,
    )
  }
}

private fun createJobResultTable(
  con: Connection,
  schema: String,
): SQLResult {
  // language=PostgreSQL
  val createTableSQL = """
    |CREATE TABLE IF NOT EXISTS $schema.job_result (
    |  id SERIAL PRIMARY KEY
    |, job_name TEXT NOT NULL CHECK (LENGTH(job_name) > 0)
    |, start_time TIMESTAMP NOT NULL
    |, end_time TIMESTAMP NOT NULL CHECK (end_time >= start_time)
    |, result TEXT NOT NULL CHECK (
    |    result IN ('cancelled', 'failed', 'skipped', 'successful')
    |  )
    |, error_message TEXT NULL CHECK (
    |    (result = 'failed' AND LENGTH(error_message) > 0)
    |    OR (result <> 'failed' AND error_message IS NULL)
    |  )
    |, skip_reason TEXT NULL CHECK (
    |    (result = 'skipped' AND LENGTH(skip_reason) > 0)
    |    OR (result <> 'skipped' AND skip_reason IS NULL)
    |  )
    |)
  """.trimMargin()

  // language=PostgreSQL
  val startTimeIndexSQL = """
    |-- noinspection SqlResolve @ table/"job_result"
    |CREATE INDEX IF NOT EXISTS ix_job_result_job_name_start_time 
    |  ON $schema.job_result (job_name, start_time)
  """.trimMargin()

  val fullSQL = "$createTableSQL;\n$startTimeIndexSQL"

  return try {
    con.createStatement().use { statement ->
      statement.queryTimeout = 60

      statement.execute(createTableSQL.trimIndent())

      statement.execute(startTimeIndexSQL.trimIndent())
    }
    SQLResult.Success(sql = fullSQL, parameters = mapOf("schema" to schema))
  } catch (e: Exception) {
    SQLResult.Error(sql = fullSQL, parameters = mapOf("schema" to schema), error = e)
  }
}

private fun createJobResultSnapshotTable(
  con: Connection,
  schema: String,
): SQLResult {
  // language=PostgreSQL
  val sql = """
    |CREATE TABLE IF NOT EXISTS $schema.job_result_snapshot (
    |  job_name TEXT NOT NULL CHECK (LENGTH(job_name) > 0)
    |, start_time TIMESTAMP NOT NULL
    |, end_time TIMESTAMP NOT NULL CHECK (end_time >= start_time)
    |, result TEXT NOT NULL CHECK (
    |    result IN ('cancelled', 'failed', 'skipped', 'successful')
    |  )
    |, error_message TEXT NULL CHECK (
    |    (result = 'failed' AND LENGTH(error_message) > 0)
    |    OR (result <> 'failed' AND error_message IS NULL)
    |  )
    |, skip_reason TEXT NULL CHECK (
    |    (result = 'skipped' AND LENGTH(skip_reason) > 0)
    |    OR (result <> 'skipped' AND skip_reason IS NULL)
    |  )
    |, PRIMARY KEY (job_name)
    |)
  """.trimMargin()

  return try {
    con.createStatement().use { statement ->
      statement.queryTimeout = 60

      statement.execute(sql)
    }

    SQLResult.Success(sql = sql, parameters = mapOf("schema" to schema))
  } catch (e: Exception) {
    SQLResult.Error(sql = sql, parameters = mapOf("schema" to schema), error = e)
  }
}

private fun deleteResultsOnJobResultTableBefore(
  con: Connection,
  schema: String,
  ts: LocalDateTime,
): SQLResult {
  // language=PostgreSQL
  val sql = "DELETE FROM $schema.job_result WHERE start_time < ?"

  return try {
    con.prepareStatement(sql).use { preparedStatement ->
      preparedStatement.queryTimeout = 60

      preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

      preparedStatement.executeUpdate()
    }

    SQLResult.Success(
      sql = sql,
      parameters = mapOf(
        "schema" to schema,
        "start_time" to ts,
      ),
    )
  } catch (e: Exception) {
    SQLResult.Error(
      sql = sql,
      parameters = mapOf(
        "schema" to schema,
        "start_time" to ts,
      ),
      error = e,
    )
  }
}

private fun deleteResultsOnJobResultSnapshotTableBefore(
  con: Connection,
  schema: String,
  ts: LocalDateTime,
): SQLResult {
  // language=PostgreSQL
  val sql = "DELETE FROM $schema.job_result_snapshot WHERE start_time < ?"

  return try {
    con.prepareStatement(sql).use { preparedStatement ->
      preparedStatement.queryTimeout = 60
      preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

      preparedStatement.executeUpdate()
    }

    SQLResult.Success(
      sql = sql,
      parameters = mapOf(
        "schema" to schema,
        "ts" to ts,
      ),
    )
  } catch (e: Exception) {
    SQLResult.Error(
      sql = sql,
      parameters = mapOf(
        "schema" to schema,
        "ts" to ts,
      ),
      error = e,
    )
  }
}
