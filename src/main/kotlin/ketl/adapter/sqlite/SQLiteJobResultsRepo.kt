@file:Suppress("SqlResolve")

package ketl.adapter.sqlite

import ketl.domain.DbJobResultsRepo
import ketl.domain.JobResult
import ketl.domain.KETLError
import ketl.domain.Log
import java.sql.Connection
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import javax.sql.DataSource

data class SQLiteJobResultsRepo(
  private val ds: DataSource,
  private val log: Log,
) : DbJobResultsRepo {
  override suspend fun createTables() {
    ds.connection.use { con ->
      createJobResultSnapshotTable(con = con, log = log)

      createJobResultTable(con = con, log = log)
    }
  }

  override suspend fun add(jobResult: JobResult) {
    ds.connection.use { con ->
      addResultToJobResultSnapshotTable(con = con, jobResult = jobResult, log = log)

      addResultToJobResultTable(con = con, jobResult = jobResult, log = log)
    }
  }

  override suspend fun deleteBefore(ts: LocalDateTime) {
    ds.connection.use { con ->
      deleteResultsOnJobResultSnapshotTableBefore(con = con, ts = ts, log = log)

      deleteResultsOnJobResultTableBefore(con = con, ts = ts, log = log)
    }
  }

  override suspend fun getLatestResults(): Set<JobResult> {
    // language=SQLite
    val sql = """
      |SELECT
      |  jr.job_name
      |, jr.start_time
      |, jr.end_time
      |, jr.result
      |, jr.error_message
      |, jr.skip_reason
      |FROM ketl_job_result_snapshot AS jr
    """.trimMargin()

    log.debug(sql)

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

        return jobResults.toSet()
      }
    }
  }
}

private suspend fun addResultToJobResultTable(
  con: Connection,
  jobResult: JobResult,
  log: Log,
) {
  // language=SQLite
  val sql = """
    |INSERT INTO ketl_job_result (
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

  log.debug(sql)

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
  }
}

@Suppress("SqlResolve")
private suspend fun addResultToJobResultSnapshotTable(
  con: Connection,
  jobResult: JobResult,
  log: Log,
) {
  // language=SQLite
  val sql = """
    |INSERT INTO ketl_job_result_snapshot (
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
    |  start_time <> EXCLUDED.start_time
    |  OR end_time <> EXCLUDED.end_time
    |  OR result <> EXCLUDED.result
    |  OR COALESCE(error_message, '') <> COALESCE(EXCLUDED.error_message, '')
    |  OR COALESCE(skip_reason, '') <> COALESCE(EXCLUDED.skip_reason, '')
  """.trimMargin()

  log.debug(sql)

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
  }
}

private suspend fun createJobResultTable(
  con: Connection,
  log: Log,
) =
  con.createStatement().use { statement ->
    statement.queryTimeout = 60

    // language=SQLite
    val createTableSQL = """
      |CREATE TABLE IF NOT EXISTS ketl_job_result (
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

    log.debug(createTableSQL)

    statement.execute(createTableSQL.trimIndent())

    // language=SQLite
    val startTimeIndexSQL = """
      |CREATE INDEX IF NOT EXISTS ix_job_result_job_name_start_time 
      |  ON ketl_job_result (job_name, start_time)
    """.trimMargin()

    log.debug(startTimeIndexSQL)

    statement.execute(startTimeIndexSQL.trimIndent())
  }

private suspend fun createJobResultSnapshotTable(
  con: Connection,
  log: Log,
) =
  con.createStatement().use { statement ->
    statement.queryTimeout = 60

    // language=SQLite
    val sql = """
      |CREATE TABLE IF NOT EXISTS ketl_job_result_snapshot (
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

    log.debug(sql)

    statement.execute(sql)
  }

private suspend fun deleteResultsOnJobResultTableBefore(
  con: Connection,
  ts: LocalDateTime,
  log: Log,
) {
  // language=SQLite
  val sql = """
    |DELETE FROM ketl_job_result
    |WHERE start_time < ?
  """.trimMargin()

  log.debug(sql)

  con.prepareStatement(sql).use { preparedStatement ->
    preparedStatement.queryTimeout = 60

    preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

    preparedStatement.executeUpdate()
  }
}

private suspend fun deleteResultsOnJobResultSnapshotTableBefore(
  con: Connection,
  ts: LocalDateTime,
  log: Log,
) {
  // language=SQLite
  val sql = """
    |DELETE FROM ketl_job_result_snapshot 
    |WHERE start_time < ?
  """.trimMargin()

  log.debug(sql)

  con.prepareStatement(sql).use { preparedStatement ->
    preparedStatement.queryTimeout = 60

    preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

    preparedStatement.executeUpdate()
  }
}
