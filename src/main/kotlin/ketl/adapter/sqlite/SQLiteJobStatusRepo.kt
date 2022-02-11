@file:Suppress("SqlResolve")

package ketl.adapter.sqlite

import ketl.domain.DbJobStatusRepo
import ketl.domain.JobStatus
import ketl.domain.KETLErrror
import ketl.domain.Log
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import javax.sql.DataSource

class SQLiteJobStatusRepo(
  private val ds: DataSource,
  private val log: Log,
) : DbJobStatusRepo {
  override suspend fun add(jobStatus: JobStatus) {
    ds.connection.use { connection ->
      addToHistoricalTable(con = connection, jobStatus = jobStatus, log = log)
      addToSnapshotTable(con = connection, jobStatus = jobStatus, log = log)
    }
  }

  override suspend fun cancelRunningJobs() {
    //language=SQLite
    val sql = """
      |UPDATE ketl_job_status_snapshot
      |SET status = 'cancelled'
      |WHERE status = 'running'
    """.trimMargin()

    log.debug(sql)

    ds.connection.use { connection ->
      connection.createStatement().use { statement ->
        statement.queryTimeout = 60

        statement.execute(sql)
      }
    }
  }

  override suspend fun currentStatus(): Set<JobStatus> {
    //language=SQLite
    val sql = """
      |SELECT
      |  job_name
      |, status
      |, error_message
      |, skip_reason
      |, ts
      |FROM ketl_job_status_snapshot
    """.trimMargin()

    val jobStatuses = mutableListOf<JobStatus>()
    ds.connection.use { connection ->
      connection.createStatement().use { statement ->
        statement.queryTimeout = 60

        statement.executeQuery(sql).use { resultSet ->
          while (resultSet.next()) {
            val jobStatus = getJobStatusFromResultSet(resultSet)
            jobStatuses.add(jobStatus)
          }
        }
      }
    }
    return jobStatuses.toSet()
  }

  override suspend fun createTables() {
    //language=SQLite
    val createHistTableSQL = """
      |-- noinspection SqlResolve @ column/"result"
      |CREATE TABLE IF NOT EXISTS ketl_job_status (
      |  id SERIAL PRIMARY KEY
      |, job_name TEXT NOT NULL CHECK (LENGTH(job_name) > 0)
      |, status TEXT NOT NULL CHECK (
      |    status IN ('cancelled', 'failed', 'running', 'skipped', 'successful')
      |  )
      |, error_message TEXT NULL CHECK (
      |    (status = 'failed' AND LENGTH(error_message) > 0)
      |    OR (status <> 'failed' AND error_message IS NULL)
      |  )
      |, skip_reason TEXT NULL CHECK (
      |    (status = 'skipped' AND LENGTH(skip_reason) > 0)
      |    OR (status <> 'skipped' AND skip_reason IS NULL)
      |  )
      |, ts TIMESTAMP NOT NULL
      |)
    """.trimMargin()

    log.debug(createHistTableSQL)

    //language=SQLite
    val createSnapshotTableSQL = """
      |-- noinspection SqlResolve @ column/"result"
      |CREATE TABLE IF NOT EXISTS ketl_job_status_snapshot (
      |  job_name TEXT NOT NULL CHECK (LENGTH(job_name) > 0)
      |, status TEXT NOT NULL CHECK (
      |    status IN ('cancelled', 'failed', 'running', 'skipped', 'successful')
      |  )
      |, error_message TEXT NULL CHECK (
      |    (status = 'failed' AND LENGTH(error_message) > 0)
      |    OR (status <> 'failed' AND error_message IS NULL)
      |  )
      |, skip_reason TEXT NULL CHECK (
      |    (status = 'skipped' AND LENGTH(skip_reason) > 0)
      |    OR (status <> 'skipped' AND skip_reason IS NULL)
      |  )
      |, ts TIMESTAMP NOT NULL
      |, PRIMARY KEY (job_name)
      |)
    """.trimMargin()

    log.debug(createSnapshotTableSQL)

    ds.connection.use { connection ->
      connection.createStatement().use { statement ->
        statement.queryTimeout = 60

        statement.execute(createHistTableSQL)
        statement.execute(createSnapshotTableSQL)
      }
    }
  }

  override suspend fun deleteBefore(ts: LocalDateTime) {
    //language=SQLite
    val sql = """
      |-- noinspection SqlResolve @ table/"ketl_job_status"
      |DELETE FROM ketl_job_status
      |WHERE ts < ?
    """.trimMargin()

    log.debug(sql)

    ds.connection.use { connection ->
      deleteBeforeTsOnHistoricalTable(log = log, con = connection, ts = ts)
      deleteBeforeTsOnSnapshotTable(log = log, con = connection, ts = ts)
    }
  }
}

private suspend fun addToHistoricalTable(
  con: Connection,
  jobStatus: JobStatus,
  log: Log,
) {
  //language=SQLite
  val sql = """
    |-- noinspection SqlResolve @ table/"ketl_job_status"
    |INSERT INTO ketl_job_status (
    |  job_name
    |, status
    |, error_message
    |, skip_reason
    |, ts
    |) VALUES (
    |  ?
    |, ?
    |, ?
    |, ?
    |, ?
    |)  
  """.trimMargin()

  log.debug(sql)

  con.prepareStatement(sql).use { preparedStatement ->
    preparedStatement.queryTimeout = 60

    preparedStatement.setString(1, jobStatus.jobName)
    preparedStatement.setString(2, jobStatus.statusName)
    if (jobStatus is JobStatus.Failed) {
      preparedStatement.setString(3, jobStatus.errorMessage)
    } else {
      preparedStatement.setNull(3, Types.VARCHAR)
    }
    if (jobStatus is JobStatus.Skipped) {
      preparedStatement.setString(4, jobStatus.reason)
    } else {
      preparedStatement.setNull(4, Types.VARCHAR)
    }
    preparedStatement.setTimestamp(5, Timestamp.valueOf(jobStatus.ts))

    preparedStatement.execute()
  }
}

private suspend fun addToSnapshotTable(
  con: Connection,
  jobStatus: JobStatus,
  log: Log,
) {
  //language=SQLite
  val sql = """
    |-- noinspection SqlResolve @ table/"ketl_job_status_snapshot"
    |INSERT OR REPLACE INTO ketl_job_status_snapshot (
    |  job_name
    |, status
    |, error_message
    |, skip_reason
    |, ts
    |) VALUES (
    |  ?
    |, ?
    |, ?
    |, ?
    |, ?
    |)
  """.trimMargin()

  log.debug(sql)

  con.prepareStatement(sql).use { preparedStatement ->
    preparedStatement.queryTimeout = 60

    preparedStatement.setString(1, jobStatus.jobName)
    preparedStatement.setString(2, jobStatus.statusName)
    if (jobStatus is JobStatus.Failed) {
      preparedStatement.setString(3, jobStatus.errorMessage)
    } else {
      preparedStatement.setNull(3, Types.VARCHAR)
    }
    if (jobStatus is JobStatus.Skipped) {
      preparedStatement.setString(4, jobStatus.reason)
    } else {
      preparedStatement.setNull(4, Types.VARCHAR)
    }
    preparedStatement.setTimestamp(5, Timestamp.valueOf(jobStatus.ts))

    preparedStatement.execute()
  }
}

private suspend fun deleteBeforeTsOnHistoricalTable(
  log: Log,
  con: Connection,
  ts: LocalDateTime,
) {
  //language=SQLite
  val sql = """
    |-- noinspection SqlResolve @ table/"ketl_job_status"
    |DELETE FROM ketl_job_status
    |WHERE ts < ?
  """.trimMargin()

  log.debug(sql)

  con.prepareStatement(sql).use { preparedStatement ->
    preparedStatement.queryTimeout = 60

    preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

    preparedStatement.execute()
  }
}

private suspend fun deleteBeforeTsOnSnapshotTable(
  log: Log,
  con: Connection,
  ts: LocalDateTime,
) {
  //language=SQLite
  val sql = """
      |-- noinspection SqlResolve @ table/"ketl_job_status_snapshot"
      |DELETE FROM ketl_job_status_snapshot
      |WHERE ts < ?
    """.trimMargin()

  log.debug(sql)

  con.prepareStatement(sql).use { preparedStatement ->
    preparedStatement.queryTimeout = 60

    preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

    preparedStatement.execute()
  }
}

internal fun getJobStatusFromResultSet(resultSet: ResultSet): JobStatus {
  val jobName = resultSet.getString("job_name")
  val ts = resultSet.getTimestamp("ts").toLocalDateTime()
  val statusName = resultSet.getString("status")

  return when (statusName) {
    "cancelled" -> JobStatus.Cancelled(jobName = jobName, ts = ts)
    "initial" -> JobStatus.Initial(jobName = jobName, ts = ts)
    "running" -> JobStatus.Running(jobName = jobName, ts = ts)
    "skipped" -> {
      val skipReason = resultSet.getString("skip_reason")
      JobStatus.Skipped(jobName = jobName, ts = ts, reason = skipReason)
    }
    "successful" -> JobStatus.Success(jobName = jobName, ts = ts)
    "failed" -> {
      val errorMessage = resultSet.getString("error_message")
      JobStatus.Failed(jobName = jobName, ts = ts, errorMessage = errorMessage)
    }
    else -> throw KETLErrror.UnrecognizedStatusName(statusName)
  }
}
