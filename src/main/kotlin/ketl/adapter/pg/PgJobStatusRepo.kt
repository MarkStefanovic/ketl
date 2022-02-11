package ketl.adapter.pg

import ketl.domain.DbJobStatusRepo
import ketl.domain.JobStatus
import ketl.domain.KETLErrror
import ketl.domain.NamedLog
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import javax.sql.DataSource

class PgJobStatusRepo(
  val schema: String,
  private val ds: DataSource,
  private val log: NamedLog = NamedLog("PgJobStatusRepo"),
) : DbJobStatusRepo {
  override suspend fun add(jobStatus: JobStatus) {
    ds.connection.use { connection ->
      addToHistoricalTable(con = connection, schema = schema, jobStatus = jobStatus, log = log)
      addToSnapshotTable(con = connection, schema = schema, jobStatus = jobStatus, log = log)
    }
  }

  override suspend fun cancelRunningJobs() {
    //language=PostgreSQL
    val sql = """
      |UPDATE $schema.job_status_snapshot
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
    //language=PostgreSQL
    val sql = """
      |SELECT
      |  job_name
      |, status
      |, error_message
      |, skip_reason
      |, ts
      |FROM $schema.job_status_snapshot
    """.trimMargin()

    val jobStatuses = mutableListOf<JobStatus>()
    ds.connection.use { connection ->
      connection.createStatement().use { statement ->
        statement.queryTimeout = 60

        statement.executeQuery(sql).use { resultSet ->
          val jobStatus = getJobStatusFromResultSet(resultSet)
          jobStatuses.add(jobStatus)
        }
      }
    }
    return jobStatuses.toSet()
  }

  override suspend fun createTables() {
    //language=PostgreSQL
    val createHistTableSQL = """
      |CREATE TABLE IF NOT EXISTS $schema.job_status (
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

    //language=PostgreSQL
    val createSnapshotTableSQL = """
      |CREATE TABLE IF NOT EXISTS $schema.job_status_snapshot (
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
    //language=PostgreSQL
    val sql = """
      |DELETE FROM $schema.job_status
      |WHERE ts < ?
    """.trimMargin()

    log.debug(sql)

    ds.connection.use { connection ->
      deleteBeforeTsOnHistoricalTable(log = log, con = connection, schema = schema, ts = ts)
      deleteBeforeTsOnSnapshotTable(log = log, con = connection, schema = schema, ts = ts)
    }
  }
}

private suspend fun addToHistoricalTable(
  con: Connection,
  schema: String,
  jobStatus: JobStatus,
  log: NamedLog,
) {
  //language=PostgreSQL
  val sql = """
    |INSERT INTO $schema.job_status (
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
  schema: String,
  jobStatus: JobStatus,
  log: NamedLog,
) {
  //language=PostgreSQL
  val sql = """
    |INSERT INTO $schema.job_status_snapshot (
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
    |ON CONFLICT (job_name) 
    |DO UPDATE SET
    |  status = EXCLUDED.status
    |, error_message = EXCLUDED.error_message
    |, skip_reason = EXCLUDED.skip_reason
    |, ts = EXCLUDED.ts
    |WHERE
    |(
    |  $schema.job_status_snapshot.status
    |, $schema.job_status_snapshot.error_message
    |, $schema.job_status_snapshot.skip_reason
    |, $schema.job_status_snapshot.ts  
    |) IS DISTINCT FROM (
    |  EXCLUDED.status
    |, EXCLUDED.error_message
    |, EXCLUDED.skip_reason
    |, EXCLUDED.ts
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
  log: NamedLog,
  con: Connection,
  schema: String,
  ts: LocalDateTime,
) {
  //language=PostgreSQL
  val sql = """
    |DELETE FROM $schema.job_status
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
  log: NamedLog,
  con: Connection,
  schema: String,
  ts: LocalDateTime,
) {
  //language=PostgreSQL
  val sql = """
    |DELETE FROM $schema.job_status_snapshot
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

  return when (val statusName = resultSet.getString("status")) {
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
