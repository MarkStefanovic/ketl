@file:Suppress("SqlResolve")

package ketl.adapter.sqlite

import ketl.domain.DbJobStatusRepo
import ketl.domain.JobStatus
import ketl.domain.KETLError
import ketl.domain.SQLResult
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime
import javax.sql.DataSource

class SQLiteJobStatusRepo(
  private val ds: DataSource,
) : DbJobStatusRepo {
  override fun add(jobStatus: JobStatus): SQLResult =
    ds.connection.use { connection ->
      addToHistoricalTable(con = connection, jobStatus = jobStatus) +
        addToSnapshotTable(con = connection, jobStatus = jobStatus)
    }

  override fun cancelRunningJobs(): SQLResult {
    //language=SQLite
    val sql = """
      |UPDATE ketl_job_status_snapshot
      |SET status = 'cancelled'
      |WHERE status = 'running'
    """.trimMargin()

    return try {
      ds.connection.use { connection ->
        connection.createStatement().use { statement ->
          statement.queryTimeout = 60

          statement.execute(sql)
        }
      }
      SQLResult.Success(sql = sql, parameters = null)
    } catch (e: Exception) {
      SQLResult.Error(sql = sql, parameters = null, error = e)
    }
  }

  override fun currentStatus(): Pair<SQLResult, Set<JobStatus>> {
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

    return try {
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
      SQLResult.Success(sql = sql, parameters = null) to jobStatuses.toSet()
    } catch (e: Exception) {
      SQLResult.Error(sql = sql, parameters = null, error = e) to emptySet()
    }
  }

  override fun createTables(): SQLResult {
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

    val fullSQL = "$createHistTableSQL;\n$createSnapshotTableSQL"

    return try {
      ds.connection.use { connection ->
        connection.createStatement().use { statement ->
          statement.queryTimeout = 60

          statement.execute(createHistTableSQL)
          statement.execute(createSnapshotTableSQL)
        }
      }
      SQLResult.Success(sql = fullSQL, parameters = null)
    } catch (e: Exception) {
      SQLResult.Error(sql = fullSQL, parameters = null, error = e)
    }
  }

  override fun deleteBefore(ts: LocalDateTime): SQLResult {
    //language=SQLite
    val sql = """
      |-- noinspection SqlResolve @ table/"ketl_job_status"
      |DELETE FROM ketl_job_status
      |WHERE ts < ?
    """.trimMargin()

    return try {
      ds.connection.use { connection ->
        deleteBeforeTsOnHistoricalTable(con = connection, ts = ts) +
          deleteBeforeTsOnSnapshotTable(con = connection, ts = ts)
      }
    } catch (e: Exception) {
      SQLResult.Error(sql = sql, parameters = mapOf("ts" to ts), error = e)
    }
  }
}

private fun addToHistoricalTable(
  con: Connection,
  jobStatus: JobStatus,
): SQLResult {
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

  return try {
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
    SQLResult.Success(sql = sql, parameters = mapOf("jobStatus" to jobStatus))
  } catch (e: Exception) {
    SQLResult.Error(sql = sql, parameters = mapOf("jobStatus" to jobStatus), error = e)
  }
}

private fun addToSnapshotTable(
  con: Connection,
  jobStatus: JobStatus,
): SQLResult {
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

  return try {
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
    SQLResult.Success(sql = sql, parameters = mapOf("jobStatus" to jobStatus))
  } catch (e: Exception) {
    SQLResult.Error(sql = sql, parameters = mapOf("jobStatus" to jobStatus), error = e)
  }
}

private fun deleteBeforeTsOnHistoricalTable(
  con: Connection,
  ts: LocalDateTime,
): SQLResult {
  //language=SQLite
  val sql = """
    |-- noinspection SqlResolve @ table/"ketl_job_status"
    |DELETE FROM ketl_job_status
    |WHERE ts < ?
  """.trimMargin()

  return try {
    con.prepareStatement(sql).use { preparedStatement ->
      preparedStatement.queryTimeout = 60

      preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

      preparedStatement.execute()
    }
    SQLResult.Success(sql = sql, parameters = mapOf("ts" to ts))
  } catch (e: Exception) {
    SQLResult.Error(sql = sql, parameters = mapOf("ts" to ts), error = e)
  }
}

private fun deleteBeforeTsOnSnapshotTable(
  con: Connection,
  ts: LocalDateTime,
): SQLResult {
  //language=SQLite
  val sql = """
      |-- noinspection SqlResolve @ table/"ketl_job_status_snapshot"
      |DELETE FROM ketl_job_status_snapshot
      |WHERE ts < ?
    """.trimMargin()

  return try {
    con.prepareStatement(sql).use { preparedStatement ->
      preparedStatement.queryTimeout = 60

      preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

      preparedStatement.execute()
    }
    SQLResult.Success(sql = sql, parameters = mapOf("ts" to ts))
  } catch (e: Exception) {
    SQLResult.Error(sql = sql, parameters = mapOf("ts" to ts), error = e)
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
    else -> throw KETLError.UnrecognizedStatusName(statusName)
  }
}
