package ketl.adapter.pg

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

class PgJobStatusRepo(
  val schema: String,
  private val ds: DataSource,
) : DbJobStatusRepo {
  override fun add(jobStatus: JobStatus): SQLResult =
    ds.connection.use { connection ->
      addToHistoricalTable(con = connection, schema = schema, jobStatus = jobStatus) +
        addToSnapshotTable(con = connection, schema = schema, jobStatus = jobStatus)
    }

  override fun cancelRunningJobs(): SQLResult {
    //language=PostgreSQL
    val sql = """
      |UPDATE $schema.job_status_snapshot
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

    return try {
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
      SQLResult.Success(sql = sql, parameters = null) to jobStatuses.toSet()
    } catch (e: Exception) {
      SQLResult.Error(sql = sql, parameters = null, error = e) to emptySet()
    }
  }

  override fun createTables(): SQLResult {
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

    val fullSQL = "$createHistTableSQL\n$createSnapshotTableSQL"

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
    //language=PostgreSQL
    val sql = """
      |DELETE FROM $schema.job_status
      |WHERE ts < ?
    """.trimMargin()

    return try {
      ds.connection.use { connection ->
        deleteBeforeTsOnHistoricalTable(con = connection, schema = schema, ts = ts) +
          deleteBeforeTsOnSnapshotTable(con = connection, schema = schema, ts = ts)
      }
    } catch (e: Exception) {
      SQLResult.Error(sql = sql, parameters = mapOf("ts" to ts), error = e)
    }
  }
}

private fun addToHistoricalTable(
  con: Connection,
  schema: String,
  jobStatus: JobStatus,
): SQLResult {
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
    SQLResult.Success(
      sql = sql,
      parameters = mapOf("schema" to schema, "jobStatus" to jobStatus),
    )
  } catch (e: Exception) {
    SQLResult.Error(
      sql = sql,
      parameters = mapOf("schema" to schema, "jobStatus" to jobStatus),
      error = e,
    )
  }
}

private fun addToSnapshotTable(
  con: Connection,
  schema: String,
  jobStatus: JobStatus,
): SQLResult {
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
    SQLResult.Success(
      sql = sql,
      parameters = mapOf("schema" to schema, "jobStatus" to jobStatus),
    )
  } catch (e: Exception) {
    SQLResult.Error(
      sql = sql,
      parameters = mapOf("schema" to schema, "jobStatus" to jobStatus),
      error = e,
    )
  }
}

private fun deleteBeforeTsOnHistoricalTable(
  con: Connection,
  schema: String,
  ts: LocalDateTime,
): SQLResult {
  //language=PostgreSQL
  val sql = """
    |DELETE FROM $schema.job_status
    |WHERE ts < ?
  """.trimMargin()

  return try {
    con.prepareStatement(sql).use { preparedStatement ->
      preparedStatement.queryTimeout = 60

      preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

      preparedStatement.execute()
    }
    SQLResult.Success(
      sql = sql,
      parameters = mapOf("schema" to schema, "ts" to ts)
    )
  } catch (e: Exception) {
    SQLResult.Error(
      sql = sql,
      parameters = mapOf("schema" to schema, "ts" to ts),
      error = e,
    )
  }
}

private fun deleteBeforeTsOnSnapshotTable(
  con: Connection,
  schema: String,
  ts: LocalDateTime,
): SQLResult {
  //language=PostgreSQL
  val sql = """
    |DELETE FROM $schema.job_status_snapshot
    |WHERE ts < ?
  """.trimMargin()

  return try {
    con.prepareStatement(sql).use { preparedStatement ->
      preparedStatement.queryTimeout = 60

      preparedStatement.setTimestamp(1, Timestamp.valueOf(ts))

      preparedStatement.execute()
    }
    SQLResult.Success(
      sql = sql,
      parameters = mapOf("schema" to schema, "ts" to ts)
    )
  } catch (e: Exception) {
    SQLResult.Error(
      sql = sql,
      parameters = mapOf("schema" to schema, "ts" to ts),
      error = e,
    )
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
    else -> throw KETLError.UnrecognizedStatusName(statusName)
  }
}
