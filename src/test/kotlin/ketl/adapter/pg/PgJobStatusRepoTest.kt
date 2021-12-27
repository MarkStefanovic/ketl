package ketl.adapter.pg

import ketl.domain.JobStatus
import ketl.domain.LogLevel
import ketl.service.consoleLogger
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import testutil.pgDataSource
import java.sql.Connection
import java.time.LocalDateTime
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private fun getJobStatuses(ds: DataSource, tableName: String): Set<JobStatus> {
  //language=PostgreSQL
  val sql = """
    |-- noinspection SqlResolve @ any/"ketl"
    |SELECT
    |  job_name
    |, status
    |, error_message
    |, skip_reason
    |, ts
    |FROM ketl.$tableName
  """.trimMargin()

  val jobStatuses = mutableListOf<JobStatus>()
  ds.connection.use { connection ->
    connection.createStatement().use { statement ->
      statement.executeQuery(sql).use { rs ->
        while (rs.next()) {
          val jobStatus = getJobStatusFromResultSet(rs)
          jobStatuses.add(jobStatus)
        }
      }
    }
  }
  return jobStatuses.toSet()
}

private fun getJobStatusSnapshotEntries(ds: DataSource): Set<JobStatus> =
  getJobStatuses(ds = ds, tableName = "job_status_snapshot")

private fun getJobStatusHistoricalEntries(ds: DataSource): Set<JobStatus> =
  getJobStatuses(ds = ds, tableName = "job_status")

private fun deleteTables(con: Connection) {
  con.createStatement().use { statement ->
    //language=PostgreSQL
    statement.executeUpdate(
      """
            -- noinspection SqlResolve @ any/"ketl"
            DROP TABLE IF EXISTS ketl.job_status
      """.trimIndent()
    )
    //language=PostgreSQL
    statement.executeUpdate(
      """
            -- noinspection SqlResolve @ any/"ketl"
            DROP TABLE IF EXISTS ketl.job_status_snapshot
      """.trimIndent()
    )
  }
}

@DelicateCoroutinesApi
class PgJobStatusRepoTest {
  @Test
  fun add_happy_path() = runBlocking {
    GlobalScope.launch {
      consoleLogger(minLogLevel = LogLevel.Debug)
    }

    pgDataSource().let { ds ->
      val repo = PgJobStatusRepo(ds = ds, schema = "ketl")

      ds.connection.use { connection ->
        deleteTables(connection)

        repo.createTables()

        val jobStatus = JobStatus.Success(jobName = "test_job", ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5))

        repo.add(jobStatus)

        val jobStatusSnapshots = getJobStatusSnapshotEntries(ds)

        assertEquals(expected = setOf(jobStatus), actual = jobStatusSnapshots)

        val jobStatusHistory = getJobStatusHistoricalEntries(ds)

        assertEquals(expected = setOf(jobStatus), actual = jobStatusHistory)
      }
    }
  }

  @Test
  fun cancelRunningJobs_happy_path() = runBlocking {
    GlobalScope.launch {
      consoleLogger(minLogLevel = LogLevel.Debug)
    }

    pgDataSource().let { ds ->
      val repo = PgJobStatusRepo(ds = ds, schema = "ketl")

      ds.connection.use { connection ->
        deleteTables(connection)
        repo.createTables()

        val jobStatus1 = JobStatus.Success(jobName = "test_job_1", ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5))
        val jobStatus2 = JobStatus.Running(jobName = "test_job_2", ts = LocalDateTime.of(2011, 1, 2, 3, 4, 5))
        val jobStatus3 = JobStatus.Failed(jobName = "test_job_3", ts = LocalDateTime.of(2010, 2, 2, 3, 4, 5), errorMessage = "Whoops!")

        repo.add(jobStatus1)
        repo.add(jobStatus2)
        repo.add(jobStatus3)

        val initialJobStatusSnapshots = getJobStatusSnapshotEntries(ds)

        assertEquals(expected = 3, actual = initialJobStatusSnapshots.count())

        repo.cancelRunningJobs()

        val snapshotEntries = getJobStatusSnapshotEntries(ds)

        assertTrue(snapshotEntries.all { it.statusName != "running" })

        assertTrue(snapshotEntries.first { it.jobName == "test_job_2" }.statusName == "cancelled")
      }
    }
  }

  @Test
  fun deleteBefore_happy_path() = runBlocking {
    GlobalScope.launch {
      consoleLogger(minLogLevel = LogLevel.Debug)
    }

    pgDataSource().let { ds ->
      val repo = PgJobStatusRepo(ds = ds, schema = "ketl")

      ds.connection.use { connection ->
        deleteTables(connection)

        repo.createTables()

        val jobStatus1 = JobStatus.Success(jobName = "test_job_1", ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5))
        val jobStatus2 = JobStatus.Success(jobName = "test_job_2", ts = LocalDateTime.of(2011, 1, 2, 3, 4, 5))
        val jobStatus3 = JobStatus.Success(jobName = "test_job_3", ts = LocalDateTime.of(2010, 2, 2, 3, 4, 5))

        repo.add(jobStatus1)
        repo.add(jobStatus2)
        repo.add(jobStatus3)

        val initialJobStatusSnapshots = getJobStatusSnapshotEntries(ds)

        assertEquals(expected = 3, actual = initialJobStatusSnapshots.count())

        repo.deleteBefore(LocalDateTime.of(2011, 1, 1, 1, 1, 1))

        val postDeleteJobStatusSnapshots = getJobStatusSnapshotEntries(ds)

        assertEquals(expected = setOf(jobStatus2), actual = postDeleteJobStatusSnapshots)

        val postDeleteJobStatusHistory = getJobStatusHistoricalEntries(ds)

        assertEquals(expected = setOf(jobStatus2), actual = postDeleteJobStatusHistory)
      }
    }
  }
}
