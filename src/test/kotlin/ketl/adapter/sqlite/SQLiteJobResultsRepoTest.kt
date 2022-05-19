@file:Suppress("SqlResolve", "SqlNoDataSourceInspection")

package ketl.adapter.sqlite

import ketl.domain.JobResult
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.NamedLog
import ketl.service.consoleLogger
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.sqlite.SQLiteDataSource
import java.time.LocalDateTime
import kotlin.test.assertEquals

private fun testDataSource() = SQLiteDataSource().apply {
  url = "jdbc:sqlite:file:test?mode=memory&cache=shared"
}

@DelicateCoroutinesApi
class SQLiteJobResultsRepoTest {
  @Test
  fun round_trip_test() {
    val ds = testDataSource()

    val logMessages = LogMessages()

    val log = NamedLog(
      name = "test_log",
      minLogLevel = LogLevel.Info,
      logMessages = logMessages,
    )

    GlobalScope.launch {
      consoleLogger(minLogLevel = LogLevel.Debug, logMessages = logMessages.stream)
    }

    ds.connection.use { connection ->
      connection.createStatement().use { statement ->
        //language=SQLite
        statement.execute("DROP TABLE IF EXISTS ketl_job_result")
        //language=SQLite
        statement.execute("DROP TABLE IF EXISTS ketl_job_result_snapshot")
      }

      val repo = SQLiteJobResultsRepo(ds = ds, log = log)

      val jobResult = JobResult.Successful(
        jobName = "test_job",
        start = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
        end = LocalDateTime.of(2010, 1, 2, 3, 5, 6),
      )

      val jobResult2 = JobResult.Successful(
        jobName = "test_job_2",
        start = LocalDateTime.of(2011, 1, 2, 3, 4, 5),
        end = LocalDateTime.of(2011, 1, 2, 3, 5, 6),
      )

      runBlocking {
        repo.createTables()

        repo.add(jobResult)

        val (_, results) = repo.getLatestResults()

        assertEquals(expected = 1, actual = results.count())

        repo.add(jobResult2)

        val (_, resultsAfterSecondAdd) = repo.getLatestResults()

        assertEquals(expected = 2, actual = resultsAfterSecondAdd.count())

        repo.deleteBefore(LocalDateTime.of(2011, 1, 1, 1, 1, 1))

        val (_, resultsAfterDelete) = repo.getLatestResults()

        assertEquals(expected = 1, actual = resultsAfterDelete.count())
      }
    }
  }
}
