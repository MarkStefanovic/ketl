package ketl.adapter

import ketl.domain.JobResult
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import testutil.pgDataSource
import java.time.LocalDateTime
import kotlin.test.assertEquals

class PgJobResultsRepoTest {
  @Test
  fun round_trip_test() {
    val ds = pgDataSource()

    ds.connection.use { connection ->
      connection.createStatement().use { statement ->
        statement.execute("DROP TABLE IF EXISTS ketl.job_result")
      }

      val repo = PgJobResultsRepo(schema = "ketl", showSQL = true, ds = ds)

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
        repo.add(jobResult)

        val results = repo.getLatestResults()

        assertEquals(expected = results.count(), actual = 1)

        repo.add(jobResult2)

        val resultsAfterSecondAdd = repo.getLatestResults()

        assertEquals(expected = resultsAfterSecondAdd.count(), actual = 2)

        repo.deleteBefore(LocalDateTime.of(2011, 1, 1, 1, 1, 1))

        val resultsAfterDelete = repo.getLatestResults()

        assertEquals(expected = resultsAfterDelete.count(), actual = 1)
      }
    }
  }
}