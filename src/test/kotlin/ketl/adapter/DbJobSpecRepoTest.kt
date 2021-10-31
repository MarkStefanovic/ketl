package ketl.adapter

import ketl.domain.JobSpec
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.StdOutSqlLogger
import org.jetbrains.exposed.sql.addLogger
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

private fun testDb() =
  Database.connect("jdbc:sqlite:file:test?mode=memory&cache=shared", "org.sqlite.JDBC")

@ExperimentalTime
class DbJobSpecRepoTest {
  @Test
  fun round_trip() {
    val jobSpec = JobSpec(
      jobName = "test_job",
      scheduleName = "test_job schedule",
      timeout = Duration.minutes(10),
      retries = 1,
      enabled = true,
      dependencies = setOf(),
    )

    val repo = DbJobSpecRepo()

    transaction(db = testDb()) {
      addLogger(StdOutSqlLogger)

      SchemaUtils.create(JobDepTable, JobSpecTable)

      repo.add(jobSpec)

      val actual = repo.get("test_job")

      assertEquals(expected = jobSpec, actual = actual)
    }
  }

  @Test
  fun upsert_where_exists() {
    val jobSpec = JobSpec(
      jobName = "test_job",
      scheduleName = "test_job schedule",
      timeout = Duration.minutes(10),
      retries = 1,
      enabled = true,
      dependencies = setOf(),
    )

    val repo = DbJobSpecRepo()

    transaction(db = testDb()) {
      addLogger(StdOutSqlLogger)

      SchemaUtils.create(JobDepTable, JobSpecTable)

      repo.add(jobSpec)

      repo.upsert(jobSpec)

      val allJobs = repo.getActiveJobs()

      assertEquals(expected = 1, actual = allJobs.count())

      assertEquals(expected = jobSpec, actual = allJobs.first())
    }
  }
}
