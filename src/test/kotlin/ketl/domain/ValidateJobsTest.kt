package ketl.domain

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class DummyJob(
  override val name: String,
  override val schedule: Schedule,
  override val timeout: Duration,
  override val maxRetries: Int,
  override val dependencies: Set<String>,
) : KETLJob {
  override suspend fun run(log: Log): Status {
    TODO("Not yet implemented")
  }
}

@ExperimentalTime
class ValidateJobsTest {
  @Test
  fun given_duplicate_job_names_and_missing_dependencies() {
    val jobs = setOf(
      DummyJob(
        name = "test_job_1",
        schedule = every("every_day", frequency = Duration.days(1)),
        timeout = Duration.minutes(15),
        maxRetries = 1,
        dependencies = setOf(),
      ),
      DummyJob(
        name = "test_job_2",
        schedule = every("every_day", frequency = Duration.days(1)),
        timeout = Duration.minutes(15),
        maxRetries = 1,
        dependencies = setOf("test_job_1"),
      ),
      DummyJob(
        name = "test_job_1",
        schedule = every("every_day", frequency = Duration.days(2)),
        timeout = Duration.minutes(15),
        maxRetries = 1,
        dependencies = setOf("test_job_4"),
      ),
      DummyJob(
        name = "test_job_2",
        schedule = every("every_day", frequency = Duration.days(3)),
        timeout = Duration.minutes(15),
        maxRetries = 1,
        dependencies = setOf("test_job_1"),
      ),
      DummyJob(
        name = "test_job_3",
        schedule = every("every_day", frequency = Duration.days(1)),
        timeout = Duration.minutes(15),
        maxRetries = 1,
        dependencies = setOf(),
      ),
    )

    val validationResult = validateJobs(jobs)

    assertEquals(
      expected = true,
      actual = validationResult.hasErrors,
    )

    assertEquals(
      expected = false,
      actual = validationResult.isOk,
    )

    assertEquals(
      expected = setOf("test_job_3"),
      actual = validationResult.validJobNames,
    )

    assertEquals(
      expected = setOf("test_job_1", "test_job_2"),
      actual = validationResult.invalidJobNames,
    )

    assertEquals(
      expected = """
        |Job Validation Errors:
        |  test_job_1:
        |    2 jobs are named test_job_1.
        |    The following jobs are listed as dependencies, but they're not scheduled: test_job_4.
        |  test_job_2:
        |    2 jobs are named test_job_2.
      """.trimMargin(),
      actual = validationResult.errorMessage,
    )

    assertEquals(
      expected = """
      |JobValidationResult [
      |  test_job_1:
      |    2 jobs are named test_job_1.
      |    The following jobs are listed as dependencies, but they're not scheduled: test_job_4.
      |  test_job_2:
      |    2 jobs are named test_job_2.
      |]
      """.trimMargin(),
      actual = validationResult.toString(),
    )
  }
}
