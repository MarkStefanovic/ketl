package ketl.domain

import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
data class JobSpec(
  val jobName: String,
  val scheduleName: String,
  val timeout: Duration = Duration.days(9999),
  val retries: Int = 0,
  val dependencies: Set<String> = setOf(),
) {
  override fun toString() = """
    |JobSpec [
    |  jobName: $jobName
    |  scheduleName: $scheduleName
    |  timeout: $timeout
    |  retries: $retries
    |  dependencies: $dependencies
    |]
  """.trimIndent()
}
