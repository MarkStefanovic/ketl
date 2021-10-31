package ketl.domain

import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
data class JobSpec(
  val jobName: String,
  val scheduleName: String,
  val timeout: Duration,
  val retries: Int,
  val enabled: Boolean,
  val dependencies: Set<String>,
) {
  override fun toString() = """
    |JobSpec [
    |  jobName: $jobName
    |  scheduleName: $scheduleName
    |  timeout: $timeout
    |  retries: $retries
    |  enabled: $enabled
    |  dependencies: $dependencies
    |]
  """.trimIndent()
}
