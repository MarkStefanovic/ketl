package ketl.domain

import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
interface KETLJob {
  val name: String
  val schedule: Schedule
  val timeout: Duration
  val maxRetries: Int
  val dependencies: Set<String>

  suspend fun run(log: Log): Status

  fun failed(errorMessage: String): Status =
    Status.Failure(errorMessage)

  fun skipped(reason: String): Status =
    Status.Skipped(reason)

  fun success(): Status =
    Status.Success
}
