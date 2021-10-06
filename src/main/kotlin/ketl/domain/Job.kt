package ketl.domain

import kotlinx.coroutines.coroutineScope
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
data class Job<Ctx : JobContext>(
  val name: String,
  val schedule: List<Schedule>,
  val timeout: Duration = Duration.days(9999),
  val retries: Int = 0,
  val dependencies: Set<String> = setOf(),
  val ctx: Ctx,
  val onRun: suspend Ctx.() -> Status,
) {

  init {
    require(name.isNotBlank()) { "name cannot be blank." }
    require(schedule.isNotEmpty()) { "At least 1 schedule must be provided." }
    require(retries >= 0) { "Timeout seconds must be >= 0." }
  }

  fun isReady(
    refTime: LocalDateTime,
    lastRun: LocalDateTime?,
  ): Boolean =
    schedule.any { schedule ->
      schedule.ready(
        refTime = refTime,
        lastRun = lastRun,
      )
    }

  @ExperimentalTime
  suspend fun run() = coroutineScope { with(ctx) { onRun() } }
}
