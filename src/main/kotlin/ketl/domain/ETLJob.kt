package ketl.domain

import kotlinx.coroutines.coroutineScope
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
data class ETLJob<Ctx : JobContext>(
  val name: String,
  val schedule: Schedule,
  val timeout: Duration = Duration.days(9999),
  val retries: Int = 0,
  val dependencies: Set<String> = setOf(),
  val ctx: Ctx,
  val onRun: suspend Ctx.(log: ETLLog) -> Status,
) {
  init {
    require(name.isNotBlank()) { "name cannot be blank." }
    require(schedule.parts.isNotEmpty()) { "At least 1 schedule must be provided." }
    require(retries >= 0) { "Timeout seconds must be >= 0." }
  }

  @ExperimentalTime suspend fun run(log: ETLLog) = coroutineScope { with(ctx) { onRun(log) } }

  override fun toString(): String {
    val dependenciesCSV = if (dependencies.isEmpty()) {
      "[]"
    } else {
      dependencies.toSortedSet().joinToString(", ") { "[\"$it\"]" }
    }
    return """
      |ETLJob [
      |  name: $name
      |  schedule: ${schedule.displayName}
      |  timeout: $timeout
      |  retries: $retries
      |  dependencies: $dependenciesCSV
      |]
    """.trimMargin()
  }
}
