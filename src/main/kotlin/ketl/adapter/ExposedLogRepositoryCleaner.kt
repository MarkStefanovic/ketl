package ketl.adapter

import ketl.domain.LogMessages
import ketl.domain.LogRepository
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
suspend fun exposedLogRepositoryCleaner(
  db: Db,
  log: LogMessages,
  repository: LogRepository,
  timeBetweenCleanup: Duration = Duration.minutes(30),
  durationToKeep: Duration = Duration.days(3),
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  launch(dispatcher) {
    while (true) {
      log.info("Cleaning up the log...")
      val cutoff = LocalDateTime.now().minusSeconds(durationToKeep.inWholeSeconds)
      db.exec { repository.deleteBefore(cutoff) }
      log.info("Finished cleaning up the log.")
      delay(timeBetweenCleanup.inWholeMilliseconds)
    }
  }
}
