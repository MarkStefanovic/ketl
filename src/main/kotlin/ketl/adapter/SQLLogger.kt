package ketl.adapter

import ketl.domain.LogMessage
import ketl.domain.LogRepository
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
suspend fun sqlLogger(
  db: Db,
  repository: LogRepository,
  messages: SharedFlow<LogMessage>,
  timeout: Duration = Duration.seconds(10),
) {
  messages.collect { msg ->
    try {
      withTimeout(timeout) {
        db.exec { repository.add(msg) }
      }
    } catch (e: Exception) {
      if (e is CancellationException) {
        println("sqlLogger cancelled.")
      } else {
        e.printStackTrace()
      }
      throw e
    }
  }
}
