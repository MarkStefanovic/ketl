package ketl.adapter

import ketl.domain.LogMessage
import ketl.domain.LogRepository
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect

suspend fun sqlLogger(
  db: Db,
  repository: LogRepository,
  messages: SharedFlow<LogMessage>,
) {
  messages.collect { msg -> db.exec { repository.add(msg) } }
}
