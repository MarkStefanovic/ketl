package ketl.adapter

import ketl.domain.LogMessage
import ketl.domain.LogRepository
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch

@DelicateCoroutinesApi
suspend fun sqlLogger(
  db: Db,
  repository: LogRepository,
  messages: SharedFlow<LogMessage>,
  dispatcher: CoroutineDispatcher = Dispatchers.Default
) = coroutineScope {
  launch(dispatcher) {
    messages.collect { msg -> db.exec { repository.add(msg) } }
  }
}
