package ketl.domain

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch

@DelicateCoroutinesApi
suspend fun consoleLogger(
  messages: SharedFlow<LogMessage>,
  minLogLevel: LogLevel,
  format: (LogMessage) -> String = ::defaultLogFormat,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  launch(dispatcher) {
    messages.collect { msg ->
      if (msg.level.gte(minLogLevel)) {
        println(format(msg))
      }
    }
  }
}
