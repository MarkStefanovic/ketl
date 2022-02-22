package ketl.service

import ketl.domain.LogLevel
import ketl.domain.LogMessage
import ketl.domain.defaultLogFormat
import ketl.domain.gte
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch

fun CoroutineScope.consoleLogger(
  logMessages: SharedFlow<LogMessage>,
  minLogLevel: LogLevel = LogLevel.Info,
  format: (LogMessage) -> String = ::defaultLogFormat,
) = launch {
  logMessages.collect { logMessage ->
    if (logMessage.level gte minLogLevel) {
      println(format(logMessage))
    }
  }
}
