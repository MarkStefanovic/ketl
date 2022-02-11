package ketl.service

import ketl.domain.LogLevel
import ketl.domain.LogMessage
import ketl.domain.defaultLogFormat
import ketl.domain.gte
import kotlinx.coroutines.flow.SharedFlow

suspend fun consoleLogger(
  logMessages: SharedFlow<LogMessage>,
  minLogLevel: LogLevel = LogLevel.Info,
  format: (LogMessage) -> String = ::defaultLogFormat,
) {
  logMessages.collect { logMessage ->
    if (logMessage.level gte minLogLevel) {
      println(format(logMessage))
    }
  }
}
