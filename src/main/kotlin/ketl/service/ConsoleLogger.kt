package ketl.service

import ketl.domain.Log
import ketl.domain.LogLevel
import ketl.domain.LogMessage
import ketl.domain.defaultLogFormat
import ketl.domain.gte
import kotlinx.coroutines.flow.collect

suspend fun consoleLogger(
  log: Log,
  minLogLevel: LogLevel,
  format: (LogMessage) -> String = ::defaultLogFormat,
) {
  log.stream.collect { logMessage ->
    if (logMessage.level gte minLogLevel) {
      println(format(logMessage))
    }
  }
}
