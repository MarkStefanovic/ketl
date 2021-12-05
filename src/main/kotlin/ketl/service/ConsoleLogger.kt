package ketl.service

import ketl.domain.DefaultLog
import ketl.domain.Log
import ketl.domain.LogLevel
import ketl.domain.LogMessage
import ketl.domain.defaultLogFormat
import ketl.domain.gte
import kotlinx.coroutines.flow.collect

suspend fun consoleLogger(
  log: Log = DefaultLog,
  minLogLevel: LogLevel = LogLevel.Info,
  format: (LogMessage) -> String = ::defaultLogFormat,
) {
  log.stream.collect { logMessage ->
    if (logMessage.level gte minLogLevel) {
      println(format(logMessage))
    }
  }
}
