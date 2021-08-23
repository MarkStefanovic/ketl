package main.kotlin.domain

import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect

suspend fun consoleLogger(
  messages: SharedFlow<LogMessage>,
  minLogLevel: LogLevel,
  format: (LogMessage) -> String = ::defaultLogFormat,
) {
  messages.collect { msg ->
    if (msg.level.gte(minLogLevel)) {
      println(format(msg))
    }
  }
}
