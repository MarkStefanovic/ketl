package main.kotlin.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch

class ConsoleLogger(
  private val scope: CoroutineScope,
  private val messages: SharedFlow<LogMessage>,
  val minLogLevel: LogLevel,
  val format: (LogMessage) -> String = ::defaultLogFormat,
) {
  fun start() {
    scope.launch {
      messages.collect { msg ->
        if (msg.level.gte(minLogLevel)) {
          println(format(msg))
        }
      }
    }
  }
}
