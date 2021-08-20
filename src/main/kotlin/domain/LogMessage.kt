package main.kotlin.domain

data class LogMessage(
  val loggerName: String,
  val level: LogLevel,
  val message: String,
)
