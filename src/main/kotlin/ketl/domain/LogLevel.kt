package ketl.domain

val logLevelNumericValues =
  mapOf(
    LogLevel.Debug to 1,
    LogLevel.Info to 2,
    LogLevel.Warning to 3,
    LogLevel.Error to 4,
  )

enum class LogLevel {
  Debug,
  Error,
  Info,
  Warning;
}

infix fun LogLevel.gte(other: LogLevel) = logLevelNumericValues[this]!! >= logLevelNumericValues[other]!!
