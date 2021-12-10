package ketl.domain

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

fun defaultLogFormat(msg: LogMessage): String =
  "${LocalDateTime.now().format(DateTimeFormatter.ofPattern("M/d @ hh:mm:ss a"))} - ${msg.loggerName} " +
    "[${msg.level.toString().uppercase()}]: ${msg.message}"
