package ketl.domain

import java.time.LocalDateTime

interface LogRepository {
  fun add(message: LogMessage)

  fun deleteBefore(ts: LocalDateTime)
}
