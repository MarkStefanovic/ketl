package ketl.domain

import java.time.LocalDateTime

interface DbLogRepo {
  fun add(message: LogMessage): SQLResult

  fun createTable(): SQLResult

  fun deleteBefore(ts: LocalDateTime): SQLResult
}
