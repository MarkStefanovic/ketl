package ketl.domain

import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import kotlin.test.assertEquals

private const val expectedToStringResult = """LogMessage [
  loggerName: test_logger
  level: Info
  message: This is a test
  ts: 2010-01-02T03:04:05
]"""

class LogMessageTest {
  @Test
  fun to_string_happy_path() {
    val msg = LogMessage(
      loggerName = "test_logger",
      level = LogLevel.Info,
      message = "This is a test",
      ts = LocalDateTime.of(2010, 1, 2, 3, 4, 5),
    )
    assertEquals(expected = expectedToStringResult, actual = msg.toString())
  }
}
