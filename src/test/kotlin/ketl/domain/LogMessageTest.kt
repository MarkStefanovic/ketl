package ketl.domain

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

private const val expectedToStringResult = """LogMessage [
  loggerName: test_logger
  level: Info
  message: This is a test
  thread: test_thread
]"""

class LogMessageTest {
  @Test
  fun to_string_happy_path() {
    val msg = LogMessage(
      loggerName = "test_logger",
      level = LogLevel.Info,
      message = "This is a test",
      thread = "test_thread",
    )
    assertEquals(expected = expectedToStringResult, actual = msg.toString())
  }
}
