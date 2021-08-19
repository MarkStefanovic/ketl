package domain

import main.kotlin.domain.ExecutionWindow
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import kotlin.time.ExperimentalTime

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExecutionWindowTest {
  @Test
  fun ANYTIME_returns_true() {
    val window = ExecutionWindow.ANYTIME
    val inWindow = window.inWindow(refTime = LocalDateTime.now())
    assert(inWindow)
  }
}
