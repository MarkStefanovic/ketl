package ketl.domain

import java.io.Closeable
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

abstract class JobContext(val log: LogMessages) : Closeable

class BaseContext(log: LogMessages) : JobContext(log) {
  override fun close() {}
}

@ExperimentalContracts
suspend fun <Ctx: JobContext> Ctx.exec(block: suspend Ctx.() -> Unit): Ctx {
  contract {
    callsInPlace(block, InvocationKind.EXACTLY_ONCE)
  }
  this.use {
    block()
  }
  return this
}