package ketl.domain

import ketl.adapter.Db
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlin.coroutines.cancellation.CancellationException

@DelicateCoroutinesApi
suspend fun jobResultLogger(
  db: Db,
  results: SharedFlow<JobResult>,
  repository: ResultRepo,
) {
  results.collect { result ->
    try {
      db.exec {
        repository.add(result)
      }
    } catch (te: TimeoutCancellationException) {
      println("""
        |jobResultLogger: 
        |  db.exec timed out while attempting to add the following result:
        |    $result
        |  message: ${te.message}
        """.trimMargin()
      )
    } catch (e: Exception) {
      if (e is CancellationException) {
        println("jobResultLogger cancelled.")
      } else {
        e.printStackTrace()
      }
      throw e
    }
  }
}
