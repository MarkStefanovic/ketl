package ketl.domain

import ketl.adapter.Db
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlin.coroutines.cancellation.CancellationException

@DelicateCoroutinesApi
suspend fun jobResultLogger(
  db: Db,
  results: SharedFlow<JobResult>,
  repository: ResultRepository,
) {
  results.collect { result ->
    try {
      db.exec {
        repository.add(result)
      }
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
