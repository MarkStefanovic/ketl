@file:Suppress("unused")

package ketl

import ketl.domain.DefaultJobQueue
import ketl.domain.DefaultJobResults
import ketl.domain.DefaultJobStatuses
import ketl.domain.JobQueue
import ketl.domain.JobResults
import ketl.domain.JobService
import ketl.domain.JobStatuses
import ketl.domain.Log
import ketl.domain.LogMessage
import ketl.domain.LogMessages
import ketl.domain.NamedLog
import ketl.domain.jobRunner
import ketl.domain.jobScheduler
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import java.io.File
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@DelicateCoroutinesApi
@ExperimentalTime
suspend fun start(
  jobService: JobService,
  logMessages: SharedFlow<LogMessage> = LogMessages.stream,
  log: Log = NamedLog(name = "ketl", stream = logMessages),
  jobQueue: JobQueue = DefaultJobQueue,
  jobStatuses: JobStatuses = DefaultJobStatuses,
  jobResults: JobResults = DefaultJobResults,
  maxSimultaneousJobs: Int = 10,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  timeBetweenScans: Duration = Duration.seconds(10),
) = coroutineScope {
  try {
    log.info("Starting services...")

    launch(dispatcher) {
      jobScheduler(
        jobService = jobService,
        queue = jobQueue,
        results = jobResults,
        statuses = jobStatuses,
        timeBetweenScans = timeBetweenScans,
      )
    }

    val job = launch(dispatcher) {
      jobRunner(
        queue = jobQueue,
        results = jobResults,
        statuses = jobStatuses,
        logMessages = logMessages,
        dispatcher = dispatcher,
        maxSimultaneousJobs = maxSimultaneousJobs,
        timeBetweenScans = timeBetweenScans,
      )
    }

    job.invokeOnCompletion {
      it?.printStackTrace()

      try {
        dispatcher.cancelChildren(it as? CancellationException)
        launch(dispatcher) {
          log.info(message = "Children cancelled.")
        }
      } catch (e: Throwable) {
        launch(dispatcher) {
          log.info("An exception occurred while closing child coroutines: ${e.stackTraceToString()}")
        }
      }
    }

    job
  } catch (e: Throwable) {
    log.info(e.stackTraceToString())
    throw e
  }
}

private fun runJar(jarPath: File, jvmArgs: List<String> = emptyList()) {
  val javaHome = System.getProperty("java.home")
  val javaPath = javaHome + File.separator + "bin" + File.separator + "java"
  val cmd = arrayListOf(javaPath, "-jar", jarPath.path, *jvmArgs.toTypedArray())
  ProcessBuilder(cmd)
    .directory(File(jarPath.parent))
    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
    .redirectError(ProcessBuilder.Redirect.INHERIT)
    .start()
    .waitFor()
}

@ExperimentalTime
fun restartJarOnCrash(
  jarPath: File,
  jvmArgs: List<String> = emptyList(),
  timeBetweenRestarts: Duration = Duration.seconds(10),
) {
  while (true) {
    runJar(jarPath = jarPath, jvmArgs = jvmArgs)
    println("Process crashed.  Waiting $timeBetweenRestarts and then restarting...")
    Thread.sleep(timeBetweenRestarts.inWholeMilliseconds)
  }
}
