@file:Suppress("unused")

package ketl

import ketl.adapter.Db
import ketl.adapter.DbJobStatusRepository
import ketl.adapter.DbLogRepository
import ketl.adapter.ExposedResultRepository
import ketl.adapter.SingleThreadedDb
import ketl.adapter.exposedLogRepositoryCleaner
import ketl.adapter.exposedResultRepositoryCleaner
import ketl.adapter.sqlLogger
import ketl.domain.ETLJob
import ketl.domain.JobContext
import ketl.domain.JobQueue
import ketl.domain.JobResults
import ketl.domain.JobStatuses
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.consoleLogger
import ketl.domain.jobResultLogger
import ketl.domain.jobRunner
import ketl.domain.jobScheduler
import ketl.domain.jobStatusLogger
import ketl.domain.jobStatusSnapshotConsoleLogger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.io.File
import kotlin.system.exitProcess
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@DelicateCoroutinesApi
@InternalCoroutinesApi
@ExperimentalTime
private suspend fun startServices(
  db: Db,
  log: LogMessages,
  jobs: List<ETLJob<*>>,
  logStatusToConsole: Boolean,
  maxSimultaneousJobs: Int,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  log.info("Starting ketl services...")

  log.info("Checking if ETL tables exist...")
  db.createTables().join()

  val logRepository = DbLogRepository()

  log.info("Starting log repository cleaner...")
  launch(dispatcher) {
    exposedLogRepositoryCleaner(
      db = db,
      repository = logRepository,
      log = log,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = Duration.days(3),
    )
  }

  log.info("Starting SQL logging...")
  launch(dispatcher) {
    sqlLogger(
      db = db,
      repository = logRepository,
      messages = log.stream,
    )
  }

  val statusRepository = DbJobStatusRepository()

  log.info("Starting JobStatuses...")
  val statuses = JobStatuses()

  log.info("Starting console logger...")
  if (logStatusToConsole) {
    launch(dispatcher) {
      jobStatusLogger(
        db = db,
        repository = statusRepository,
        status = statuses.stream,
      )
    }
  }

  log.info("Starting job status console logger...")
  launch(dispatcher) { jobStatusSnapshotConsoleLogger(statuses = statuses) }
  val resultRepository = ExposedResultRepository()

  log.info("Starting result repository cleaner...")
  launch(dispatcher) {
    exposedResultRepositoryCleaner(
      db = db,
      repository = resultRepository,
      log = log,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = Duration.days(3),
    )
  }

  log.info("Starting JobResults...")
  val results = JobResults(db = db)

  log.info("Starting job result logger...")
  launch(dispatcher) {
    jobResultLogger(
      db = db,
      results = results.stream,
      repository = resultRepository,
    )
  }

  val jobQueue = JobQueue()

  log.info("Starting job scheduler...")
  launch(dispatcher) {
    jobScheduler(
      queue = jobQueue,
      jobs = jobs,
      status = statuses,
      maxSimultaneousJobs = maxSimultaneousJobs,
      scanFrequency = Duration.seconds(10),
      results = results,
    )
  }

  log.info("Starting JobRunner...")
  launch(dispatcher) {
    jobRunner(
      log = log,
      queue = jobQueue.stream,
      results = results,
      status = statuses,
      dispatcher = dispatcher,
    )
  }

  log.info("ketl services launched.")
}

@Suppress("BlockingMethodInNonBlockingContext")
@DelicateCoroutinesApi
@InternalCoroutinesApi
@ExperimentalTime
suspend fun <Ctx : JobContext> start(
  createContext: (log: LogMessages) -> Ctx,
  createJobs: (ctx: Ctx) -> List<ETLJob<*>>,
  maxSimultaneousJobs: Int,
  logJobMessagesToConsole: Boolean = true,
  logStatusChangesToConsole: Boolean = true,
  minLogLevel: LogLevel = LogLevel.Info,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  etlDbPath: String = "./etl.db",
) = coroutineScope {
  val log = LogMessages("ketl")

  if (logJobMessagesToConsole) {
    launch(dispatcher) {
      println("Starting console logger...")
      consoleLogger(messages = log.stream, minLogLevel = minLogLevel)
    }
  }

  val ctx = createContext(log)
  try {
    val jobs = try {
      createJobs(ctx)
    } catch (e: Throwable) {
      println("An error occurred while creating jobs: ${e.stackTraceToString()}")
      throw e
    }

    val ds = sqliteDatasource(etlDbPath)
    val job = try {
      log.info("Starting services...")
      launch(dispatcher) {
        startServices(
          db = SingleThreadedDb(ds),
          log = log,
          jobs = jobs,
          logStatusToConsole = logStatusChangesToConsole,
          dispatcher = dispatcher,
          maxSimultaneousJobs = maxSimultaneousJobs,
        )
      }
    } catch (e: Throwable) {
      ds.close()
      println("Closed connection to ETL database.")
      throw e
    }

    job.invokeOnCompletion {
      it?.printStackTrace()

      try {
        dispatcher.cancelChildren(it as? CancellationException)
        println("Children cancelled.")
      } catch (e: Throwable) {
        println("An exception occurred while closing child processes: ${e.stackTraceToString()}")
      }

      try {
        ds.close()
        println("Closed connection to ETL database.")
      } catch (e: Throwable) {
        println("An error occurred while closing the ETL datasource: ${e.stackTraceToString()}")
      }

      try {
        ctx.close()
        println("Closed job context.")
      } catch (e: Throwable) {
        println("An error occurred while closing the enclosing job context: ${e.stackTraceToString()}")
      } finally {
        exitProcess(1)
      }
    }

    job
  } catch (e: Throwable) {
    try {
      ctx.close()
    } catch (e2: Throwable) {
      println("An error occurred while closing the job context: ${e2.stackTraceToString()}")
    }
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
