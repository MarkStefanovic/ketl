package ketl.domain

import kotlin.time.ExperimentalTime

@JvmInline
value class JobValidationResult(val value: Map<String, Set<String>>) {
  val hasErrors: Boolean
    get() = value.values.any { it.isNotEmpty() }

  val isOk: Boolean
    get() = value.values.all { it.isEmpty() }

  val errorMessage: String
    get() = "Job Validation Errors:\n" +
      value
        .filter { it.value.isNotEmpty() }
        .map { (jobName, errors) ->
          "  $jobName:\n    ${errors.joinToString("\n    ")}"
        }
        .joinToString("\n")

  val invalidJobNames: Set<String>
    get() = value.filterValues { it.isNotEmpty() }.keys

  val validJobNames: Set<String>
    get() = value.filterValues { it.isEmpty() }.keys

  override fun toString(): String =
    "JobValidationResult [\n" +
      value
        .filter { it.value.isNotEmpty() }
        .map { (jobName, errors) ->
          if (errors.isEmpty()) {
            "  $jobName: No errors"
          } else {
            "  $jobName:\n    ${errors.joinToString("\n    ")}"
          }
        }
        .joinToString("\n") +
      "\n]"
}

@ExperimentalTime
fun validateJobs(jobs: Set<KETLJob>): JobValidationResult {
  val allJobNames = jobs.map { it.name }

  val validationErrors: Map<String, MutableList<String>> =
    allJobNames.associateWith { mutableListOf() }

  val duplicateJobNames: Map<String, Int> =
    allJobNames
      .groupingBy { it }
      .eachCount()
      .filter { it.value > 1 }

  if (duplicateJobNames.isNotEmpty()) {
    duplicateJobNames.forEach { (jobName, count) ->
      validationErrors[jobName]?.add("$count jobs are named $jobName.")
    }
  }

  jobs.forEach { job ->
    val missingJobDeps = job.dependencies.filter { jobName ->
      jobName !in allJobNames
    }

    if (missingJobDeps.isNotEmpty()) {
      validationErrors[job.name]?.add(
        "The following jobs are listed as dependencies, but they're not scheduled: " +
          missingJobDeps.joinToString(", ") + "."
      )
    }
  }

  return JobValidationResult(
    validationErrors
      .map { it.key to it.value.toSet() }
      .toMap()
  )
}
