package ketl

import com.zaxxer.hikari.HikariDataSource
import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDate
import java.time.LocalDateTime

@DslMarker annotation class RunSpDSL

enum class DbDialect {
  PostgreSQL,
}

fun runSp(
  ds: HikariDataSource,
  schemaName: String,
  spName: String,
  dialect: DbDialect,
  init: Sp.Builder.() -> Unit = {},
): Map<String, Any?> {
  val sp = Sp.Builder(dialect = dialect).apply(init).build()

  val inputParams = sp.inputParams.sortedBy { it.name }

  val outputParams = sp.outputParams.sortedBy { it.name }

  val sql = when (dialect) {
    DbDialect.PostgreSQL -> {
      val paramsSQL = inputParams.joinToString(", ") { p -> "${p.name} := ?" }
      "CALL $schemaName.$spName($paramsSQL)"
    }
  }

  ds.connection.use { con ->
    con.prepareCall(sql).use { call ->
      inputParams.forEach { param ->
        when (param) {
          is SpParam.bool ->
            call.setBoolean(param.name, param.value)
          is SpParam.nullableBool ->
            if (param.value == null) {
              call.setNull(param.name, Types.BOOLEAN)
            } else {
              call.setBoolean(param.name, param.value)
            }
          is SpParam.date ->
            call.setDate(param.name, Date.valueOf(param.value))
          is SpParam.nullableDate ->
            if (param.value == null) {
              call.setNull(param.name, Types.DATE)
            } else {
              call.setDate(param.name, Date.valueOf(param.value))
            }
          is SpParam.datetime ->
            call.setTimestamp(param.name, Timestamp.valueOf(param.value))
          is SpParam.nullableDatetime ->
            if (param.value == null) {
              call.setNull(param.name, Types.TIMESTAMP)
            } else {
              call.setTimestamp(param.name, Timestamp.valueOf(param.value))
            }
          is SpParam.decimal ->
            call.setBigDecimal(param.name, param.value)
          is SpParam.nullableDecimal ->
            if (param.value == null) {
              call.setNull(param.name, Types.DECIMAL)
            } else {
              call.setBigDecimal(param.name, param.value)
            }
          is SpParam.float ->
            call.setFloat(param.name, param.value)
          is SpParam.nullableFloat ->
            if (param.value == null) {
              call.setNull(param.name, Types.FLOAT)
            } else {
              call.setFloat(param.name, param.value)
            }
          is SpParam.int ->
            call.setInt(param.name, param.value)
          is SpParam.nullableInt ->
            if (param.value == null) {
              call.setNull(param.name, Types.INTEGER)
            } else {
              call.setInt(param.name, param.value)
            }
          is SpParam.text ->
            call.setString(param.name, param.value)
          is SpParam.nullableText ->
            if (param.value == null) {
              call.setNull(param.name, Types.VARCHAR)
            } else {
              call.setString(param.name, param.value)
            }
        }
      }
      if (outputParams.isEmpty()) {
        call.execute()
        return emptyMap()
      } else {
        val result = mutableMapOf<String, Any?>()
        call.executeQuery().use { rs ->
          for (param in outputParams) {
            result[param.name] = when (param) {
              is SpParam.bool -> rs.getBoolean(param.name)
              is SpParam.nullableBool -> rs.getObject(param.name)
              is SpParam.date -> rs.getDate(param.name).toLocalDate()
              is SpParam.nullableDate -> {
                val value = rs.getObject(param.name)
                if (value == null) {
                  null
                } else {
                  (value as Date).toLocalDate()
                }
              }
              is SpParam.datetime -> rs.getTimestamp(param.name).toLocalDateTime()
              is SpParam.nullableDatetime -> {
                val value = rs.getObject(param.name)
                if (value == null) {
                  null
                } else {
                  (value as Timestamp).toLocalDateTime()
                }
              }
              is SpParam.decimal -> rs.getBigDecimal(param.name)
              is SpParam.nullableDecimal -> rs.getObject(param.name)
              is SpParam.float -> rs.getFloat(param.name)
              is SpParam.nullableFloat -> rs.getObject(param.name)
              is SpParam.int -> rs.getInt(param.name)
              is SpParam.nullableInt -> rs.getObject(param.name)
              is SpParam.text -> rs.getString(param.name)
              is SpParam.nullableText -> rs.getObject(param.name)
            }
          }
        }
        return result
      }
    }
  }
}

data class Sp(
  val inputParams: Set<SpParam>,
  val outputParams: Set<SpParam>,
) {
  @RunSpDSL
  class Builder(val dialect: DbDialect) {
    private var inputs: Set<SpParam> = setOf()
    private var outputs: Set<SpParam> = setOf()

    fun build() = Sp(inputParams = inputs.toSet(), outputParams = outputs.toSet())

    fun inputParams(init: SpParams.Builder.() -> Unit) {
      inputs = SpParams.Builder().apply(init).build().params
    }

    fun outputParams(init: SpParams.Builder.() -> Unit) {
      outputs = SpParams.Builder().apply(init).build().params
    }
  }
}

data class SpParams(val params: Set<SpParam>) {
  @RunSpDSL
  class Builder {
    private val params = mutableListOf<SpParam>()

    fun build() = SpParams(params.toSet())

    fun bool(name: String, value: Boolean) {
      params.add(SpParam.bool(name = name, value = value))
    }

    fun nullableBool(name: String, value: Boolean?) {
      params.add(SpParam.nullableBool(name = name, value = value))
    }

    fun date(name: String, value: LocalDate) {
      params.add(SpParam.date(name = name, value = value))
    }

    fun nullableDate(name: String, value: LocalDate?) {
      params.add(SpParam.nullableDate(name = name, value = value))
    }

    fun datetime(name: String, value: LocalDateTime) {
      params.add(SpParam.datetime(name = name, value = value))
    }

    fun nullableDatetime(name: String, value: LocalDateTime?) {
      params.add(SpParam.nullableDatetime(name = name, value = value))
    }

    fun decimal(name: String, value: BigDecimal) {
      params.add(SpParam.decimal(name = name, value = value))
    }

    fun nullableDecimal(name: String, value: BigDecimal?) {
      params.add(SpParam.nullableDecimal(name = name, value = value))
    }

    fun float(name: String, value: Float) {
      params.add(SpParam.float(name = name, value = value))
    }

    fun nullableFloat(name: String, value: Float?) {
      params.add(SpParam.nullableFloat(name = name, value = value))
    }

    fun int(name: String, value: Int) {
      params.add(SpParam.int(name = name, value = value))
    }

    fun nullableInt(name: String, value: Int?) {
      params.add(SpParam.nullableInt(name = name, value = value))
    }

    fun text(name: String, value: String) {
      params.add(SpParam.text(name = name, value = value))
    }

    fun nullableText(name: String, value: String?) {
      params.add(SpParam.nullableText(name = name, value = value))
    }
  }
}

sealed class SpParam {
  abstract val name: String
  abstract val value: Any?

  data class bool(
    override val name: String,
    override val value: Boolean,
  ) : SpParam()

  data class nullableBool(
    override val name: String,
    override val value: Boolean?,
  ) : SpParam()

  data class date(
    override val name: String,
    override val value: LocalDate,
  ) : SpParam()

  data class nullableDate(
    override val name: String,
    override val value: LocalDate?,
  ) : SpParam()

  data class datetime(
    override val name: String,
    override val value: LocalDateTime,
  ) : SpParam()

  data class nullableDatetime(
    override val name: String,
    override val value: LocalDateTime?,
  ) : SpParam()

  data class decimal(
    override val name: String,
    override val value: BigDecimal,
  ) : SpParam()

  data class nullableDecimal(
    override val name: String,
    override val value: BigDecimal?,
  ) : SpParam()

  data class float(
    override val name: String,
    override val value: Float,
  ) : SpParam()

  data class nullableFloat(
    override val name: String,
    override val value: Float?,
  ) : SpParam()

  data class int(
    override val name: String,
    override val value: Int,
  ) : SpParam()

  data class nullableInt(
    override val name: String,
    override val value: Int?,
  ) : SpParam()

  data class text(
    override val name: String,
    override val value: String,
  ) : SpParam()

  data class nullableText(
    override val name: String,
    override val value: String?,
  ) : SpParam()
}
