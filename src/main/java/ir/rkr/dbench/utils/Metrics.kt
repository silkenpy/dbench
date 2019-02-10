package ir.rkr.dbench.utils


import com.codahale.metrics.Gauge
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import java.util.function.Supplier


data class MeterPojo(val count: Long,
                     val rate: Double,
                     val oneMinuteRate: Double,
                     val fiveMinuteRate: Double,
                     val fifteenMinuteRate: Double)

data class ServerInfo(val gauges: Map<String, Any>, val meters: Map<String, MeterPojo>)

class Metrics {

    val metricRegistry = MetricRegistry()

    val MetricsCall = metricRegistry.meter("MetricsCall")

    fun MarkMetricsCall(l: Long = 1) = MetricsCall.mark(l)

    fun <T> addGauge(name: String, supplier: Supplier<T>) = metricRegistry.register(name, Gauge<T> { supplier.get() })

    private fun sortMetersByCount(meters: Map<String, Meter>) =
            meters.toList().sortedBy { it.second.count }.reversed()
                    .map { Pair(it.first, it.second.toPojo()) }.toMap()

    private fun Meter.toPojo() = MeterPojo(count, meanRate, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate)

    fun getInfo() = ServerInfo(metricRegistry.gauges.mapValues { it.value.value },
            sortMetersByCount(metricRegistry.meters))

}

