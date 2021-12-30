@file:Suppress("unused")
package com.tryformation.beamjobs

import mu.KLogger
import java.io.File
import java.time.*
import java.time.format.DateTimeFormatter
import kotlin.math.pow
import kotlin.math.roundToInt


fun <T> KLogger.time(msg: String, block: () -> T): T {
    val start = System.currentTimeMillis()
    this.info { "begin $msg" }
    val res = block.invoke()
    this.info { "end " + msg + " in  ${System.currentTimeMillis() - start} ms." }
    return res
}

fun interpolate(newX: Long, x0: Long, x1: Long, y0: Long, y1: Long): Double {
    assert(newX in x0..x1)
    val interpolationPercent = (newX - x0).toDouble() / (x1 - x0).toDouble()
    return y0.toDouble() + interpolationPercent * (y1 - y0).toDouble()
}
fun interpolate(newX: Double, x0: Double, x1: Double, y0: Double, y1: Double): Double {
    assert(x0<newX && newX<x1)
    val interpolationPercent = (newX - x0) / (x1 - x0)
    return y0 + interpolationPercent * (y1 - y0)
}

fun interpolate(
        newTime: Long, t0: Long, t1: Long,
        latlng0: List<Double>,
        latlng1: List<Double>
): List<Double> {
    assert(newTime in t0..t1)
    assert(latlng0.size == latlng1.size)
    assert(latlng0.size == 2) // always a point ...

    val interpolationPercent = if (t0!=t1) {(newTime.toDouble() - t0.toDouble()) / (t1.toDouble() - t0.toDouble())} else {0.0}

    val lat = latlng0[0] + interpolationPercent * (latlng1[0] - latlng0[0])
    val lon = latlng0[1] + interpolationPercent * (latlng1[1] - latlng0[1])
    return listOf(lat,lon)
}
fun interpolate(time: Long, trackingEvent0: TrackingEvent, trackingEvent1: TrackingEvent): TrackingEvent {
    assert(trackingEvent0.userId==trackingEvent1.userId)
    assert(trackingEvent0.floorLvl==trackingEvent1.floorLvl)
    val t0 = trackingEvent0.timeStamp
    val t1 = trackingEvent1.timeStamp
    assert(time in t0..t1)
    val point0 = listOf(trackingEvent0.latitude, trackingEvent0.longitude)
    val point1 = listOf(trackingEvent1.latitude, trackingEvent1.longitude)
    val latLng = interpolate(time, t0, t1, point0, point1)

    val id = "interpolated_${trackingEvent0.userId}_$time"

    return trackingEvent0.copy(id=id, latitude = latLng.first(), longitude = latLng.last(), timeStamp = time)
}
fun interpolate(time: Long, trackingEvents: List<TrackingEvent>): TrackingEvent {
    assert(trackingEvents.first().timeStamp <= time && time <= trackingEvents.last().timeStamp)
    return when {
        trackingEvents.first().timeStamp < time && time < trackingEvents.last().timeStamp -> {
            val trackingEvent0 = trackingEvents.last { it.timeStamp <= time }
            val trackingEvent1 = trackingEvents.first { it.timeStamp >= time }

            interpolate(time, trackingEvent0, trackingEvent1)
        }
        trackingEvents.first().timeStamp == time -> {
            trackingEvents.first()
        }
        else -> {
            trackingEvents.last()
        }
    }
}

fun Double.roundToDecimal(decimalPlace: Int) = (this * 10.0.pow(decimalPlace)).roundToInt().toDouble() / (10.0.pow(decimalPlace))

fun boxplot(numsNotSorted: List<Double>, roundToXdecimalPlaces: Int? = null): List<Double> {
    val nums = numsNotSorted.sorted()
    return listOf(0.0, 0.25, 0.5, 0.75, 1.0).map { percent ->
        var result: Double
        val index = (nums.size - 1).toDouble() * percent
        // why are we interpolating array indices?
        // @Jilles: necessary for getting the best estimates of the quantiles if we have a small list
        // > if we have nums= [0,1,300,301,302,...], and the resulting index is 1.01, our resulting
        //   quantile should be close to 1; if the index were 1.5, the resulting quantile should be (300+1)/2
        // ??? index.roundToInt().toDouble() == index  -> index.roundToInt().toDouble() == nums.size?
        // ^no, these are not the same. the check "index.roundToInt().toDouble() == index" is correct
        // bc interpolation is only necessary if resulting index is not an integer.
        result = if (index.roundToInt().toDouble() == index) nums[index.toInt()]
        else interpolate(
                index,
                index.toInt().toDouble(),
                (index.toInt() + 1).toDouble(),
                nums[index.toInt()],
                nums[index.toInt() + 1]
        )
        if (roundToXdecimalPlaces != null) result = result.roundToDecimal(roundToXdecimalPlaces)
        result
    }
}

internal val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")

fun dateTime(timestamp: Long):OffsetDateTime = Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC)
fun instantToFileTimestamp(instant: Instant) = DateTimeFormatter
        .ofPattern("yyyy-MM-dd::HH:mm:ss")
        .withZone(ZoneOffset.UTC)
        .format(instant).replace("::","T")
fun longToFileTimeStamp(long:Long) = instantToFileTimestamp(Instant.ofEpochMilli(long))
fun epochMilliToReadable(epochMilli: Long): String? {
    val instant = Instant.ofEpochMilli(epochMilli)
    val date = LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
    return formatter.format(date) // 10/12/2019 06:35:45
}
fun String.platformIndependentPath():String {
    // Necessary bc TextIO.write().to( outputFile ) with outputFile:String does not seem to share platform independence of "/":
    // > accept any usual path seperator (to my knowledge, / and \\, and replace them by seperator accepted by current os:
    assert(this.count { this.contains('/') } * this.count { this.contains('\\') }==0) // make sure that path doesnt contain both path seperator types
    return if (count { contains('/')} >0) {
        split('/').joinToString(File.separator)
    }
    else if (count { contains('\\') } >0) {
        split("\\\\").joinToString(File.separator)
    }
    else { this }
}
