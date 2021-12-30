@file:Suppress("unused")
package com.tryformation.beamjobs

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import mu.KotlinLogging
import java.time.Instant
import kotlin.math.max
import kotlin.math.min

private val logger = KotlinLogging.logger {  }

data class TrackingEvent(
        /** unique id of the event; use a uuid */
        val id: String,
        /** epochMillis the event was recorded */
        var timeStamp: Long,
        /** id of the user sending the location */
        var userId: String,
        val longitude: Double,
        val latitude: Double,
        var objectId: String?=null,
        val altitude: Double? = null, //@Nullable
        val accuracy: Double? = null,
        val verticalAccuracy: Double? = null,
        val bearing: Double? = null,
        val bearingAccuracyDegrees: Double? = null,
        val speed: Double? = null,
        val speedAccuracyMetersPerSecond: Double? = null,
        val isoTime: String? = null,

        val tags: List<String>? = null,
        val location: List<Double>? = null
) {
    val floorLvl by lazy { this.tagMap()["FL_LVL"]?.toInt() }
        //this.tags?.first { it.contains("FL_ID:") }?.let { it.removePrefix("FL_ID:") } } // < old, for jupyter floorLvl level is needed
    val assetId by lazy { this.tagMap()["assetId"] }
    val mqtt by lazy { this.tagMap()["mqtt"] }
}
fun TrackingEvent.tagMap() = tags?.map {
    val split = it.split(':')
    split[0] to split[1]
}?.toMap() ?: mapOf()

fun TrackingEvent.assetIdAsUserId():TrackingEvent {
    this.userId = if (this.assetId!=null) this.assetId!! else {this.userId}
    return this
}

fun TrackingEvent.metersTo(otherEvent: TrackingEvent): Double {
    val lat1 = this.latitude
    val lat2 = otherEvent.latitude
    val lng1 = this.longitude
    val lng2 = otherEvent.longitude

    return org.apache.lucene.util.SloppyMath.haversinMeters(lat1, lng1, lat2, lng2)
}

fun TrackingEvent.secondsSince(otherEvent: TrackingEvent): Double = ((this.timeStamp - otherEvent.timeStamp) / 1000.0)

fun TrackingEvent.averageSpeedInMperSecComingFrom(otherEvent: TrackingEvent): Double {
    return this.metersTo(otherEvent) / this.secondsSince(otherEvent)
}

fun TrackingEvent.sameFloorLvlAs(otherEvent: TrackingEvent): Boolean {
    return this.floorLvl == otherEvent.floorLvl
}
fun realSplitConditionFulfilled(currentEvent: TrackingEvent, previousEvent: TrackingEvent?, parameters: SegmentParameters):Boolean {
    return (previousEvent == null
            || currentEvent.metersTo(previousEvent) > parameters.distanceThresholdM
            || currentEvent.averageSpeedInMperSecComingFrom(previousEvent) > parameters.speedThresholdMperS
            || ! currentEvent.sameFloorLvlAs(previousEvent)
            )
}
fun performanceSplitConditionFulfilled(currentEvent: TrackingEvent, previousEvent: TrackingEvent,
                                       currentEventIndex:Int, splitStartIndex:Int, parameters: SegmentParameters):Boolean {
    // splits that fulfill performanceSplitConditions, but not
    // realSplitConditions will be glued together again at the end
    return currentEvent.secondsSince(previousEvent) > parameters.timeThresholdS
            || (currentEventIndex - splitStartIndex).toLong() >= parameters.maxSegmentLength
}

fun List<TrackingEvent>.extractSmoothenedSegments(parameters: SegmentParameters):
        List<Segment> {
    val splitList = logger.time("splitList for ${this.size} events ${this.first().userId}") {
        this.splitIntoParts(parameters)
    }
    logger.info { "found ${splitList.size} segments for ${this.size} events" }
    val segmentList = splitList.map { it.partsToSegment(parameters) }
    return segmentList.gluePerformanceSplits(parameters)
}
fun List<TrackingEvent>.splitIntoParts(parameters: SegmentParameters):MutableList<List<TrackingEvent>> {
    var splitStartIndex = 0
    val splitList = mutableListOf<List<TrackingEvent>>()
    for (currentEventIndex in 1 until this.size) {
        val currentEvent = this[currentEventIndex]
        val previousEvent = this[currentEventIndex - 1]
        val penultimateEvent = if (currentEventIndex!=1) this[currentEventIndex - 2] else null
        if (realSplitConditionFulfilled(currentEvent,previousEvent,parameters)
                || performanceSplitConditionFulfilled(currentEvent,previousEvent,currentEventIndex,splitStartIndex,parameters)
        ) {
            // following condition manages data cleaning:
            // isolated tracking Events cut off on both sides by realSplits are regarded as corrupted data and
            // not added to our final splitList
            if (!(currentEventIndex - 1 == splitStartIndex
                            && realSplitConditionFulfilled(currentEvent,previousEvent,parameters)
                            && realSplitConditionFulfilled(previousEvent,penultimateEvent,parameters))) {
                val splitEnd = currentEventIndex - 1
                splitList.add(slice(splitStartIndex..splitEnd))

                if (currentEventIndex == this.size - 1 && !realSplitConditionFulfilled(currentEvent,previousEvent,parameters)) {
                    splitList.add(slice(currentEventIndex..currentEventIndex))
                }
            }
            splitStartIndex = currentEventIndex
        }
        else if (currentEventIndex == this.size - 1) {
            splitList.add(slice(splitStartIndex..currentEventIndex))
        }
    }
    return splitList
}

fun List<TrackingEvent>.partsToSegment(
        parameters: SegmentParameters
): Segment {
    assert(this.size <= parameters.maxSegmentLength)
    val segmentId = this.first().userId + Instant.now().toEpochMilli().toString()
    val startTime = this.first().timeStamp
    val endTime = this.last().timeStamp
    val userId = this.first().userId
    val floorLvl = this.first().floorLvl
    val mqtt = this.first().mqtt

    return logger.time("getSmoothSegment for ${this.size} events $userId & $floorLvl") {

        // the idea of firstFrame and lastFrame was that we get interpolated data for every
        // user at the same points in time so for a given point in time we have a photo of who/what is where.
        // thats why I used the various toLong(). toInt() conversions.
        // can also be added later though if necessary, we can stick with this simpler version
        val interpolated = this.getInterpolatedFrames(parameters)
        val smoothed = logger.time("smoothing events") {
            interpolated.smoothenFrames(parameters)
        }

        Segment(segmentId, startTime, endTime, userId, floorLvl, mqtt ,smoothed)
    }
}
fun List<TrackingEvent>.getInterpolatedFrames(parameters:SegmentParameters): List<TrackingEvent> {
    val startTime = this.first().timeStamp
    val endTime = this.last().timeStamp

    val numFrames = (endTime - startTime) / parameters.timeBetweenFramesMillis
    val allFrames = (0..numFrames).map { startTime + it * parameters.timeBetweenFramesMillis }

    return allFrames.map { interpolate(it, this) }
}
fun List<TrackingEvent>.smoothenFrames(parameters: SegmentParameters): List<TrackingEvent> {
    return this.map { currentEvent ->
        parameters.smoothingWindowS = max(parameters.timeBetweenFramesMillis/500,parameters.smoothingWindowS)
        var lowerBound = currentEvent.timeStamp - parameters.smoothingWindowS * 500 //get half window in ms
        var upperBound = currentEvent.timeStamp + parameters.smoothingWindowS * 500

        if (this.first().timeStamp >= lowerBound || this.last().timeStamp <= upperBound) {
            val newWindowMillis = min(
                    currentEvent.timeStamp - this.first().timeStamp,
                    this.last().timeStamp - currentEvent.timeStamp
            )
            lowerBound = currentEvent.timeStamp - newWindowMillis //get half window in m
            upperBound = currentEvent.timeStamp + newWindowMillis
        }

        val newLat = this.filter { it.timeStamp in lowerBound..upperBound }.map { it.latitude }.average()
        val newLng = this.filter { it.timeStamp in lowerBound..upperBound }.map { it.longitude }.average()

        currentEvent.copy(latitude = newLat, longitude = newLng, timeStamp = currentEvent.timeStamp)
    }
}
// https://www.baeldung.com/jackson-kotlin
val mapper:ObjectMapper = ObjectMapper().registerModule(KotlinModule())
fun TrackingEvent.toJson(): String = mapper.writeValueAsString(this)
fun String.readTrackingEvent(): TrackingEvent = mapper.readValue(this)
fun String.toTrackingEvent(): TrackingEvent = this.readTrackingEvent().assetIdAsUserId()
