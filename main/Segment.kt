@file:Suppress("unused")

package com.tryformation.beamjobs

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import mu.KotlinLogging
import org.geojson.*
import java.lang.Double.POSITIVE_INFINITY

private val logger = KotlinLogging.logger {}

data class Segment(
        /** unique id of the event; use a uuid */
        val id: String,
        /** epochMillis the event was recorded */
        val timeStampStart: Long,
        val timeStampEnd: Long,
        /** id of the user sending the location */
        val userId: String,
        val floorLvl: Int?,
        val mqtt: String?,
        var smoothedEvents: List<TrackingEvent>
)
data class SegmentParameters(
        // SEE DOCUMENTATION ON SLITE ABOUT HOW TO OPTIMALLY SET THE SEGMENT PARAMETERS
        // realSplitConditions:
        val distanceThresholdM: Double = 10.0,
        val speedThresholdMperS: Double = 10.0,     // for pedestrians = 10.0, for cars must be set much higher
        // performanceSplitConditions: (done iot reduce computing time)
        val timeThresholdS: Double = 10.0,
        val maxSegmentLength: Long = POSITIVE_INFINITY.toLong(),
        // parameters that influence output data quality (=fineness and degree of smoothing) and computing time
        val timeBetweenFramesMillis: Long = 1000,   // with walking speed of 2 to 4 meters per sec we shouldnt set much coarser frames than 1000
        var smoothingWindowS: Long = 6,             // min=timeBetweenFramesMillis/1000
        // output data format:
        val outputFormat: OutputFormat = OutputFormat.KibanaProduction
)
enum class OutputFormat { Json, GeoJson, KibanaLocal, KibanaProduction }

fun List<Segment>.gluePerformanceSplits(parameters:SegmentParameters):List<Segment> {
    // TESTED IN TRACKINGEVENTTEST.KT
    val gluedList = mutableListOf<Segment>()
    for (i in 1 until this.size) {
        var firstSegment = this[i - 1]
        val secondSegment = this[i]
        val firstEvent = this[i - 1].smoothedEvents.last()
        val secondEvent = secondSegment.smoothedEvents.first()
        if ( ! realSplitConditionFulfilled(secondEvent,firstEvent,parameters)) {
            if (gluedList.size > 0 && gluedList.last().smoothedEvents.last() == firstEvent) {
                firstSegment = gluedList.removeLast()
            }
            gluedList.add(Segment(firstSegment.id,
                    firstSegment.timeStampStart, secondSegment.timeStampEnd,
                    firstSegment.userId, firstSegment.floorLvl, firstSegment.mqtt,
                    firstSegment.smoothedEvents + secondSegment.smoothedEvents))
        }
    }
    return gluedList
}

fun Segment.toJson(): String = mapper.writeValueAsString(this)
fun String.toSegment(): Segment = mapper.readValue(this)

fun SegmentParameters.toJson(): String = mapper.writeValueAsString(this)
fun String.toSegmentParameters(): SegmentParameters = mapper.readValue(this)
class MyFeature(): Feature() {
    public var publicId = this.id
}

fun Segment.toGeoJsonFeature(): MyFeature {
    // maybe interesting in the future: https://github.com/data2viz/geojson-kotlin
    // https://geojson.org/
    // > lineString type: https://docs.mapbox.com/archive/android/java/api/libjava-geojson/3.0.1/index.html?com/mapbox/geojson/LineString.html

    val arrayLngLatAlt = this.smoothedEvents.map {LngLatAlt(it.longitude,it.latitude)}.toTypedArray()

    val feature = MyFeature()
    feature.geometry = LineString(*arrayLngLatAlt)
    feature.properties = mapOf(
            "id" to this.id,
            "timeStampStart" to this.timeStampStart,
            "timeStampEnd" to this.timeStampEnd,
            "eventsTimeStamps" to this.smoothedEvents.joinToString { longToFileTimeStamp(it.timeStamp) },
            "userId" to this.userId,
            "mqtt" to this.mqtt,
            "floorLvl" to this.floorLvl
    )
    feature.publicId = feature.properties["id"].toString()
    return feature
}

fun List<Segment>.toGeoJson():String {
    //return ObjectMapper().writeValueAsString(this.toGeoJsonFeatureCollection()) // if we want a feature collection per user instead of "lose" segments
    var oneFeaturePerSegment = ""
    this.forEach{
        println("${it.id}: ${ObjectMapper().writeValueAsString(it.toGeoJsonFeature())}")
        oneFeaturePerSegment = oneFeaturePerSegment.plus(ObjectMapper().writeValueAsString(it.toGeoJsonFeature())).plus("\n")}
    println("all features")
    println(oneFeaturePerSegment)
    return oneFeaturePerSegment
}
