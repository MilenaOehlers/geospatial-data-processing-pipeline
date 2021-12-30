@file:Suppress("unused")
package com.tryformation.beamjobs

import kotlinx.coroutines.runBlocking
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.Partition.PartitionFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList
import org.apache.http.impl.client.HttpClientBuilder
import java.time.Duration
import java.time.Instant


//region MapUser2Json
class MapUserId2Json : DoFn<String, KV<String, String>>() {
    // groupBy Key: https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/transforms/GroupByKey.html
    @ProcessElement
    fun processElement(@Element element: String, receiver: OutputReceiver<KV<String, String>>) {
        val trackingEvent = element.toTrackingEvent()
        val userId2Json = KV.of(trackingEvent.userId, element)
        println("mapUserId2Json")
        println("${trackingEvent.userId},$element")
        receiver.output(userId2Json)
    }
}
//endregion MapUser2Json

//region AnalyseUserData
class GetUserRawDataStatistics : DoFn<KV<String, Iterable<@JvmWildcard String>>, String>() {
    // Wildcard solution for groupByKey Output: https://stackoverflow.com/questions/55908999/kotlin-iterable-not-supported-in-apache-beam
    @ProcessElement
    fun processElement(
            @Element element: KV<String, Iterable<@JvmWildcard String>>,
            receiver: OutputReceiver<String>
    ) {
        val mutableList = mutableListOf<TrackingEvent>()
        element.value.forEach { mutableList.add(it.toTrackingEvent()) }
        val sorted = mutableList.sortedBy { it.timeStamp }.toMutableList()
        val userStats = RawDataStats(sorted)
        receiver.output(userStats.toJson())
    }
}

//endregion

//region RawDataToGeoJson
class GetGeoJsonOutput : DoFn<KV<String, Iterable<@JvmWildcard String>>, String>() {
    // Wildcard solution for groupByKey Output: https://stackoverflow.com/questions/55908999/kotlin-iterable-not-supported-in-apache-beam
    // kotlin warning process element never used wrong- function does process elements as expected
    @ProcessElement
    fun processElement(
            @Element element: KV<String, Iterable<@JvmWildcard String>>,
            receiver: OutputReceiver<String>
    ) {
        val mutableList = mutableListOf<TrackingEvent>()
        element.value.forEach { mutableList.add(it.toTrackingEvent()) }
        val sorted = mutableList.sortedBy { it.timeStamp }.toMutableList()

        val segmentId = sorted.first().userId + Instant.now().toEpochMilli().toString()
        val startTime = sorted.first().timeStamp
        val endTime = sorted.last().timeStamp
        val userId = sorted.first().userId
        val floorLvl = sorted.first().floorLvl

        val segment = Segment(segmentId, startTime, endTime, userId, floorLvl, sorted.first().mqtt, sorted)

        val output = listOf(segment).toGeoJson()

        return receiver.output(output)
    }
}
//endregion RawDataToGeoJson

//region SpaghettiDiagram
class GetSmoothenedSegmentsPerUser(private val parameters: String) : DoFn<KV<String, Iterable<@JvmWildcard String>>, String>() {
    // Wildcard solution for groupByKey Output: https://stackoverflow.com/questions/55908999/kotlin-iterable-not-supported-in-apache-beam
    // kotlin warning process element never used wrong- function does process elements as expected
    @ProcessElement
    fun processElement(
            @Element element: KV<String, Iterable<@JvmWildcard String>>,
            receiver: OutputReceiver<String>
    ) {
        println("a")
        val segmentParameters = parameters.toSegmentParameters()
        val mutableList = mutableListOf<TrackingEvent>()
        element.value.forEach { mutableList.add(it.toTrackingEvent()) }
        val sorted = mutableList.sortedBy { it.timeStamp }.toMutableList()
        val segments = sorted.extractSmoothenedSegments(segmentParameters)
        val features = segments.map{it.toGeoJsonFeature()}
        val segmentRepo = FeatureRepositoryConfig().setupLocal()
        println("a")
        val output = when (segmentParameters.outputFormat) {
            OutputFormat.Json -> segments.map { it.toJson() }.toString()
            OutputFormat.GeoJson -> segments.toGeoJson()
            OutputFormat.KibanaLocal -> {
                println("before creating local")
                println(segments.toGeoJson())
                runBlocking { FeatureRepositoryConfig().setupLocal().create(features) }
                println("after creating local")
                segments.toGeoJson()
            }
            OutputFormat.KibanaProduction -> {
                println("before creating production")
                println(segments.toGeoJson())
                runBlocking { FeatureRepositoryConfig().setupProduction().create(features) }
                println("after creating production")
                segments.toGeoJson()
            }
        }
        println("USER SEGEMENTS: ")
        println(output)
        return receiver.output(output)
    }
}
//endregion SpaghettiDiagram



sealed class Process : PTransform<PCollection<String>, PCollection<String>>() {
    class AnalyseRawDataOfUsers(private val unused: String = "") : Process() {
        override fun expand(lines: PCollection<String>): PCollection<String>? {
            val trackingEvents = lines.apply(ParDo.of(MapUserId2Json()))
            val trackingEventsByUser = trackingEvents.apply(GroupByKey.create())
            return trackingEventsByUser.apply(ParDo.of(GetUserRawDataStatistics()))
        }
    }
    class RawDataToGeoJson(private val unused: String = "") : Process() {
        override fun expand(lines: PCollection<String>): PCollection<String>? {
            val trackingEvents = lines.apply(ParDo.of(MapUserId2Json()))
            val trackingEventsByUser = trackingEvents.apply(GroupByKey.create())
            return trackingEventsByUser.apply(ParDo.of(GetGeoJsonOutput()))
        }
    }

    class SpaghettiDiagrams(private val parameters: String) : Process() {
        override fun expand(lines: PCollection<String>): PCollection<String>? {
            println("before mapUserId2Json")
            val trackingEvents = lines.apply(ParDo.of(MapUserId2Json()))
            println("before groupByKey")
            val trackingEventsByUser = trackingEvents.apply(GroupByKey.create())
            println("before getSmoothened")
            println(trackingEventsByUser)
            return trackingEventsByUser.apply(ParDo.of(GetSmoothenedSegmentsPerUser(parameters)))
        }
    }
}

interface ApacheBeamOptions : PipelineOptions {
    @get:Description("Path of the file to read from")
    var start:String?

    @get:Description("Path of the file to read from")
    var end :String?

    @get:Description("Path of the file to write to")
    var output: String?
}

fun runPipeline(options: ApacheBeamOptions, executionParameters: ExecutionParameters) { //inputFile: String, outputFile: String, parameters: ExecutionParameters, options: PipelineOptions) {
    // closable resource so use it and close it after we're done processing
    println("start pipeline")
    HttpClientBuilder.create().build().use { httpclient ->
        val pipeline = Pipeline.create(options)
        // the idea here is to stream the response line by line instead of creating a list ...
        println("before list creation")
        val lineIterator = synchronousReadFromApi(options.start, options.end, httpclient)
        val lst = lineIterator.toMutableList()
        //println(lst.size)
        lst.forEach { println(it)}
        //val startEndPairs = getStartEndPairs()
        // use createOf with the iterator in the hope that it will do the right thing and not buffer everything ...
        println("before pipeline")
        pipeline.apply(Create.of(lst).withCoder(StringUtf8Coder.of()))
            // fixme maybe chunk every n items so we can fork with pardo ...
            .apply(executionParameters.process)
            .apply(
                    TextIO.write().to(
                            options.output
                    )
            )

        val result = pipeline.run()
        try {
            result.waitUntilFinish()
        } catch (exc: Exception) {
            result.cancel()
        }
    }


