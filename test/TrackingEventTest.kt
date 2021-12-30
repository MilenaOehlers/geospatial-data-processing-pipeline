package com.tryformation.beamjobs

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

class TrackingEventTest {
    @Test
    fun splitConditions_test() {
        val setupTest = SetupTest()
        val segmentParametersTest = setupTest.segmentParametersTest

        val event0 = setupTest.event00
        val event1 = setupTest.event1
        val event2 = setupTest.event2
        val event3 = setupTest.event3

        event0.metersTo(event1).roundToDecimal(2) shouldBe 3.50
        event0.metersTo(event2).roundToDecimal(2) shouldBe 101.98

        event1.secondsSince(event0) shouldBe 2.0
        event2.secondsSince(event0) shouldBe 3.0

        event1.averageSpeedInMperSecComingFrom(event0).roundToDecimal(2) shouldBe (3.5/2.0).roundToDecimal(2)
        event2.averageSpeedInMperSecComingFrom(event0).roundToDecimal(2) shouldBe (101.98/3.0).roundToDecimal(2)

        event0.sameFloorLvlAs(event1) shouldBe true
        event0.sameFloorLvlAs(event2) shouldBe false

        realSplitConditionFulfilled(event1,event0,segmentParametersTest) shouldBe false
        realSplitConditionFulfilled(event2,event1,segmentParametersTest) shouldBe true
        realSplitConditionFulfilled(event3,event2,segmentParametersTest) shouldBe false

        performanceSplitConditionFulfilled(event1,event0,10,1,segmentParametersTest) shouldBe false
        performanceSplitConditionFulfilled(event1,event0,10,0,segmentParametersTest) shouldBe true
        performanceSplitConditionFulfilled(event2,event1,20,11,segmentParametersTest) shouldBe false
        performanceSplitConditionFulfilled(event2,event1,20,10,segmentParametersTest) shouldBe true
        performanceSplitConditionFulfilled(event3,event2,25,20,segmentParametersTest) shouldBe true // performanceSplit due to timeThreshold
    }

    @Test
    fun splitIntoParts_test() {
        val setupTest = SetupTest()
        val segmentParametersTest = setupTest.segmentParametersTest
        val eventList = setupTest.eventList
        val event00 = setupTest.event00
        val event01 = setupTest.event01
        val event02 = setupTest.event02
        val event03 = setupTest.event03
        val event04higherLatLng = setupTest.event04higherLatLng
        val event05smallerLatLng = setupTest.event05smallerLatLng
        val event06: TrackingEvent = setupTest.event06
        val event07: TrackingEvent = setupTest.event07
        val event08: TrackingEvent = setupTest.event08
        val event09: TrackingEvent = setupTest.event09
        val event1 = setupTest.event1
        val event2 = setupTest.event2
        val event3 = setupTest.event3

        eventList.splitIntoParts(segmentParametersTest) shouldBe mutableListOf(
                listOf(event00,event01,event02,event03,event04higherLatLng,event05smallerLatLng,event06,event07,event08,event09),
                listOf(event1,event1,event1,event1,event1,event1,event1,event1,event1,event1),
                listOf(event2,event2,event2,event2,event2),
                listOf(event3,event3,event3,event3,event3)
        )
    }

    @Test
    fun getInterpolatedFrames_test() {
        val setupTest = SetupTest()
        val segmentParametersTest = setupTest.segmentParametersTest
        val event0list = setupTest.event0list

        val interpolated = event0list.getInterpolatedFrames(segmentParametersTest)

        interpolated.map{it.timeStamp} shouldBe listOf(1000,1050,1100,1150,1200,1250,1300,1350,1400,1450,1500,1550,1600,1650,1700,1750,1800,1850,1900)
        interpolated.map{it.latitude.roundToDecimal(7)} shouldBe listOf(52.489063,52.489063,52.489063,52.489063,52.489063,52.489063,52.489063,
                52.4890635,52.489064,52.489063,52.489062,52.4890625,
                52.489063,52.489063,52.489063,52.489063,52.489063,52.489063,52.489063)
        interpolated.map{it.longitude.roundToDecimal(7)} shouldBe listOf(13.233071,13.233071,13.233071,13.233071,13.233071,13.233071,13.233071,
                13.2330715,13.233072,13.233071,13.233070,13.2330705,
                13.233071,13.233071,13.233071,13.233071,13.233071,13.233071,13.233071)
    }
    @Test
    fun smoothenFrames_partsToSegment_test() {
        val setupTest = SetupTest()
        val segmentParametersTest = setupTest.segmentParametersTest
        val event0list = setupTest.event0list

        val smoothened = event0list.getInterpolatedFrames(segmentParametersTest).smoothenFrames(segmentParametersTest)

        smoothened.map{it.timeStamp} shouldBe setupTest.event0listSmoothenedTimeStamps
        smoothened.map{it.latitude.roundToDecimal(7)} shouldBe setupTest.event0listSmoothenedLatitudes
        smoothened.map{it.longitude.roundToDecimal(7)} shouldBe setupTest.event0listSmoothenedLongitudes

        val segment = event0list.partsToSegment(segmentParametersTest)
        segment.id.slice(0..4) shouldBe "user1"
        segment.timeStampStart shouldBe 1000
        segment.timeStampEnd shouldBe 1900
        segment.userId shouldBe "user1"
        segment.floorLvl shouldBe 0
        segment.mqtt shouldBe null

    }
    @Test
    fun gluePerformanceSplits_extractSmoothenedSegments_test() {
        val setupTest = SetupTest()
        val segmentParametersTest = setupTest.segmentParametersTest
        val event0listExtended = setupTest.event0listExtended

        val finalSegment = event0listExtended.extractSmoothenedSegments(segmentParametersTest)

        finalSegment.size shouldBe 1
        finalSegment.first().smoothedEvents.map{it.timeStamp} shouldBe setupTest.event0listSmoothenedTimeStamps + listOf(2000,2050,2200+60*60*2000)
        finalSegment.first().smoothedEvents.map{it.latitude.roundToDecimal(7)} shouldBe setupTest.event0listSmoothenedLatitudes + listOf(52.489063,52.489063,52.489063)
        finalSegment.first().smoothedEvents.map{it.longitude.roundToDecimal(7)} shouldBe setupTest.event0listSmoothenedLongitudes + listOf(13.233071,13.233071,13.233071)

    }
}

data class SetupTest(
        val latLng0: Triple<Double, Double, Double> = Triple(52.489063, 13.233071,0.0),
        val latLng0higher: Triple<Double, Double, Double> = Triple(52.489064, 13.233072,0.0),
        val latLng0smaller: Triple<Double, Double, Double> = Triple(52.489062, 13.233070,0.0),
        val latLng2: Triple<Double, Double, Double> = Triple(52.489050, 13.233099,2.0), // 2m
        val latLng4: Triple<Double, Double, Double> = Triple(52.489043, 13.233111,4.0), // 4m,
        val latLng11: Triple<Double, Double, Double> = Triple(52.488999, 13.233204,11.0), // 11m
        val latLng57: Triple<Double, Double, Double> = Triple(52.488739, 13.233731,57.0), // 57m
        val latLng100: Triple<Double, Double, Double> = Triple(52.488490, 13.234247,100.0), // 100m
        val latLng500: Triple<Double, Double, Double> = Triple(52.486404, 13.238688,500.0), // 500m

        // following setup of eventList arranges events such that all combinations of
        // (realSplitConditionFulfilled,performanceSplitConditionFulfilled)
        // ("all combinations" being (false,false),(true,false),(true,true),(false,true)
        // are achieved at least once

        val segmentParametersTest: SegmentParameters = SegmentParameters(
                100.0,
                10.0,

                60.0*60.0,
                10,

                50,
                1
        ),

        val event00: TrackingEvent = TrackingEvent("0",1000,"user1",latLng0.second,latLng0.first,tags=listOf("FL_LVL:0","new:3")),
        val event01: TrackingEvent = event00.copy(timeStamp = 1005),
        val event02: TrackingEvent = event00.copy(timeStamp = 1200),
        val event03: TrackingEvent = event00.copy(timeStamp = 1300),
        val event04higherLatLng: TrackingEvent = TrackingEvent("0",1400,"user1",latLng0higher.second,latLng0higher.first,tags=listOf("FL_LVL:0","new:3")),
        val event05smallerLatLng: TrackingEvent = TrackingEvent("0",1500,"user1",latLng0smaller.second,latLng0smaller.first,tags=listOf("FL_LVL:0","new:3")),
        val event06: TrackingEvent = event00.copy(timeStamp = 1600),
        val event07: TrackingEvent = event00.copy(timeStamp = 1795),
        val event08: TrackingEvent = event00.copy(timeStamp = 1800),
        val event09: TrackingEvent = event00.copy(timeStamp = 1900),
        val event010: TrackingEvent = event00.copy(timeStamp = 2000),
        val event011: TrackingEvent = event00.copy(timeStamp = 2050),
        val event012: TrackingEvent = event00.copy(timeStamp = 2200+60*60*2000),
        val event1: TrackingEvent = TrackingEvent("1",3000,"user1",latLng4.second,latLng4.first,tags=listOf("old:4","FL_LVL:0")),
        val event2: TrackingEvent = TrackingEvent("2",4000,"user1",latLng100.second,latLng100.first,tags=listOf("FL_LVL:3")),
        val event3: TrackingEvent = TrackingEvent("3",60*60*1000+5000,"user1",latLng4.second,latLng4.first,tags=listOf("FL_LVL:3")),

        val event0list: List<TrackingEvent> = listOf(event00,event01,event02,event03,event04higherLatLng,
                event05smallerLatLng,event06,event07,event08,event09),
        val event0listExtended: List<TrackingEvent> = listOf(event00,event01,event02,event03,event04higherLatLng,
                event05smallerLatLng,event06,event07,event08,event09,event010,event011,event012),
        val eventList:List<TrackingEvent> = event0list + listOf(
                                                    event1,event1,event1,event1,event1,event1,event1,event1,event1,event1,
                                                    event2,event2,event2,event2,event2,event3,event3,event3,event3,event3),


        val event0listSmoothenedTimeStamps: List<Int> = listOf(1000,1050,1100,1150,1200,1250,1300,1350,1400,1450,1500,1550,1600,1650,1700,1750,1800,1850,1900),
        val event0listSmoothenedLatitudes: List<Double> = listOf(52.489063,52.489063,52.489063,52.489063,52.4890632,52.489063,52.489063,
52.489063,52.489063,52.489063,52.489063,52.489063,
52.489063,52.489063,52.4890628,52.489063,52.489063,52.489063,52.489063),
        val event0listSmoothenedLongitudes: List<Double> = listOf(13.233071,13.233071,13.233071,13.233071,13.2330712,13.233071,13.233071,
                13.233071,13.233071,13.233071,13.233071,13.233071,
                13.233071,13.233071,13.2330708,13.233071,13.233071,13.233071,13.233071)
)
