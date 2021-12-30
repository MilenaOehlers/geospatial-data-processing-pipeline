package com.tryformation.beamjobs

import org.junit.jupiter.api.Test
import io.kotest.matchers.shouldBe
import java.time.Instant

class FunctionsTest {
    @Test
    fun interpolate_test() {
        val event0 = TrackingEvent("0",10,"user1",1.0,0.0)
        val event1 = TrackingEvent("1",30,"user1",2.0,1.0)
        val event2 = TrackingEvent("2",40,"user1",3.0,2.0)
        val eventList = listOf(event0,event1,event2)
        val interpolatedEvent = TrackingEvent("interpolated_user1_25",25,"user1",1.75,0.75)

        interpolate(25.toLong(),10.toLong(),30.toLong(),0.toLong(),1.toLong()) shouldBe 0.75
        interpolate(2.5,1.0,3.0,0.0,1.0) shouldBe 0.75
        interpolate(25.toLong(),10.toLong(),30.toLong(),listOf(0.0,1.0),listOf(1.0,2.0)) shouldBe listOf(0.75,1.75)
        interpolate(25.toLong(),event0,event1) shouldBe interpolatedEvent
        interpolate(25.toLong(),eventList) shouldBe interpolatedEvent
    }
    @Test
    fun roundToDecimal_test() {
        123456.7.roundToDecimal(-1) shouldBe 123460.0
        123.4567.roundToDecimal(-2) shouldBe 100.0
        1.234567.roundToDecimal(0) shouldBe 1.0
        1.234567.roundToDecimal(2) shouldBe 1.23
        1.234567.roundToDecimal(5) shouldBe 1.23457
    }
    @Test
    fun boxplot_test() {
        boxplot(listOf(1.0,1.5,2.5,2.0,1.5,3.5,2.5,3.0,4.5,4.5,5.0,4.0,3.5)) shouldBe listOf(1.0,2.0,3.0,4.0,5.0)
        boxplot(listOf(0.0,1.0)) shouldBe listOf(0.0,0.25,0.5,0.75,1.0)
    }
    @Test
    fun timestampFormatterFunctions_test() {
        val instant = Instant.ofEpochMilli(0)
        instantToFileTimestamp(instant) shouldBe "1970-01-01T00:00:00"
        longToFileTimeStamp(0) shouldBe "1970-01-01T00:00:00"
        epochMilliToReadable(0) shouldBe "01/01/1970 01:00:00"
    }

}
