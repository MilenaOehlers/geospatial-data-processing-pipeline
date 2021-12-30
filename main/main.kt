@file:Suppress("unused")
package com.tryformation.beamjobs
import io.ktor.util.*
import kotlinx.coroutines.runBlocking
import org.apache.beam.sdk.options.PipelineOptionsFactory

data class ExecutionParameters(
        // SEE DOCUMENTATION ON SLITE
        // ABOUT HOW TO OPTIMALLY SET THE SEGMENT PARAMETERS
        // FOR A DESCRIPTION OF ALL POSSIBLE PROCESSES (ESPECIALLY SPAGHETTIDIAGRAMS)
        val segmentParameters: SegmentParameters = SegmentParameters(outputFormat = OutputFormat.KibanaProduction),
        val process: Process = Process.SpaghettiDiagrams(segmentParameters.toJson())
)

// to run:
// gradle build
// java -jar build/libs/beam-jobs-0.1-all.jar <input> <output>
// it falls back to hardcoded defaults if you don't specify input and output

// to run w/ dataflow:
// in idea, select Run > Edit Configurations...
// paste following to Program arguments: --project=formation-graphqlapi --output=gs://apache_beam_output/output/ --runner=DataflowRunner --region=europe-west4
// if options --start and --end are npt given, default values will be start = now - 30minutes, end = now

// press OK and run following function as always

@KtorExperimentalAPI
fun main(args: Array<String>) {
    println("start main")
    PipelineOptionsFactory.register(ApacheBeamOptions::class.java)
    val options = PipelineOptionsFactory.fromArgs(*args).withValidation().create().`as`(ApacheBeamOptions::class.java)
    val executionParameters = ExecutionParameters()

    runPipeline(options,executionParameters)

    }