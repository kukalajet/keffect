package keffect

import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.testTimeSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class ScheduleTest {
    private var runs = 0
    private val count = Effect.sync { ++runs }

    @Test
    fun `repeat runs once plus one per recurrence`() = runTest {
        assertEquals(3, count.repeat(Schedule.recurs(2)).run())
    }

    @Test
    fun `retry gives up with the last error after the recurrences`() = runTest {
        val exit = count.flatMap { Effect.fail("attempt $it") }.retry(Schedule.recurs(2)).runExit()
        assertEquals(Exit.Failure(Cause.Fail("attempt 3")), exit)
    }

    @Test
    fun `schedule waits before each run`() = runTest {
        count.schedule(Schedule.spaced(1.seconds).upTo(3.seconds)).withClock(testTimeSource).run()
        // Runs at t = 1, 2, 3 s; the decision at 3 s stops.
        assertEquals(3, runs)
    }

    @Test
    fun `schedules see 1-based attempts and the latest input`() = runTest {
        val seen = mutableListOf<Pair<Int, Int>>()
        count.repeat(Schedule.recurs(2).onDecision { n, input, _ -> seen += n to input }).run()
        assertEquals(listOf(1 to 1, 2 to 2, 3 to 3), seen)
    }
}

class LayerTest {
    @Test
    fun `a layer shared in the graph is built once per provide`() = runTest {
        var builds = 0
        val shared: Layer<Any?, Nothing, Int> = Layer.effect(Effect.sync { ++builds })
        val both = shared.zipWith(shared.map { it * 10 }) { a, b -> a + b }
        assertEquals(11, Effect.environment<Int>().provide(both).run())
        assertEquals(1, builds)
    }

    @Test
    fun `layer resources are released when the provided effect ends`() = runTest {
        val log = mutableListOf<String>()
        val layer = Layer.effect(Effect.acquireRelease(Effect.sync { log += "acquire"; 1 }) { Effect.sync { log += "release" } })
        Effect.environment<Int>().tap { Effect.sync { log += "use $it" } }.provide(layer).run()
        assertEquals(listOf("acquire", "use 1", "release"), log)
    }
}
