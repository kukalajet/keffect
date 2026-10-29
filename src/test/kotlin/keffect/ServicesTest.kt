package keffect

import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.testTimeSource
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// Under runTest, delays are virtual; withClock(testTimeSource) makes measured time virtual too.
class ServicesTest {
    @Test
    fun `timed measures with the provided clock`() = runTest {
        val (took, _) = Effect.sleep(5.seconds).timed().withClock(testTimeSource).run()
        assertEquals(5.seconds, took)
    }

    @Test
    fun `upTo stops retrying by the provided clock`() = runTest {
        var calls = 0
        val failing: IO<String, Nothing> = Effect.sync { calls++ }.andThen(Effect.fail("again"))
        val schedule = Schedule.spaced(300.milliseconds).upTo(1.seconds) and Schedule.recurs(10)
        failing.retry(schedule).withClock(testTimeSource).runExit()
        // Failures at t = 0, 300, 600, 900, 1200 ms; the last one is past the 1 s budget.
        assertEquals(5, calls)
    }

    @Test
    fun `repeat sees elapsed time from the provided clock`() = runTest {
        var runs = 0
        Effect.sync { runs++ }.repeat(Schedule.spaced(1.seconds).upTo(3.seconds))
            .withClock(testTimeSource).run()
        // Runs at t = 0, 1, 2, 3 s; the decision at 3 s stops.
        assertEquals(4, runs)
    }

    @Test
    fun `log goes to the provided logger`() = runTest {
        val lines = mutableListOf<Pair<String, String>>()
        Effect.log("hello").andThen(Effect.logError("oops"))
            .withLogger { level, message, _ -> lines += level to message }
            .run()
        assertEquals(listOf("INFO" to "hello", "ERROR" to "oops"), lines)
    }

    @Test
    fun `forked fibers inherit the logger`() = runTest {
        val lines = mutableListOf<String>()
        Effect.log("child").fork().flatMap { it.join }
            .withLogger { _, message, _ -> lines += message }
            .run()
        assertEquals(listOf("child"), lines)
    }

    @Test
    fun `jittered uses the provided random`() {
        val delays = { seed: Int ->
            val s = Schedule.spaced(1.seconds).jittered(0.5, Random(seed))
            (1..5).map { s.next(it, Unit, 0.seconds)!! }
        }
        assertEquals(delays(42), delays(42))
        assertTrue(delays(42).all { it in 500.milliseconds..1500.milliseconds })
    }
}
