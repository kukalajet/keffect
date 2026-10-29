package keffect

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class FiberTest {
    private val boom = IllegalStateException("boom")
    private val log = mutableListOf<String>()
    private fun record(line: String) = Effect.sync { log += line }

    /** Never finishes; records when it is interrupted. */
    private fun hanging(name: String) = Effect.never.onInterrupt(record("$name interrupted"))

    @Test
    fun `forked fibers are interrupted when their parent ends`() = runTest {
        effect<Any?, Nothing, Unit> {
            hanging("child").fork().bind()
            Effect.yieldNow.bind()
        }.run()
        assertEquals(listOf("child interrupted"), log)
    }

    @Test
    fun `daemon fibers outlive their parent`() = runTest {
        val fiber = effect<Any?, Nothing, Fiber<Nothing, Nothing>> {
            hanging("daemon").forkDaemon().bind().also { Effect.yieldNow.bind() }
        }.run()
        assertEquals(emptyList(), log)
        fiber.interrupt.run()
        assertEquals(listOf("daemon interrupted"), log)
    }

    @Test
    fun `scoped fibers live as long as the scope`() = runTest {
        Effect.scoped<Any?, Nothing, Unit> {
            hanging("scoped").forkScoped().bind()
            Effect.yieldNow.bind()
            record("scope ending").bind()
        }.run()
        assertEquals(listOf("scope ending", "scoped interrupted"), log)
    }

    @Test
    fun `zipPar fails fast and interrupts the sibling`() = runTest {
        val exit = Effect.fail("x").delay(10.milliseconds).zipPar(hanging("sibling")).runExit()
        assertEquals(Exit.Failure(Cause.Fail("x")), exit)
        assertEquals(listOf("sibling interrupted"), log)
    }

    @Test
    fun `zipPar keeps what the interrupted sibling reports`() = runTest {
        val exit = Effect.fail("x").delay(10.milliseconds).zipPar(Effect.never.ensuring(Effect.die(boom))).runExit()
        assertEquals(Exit.Failure(Cause.Both(Cause.Fail("x"), Cause.Then(Cause.Interrupt, Cause.Die(boom)))), exit)
    }

    @Test
    fun `concurrent forEach keeps every failure in the order they happened`() = runTest {
        for (concurrency in listOf(Concurrency.Unbounded, Concurrency(2))) {
            val exit = Effect.forEach(1..3, concurrency) { i ->
                when (i) {
                    1 -> Effect.never.ensuring(Effect.die(boom))
                    2 -> Effect.fail("x").delay(10.milliseconds)
                    else -> Effect.succeed(i)
                }
            }.runExit()
            val expected = Exit.Failure(Cause.Both(Cause.Fail("x"), Cause.Then(Cause.Interrupt, Cause.Die(boom))))
            assertEquals(expected, exit, "$concurrency")
        }
    }

    @Test
    fun `a sibling that interrupts itself interrupts zipPar`() = runTest {
        assertEquals(Exit.Failure(Cause.Interrupt), Effect.never.zipPar(Effect.interrupt).timeout(1.seconds).runExit())
    }

    @Test
    fun `race interrupts the loser`() = runTest {
        assertEquals(1, Effect.succeed(1).delay(10.milliseconds).race(hanging("loser")).run())
        assertEquals(listOf("loser interrupted"), log)
    }

    @Test
    fun `parallel children get their own fiber ids`() = runTest {
        val ids = mutableListOf<Int>()
        val logger = Logger { _, _, id -> ids += id }
        Effect.all(Effect.log("a"), Effect.log("b"), concurrency = Concurrency.Unbounded).withLogger(logger).run()
        assertEquals(2, ids.distinct().size)
    }

    @Test
    fun `bounded forEach runs at most n at a time`() = runTest {
        var running = 0
        var peak = 0
        Effect.forEach(1..6, Concurrency(2)) {
            Effect.sync { running++; peak = maxOf(peak, running) }
                .andThen(Effect.sleep(10.milliseconds))
                .andThen(Effect.sync { running-- })
        }.run()
        assertEquals(2, peak)
    }

    @Test
    fun `runExit closes the root scope with the outcome`() = runTest {
        var seen: Exit<*, *>? = null
        val exit = Effect.addFinalizer<Any?> { e -> Effect.sync { seen = e } }.andThen(Effect.fail("e")).runExit()
        assertEquals(Exit.Failure(Cause.Fail("e")), exit)
        assertEquals(exit, seen)
    }

    @Test
    fun `runExit reports interruption`() = runTest {
        var seen: Exit<*, *>? = null
        val exit = Effect.addFinalizer<Any?> { e -> Effect.sync { seen = e } }.andThen(Effect.interrupt).runExit()
        assertEquals(Exit.Failure(Cause.Interrupt), exit)
        assertEquals(exit, seen)
    }

    @Test
    fun `a failing root finalizer becomes a defect`() = runTest {
        val exit = Effect.addFinalizer<Any?> { Effect.die(boom) }.andThen(Effect.succeed(1)).runExit()
        assertEquals(Exit.Failure(Cause.Die(boom)), exit)
    }

    @Test
    fun `scoped closes its own scope before the outer one`() = runTest {
        effect<Any?, Nothing, Unit> {
            Effect.addFinalizer<Any?> { record("outer") }.bind()
            Effect.scoped(Effect.addFinalizer<Any?> { record("inner") }).bind()
            record("body").bind()
        }.run()
        assertEquals(listOf("inner", "body", "outer"), log)
    }
}
