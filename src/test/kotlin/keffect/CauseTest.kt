package keffect

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

class CauseTest {
    private val boom = IllegalStateException("boom")

    /** Left fails first, right fails later: the race fails with both causes, in that order. */
    private val bothFail: IO<String, Int> =
        Effect.fail("L").race(Effect.fail("R").delay(20.milliseconds))

    @Test
    fun `race keeps both causes when both sides fail`() {
        assertEquals(
            Exit.Failure(Cause.Both(Cause.Fail("L"), Cause.Fail("R"))),
            bothFail.runBlockingExit(),
        )
    }

    @Test
    fun `catchAllCause sees the composite cause`() {
        val seen = bothFail.map { "$it" }.catchAllCause { Effect.succeed(it.pretty()) }
        assertEquals("Both(Fail(L), Fail(R))", seen.runBlocking())
    }

    @Test
    fun `mapError maps every failure in a composite cause`() {
        assertEquals(
            Exit.Failure(Cause.Both(Cause.Fail(1), Cause.Fail(1))),
            bothFail.mapError { it.length }.runBlockingExit(),
        )
    }

    @Test
    fun `catchAll recovers from the first failure of a composite cause`() {
        assertEquals("L", bothFail.map { "$it" }.catchAll { Effect.succeed(it) }.runBlocking())
    }

    @Test
    fun `catchAll lets defects through unchanged`() {
        val exit = Effect.die(boom).catchAll { _: Nothing -> Effect.succeed(0) }.runBlockingExit()
        assertSame(boom, ((exit as Exit.Failure).cause as Cause.Die).defect)
    }

    @Test
    fun `a defect comes out as the object that was thrown, whatever it passed through`() {
        val die: UIO<Nothing> = Effect.die(boom)
        mapOf(
            "withLogger" to die.withLogger { _, _, _ -> },
            "withClock" to die.withClock(TimeSource.Monotonic),
            "uninterruptible" to die.uninterruptible(),
            "timeout" to die.timeout(1.seconds),
            "provide" to die.provide(Layer.succeed(Unit)),
            "scoped" to Effect.scoped(die),
            "acquireRelease" to Effect.acquireRelease(die) { Effect.unit },
            "zipPar" to die.zipPar(Effect.never),
        ).forEach { (through, eff) -> assertEquals(Exit.Failure(Cause.Die(boom)), eff.runBlockingExit(), through) }
    }

    @Test
    fun `catchSome re-raises when the handler declines`() {
        val eff: IO<String, Int> = Effect.fail("nope")
        assertEquals(Exit.Failure(Cause.Fail("nope")), eff.catchSome { null }.runBlockingExit())
        assertEquals(1, eff.catchSome { Effect.succeed(1) }.runBlocking())
    }

    @Test
    fun `catchSome and tapError re-raise the whole composite cause`() {
        val seen = mutableListOf<String>()
        val both = Exit.Failure(Cause.Both(Cause.Fail("L"), Cause.Fail("R")))
        assertEquals(both, bothFail.catchSome { null }.runBlockingExit())
        assertEquals(both, bothFail.tapError { e -> Effect.sync { seen += e } }.runBlockingExit())
        assertEquals(listOf("L"), seen)
    }

    @Test
    fun `either moves typed errors into the success channel`() {
        assertEquals(Either.Left("e"), Effect.fail("e").either().runBlocking())
        assertEquals(Either.Right(1), Effect.succeed(1).either().runBlocking())
    }

    @Test
    fun `orDie turns a typed error into a defect`() {
        val exit = Effect.fail(boom).orDie().runBlockingExit()
        assertSame(boom, ((exit as Exit.Failure).cause as Cause.Die).defect)
    }

    @Test
    fun `tapError observes the error and keeps failing`() {
        val seen = mutableListOf<String>()
        val exit = Effect.fail("e").tapError { e -> Effect.sync { seen += e } }.runBlockingExit()
        assertEquals(listOf("e"), seen)
        assertEquals(Exit.Failure(Cause.Fail("e")), exit)
    }

    @Test
    fun `filterOrFail and timeoutFail fail with typed errors`() {
        assertEquals(Exit.Failure(Cause.Fail("odd")), Effect.succeed(1).filterOrFail({ it % 2 == 0 }) { "odd" }.runBlockingExit())
        assertEquals(Exit.Failure(Cause.Fail("slow")), Effect.never.timeoutFail(10.milliseconds) { "slow" }.runBlockingExit())
    }

    @Test
    fun `timeoutFail keeps a null that arrived in time`() {
        val eff: IO<String, Int?> = Effect.succeed<Int?>(null).timeoutFail(1.seconds) { "slow" }
        assertEquals(Exit.Success(null), eff.runBlockingExit())
    }

    @Test
    fun `attempt and fromNullable produce typed errors`() {
        assertEquals(Exit.Failure(Cause.Fail("boom")), Effect.attempt({ it.message!! }) { throw boom }.runBlockingExit())
        assertEquals(Exit.Failure(Cause.Fail("null")), Effect.fromNullable(null as Int?) { "null" }.runBlockingExit())
    }

    @Test
    fun `effect builder fail short-circuits`() {
        val eff: IO<String, Int> = effect { fail("stop") }
        assertEquals(Exit.Failure(Cause.Fail("stop")), eff.runBlockingExit())
    }

    @Test
    fun `retry retries typed failures but not defects`() {
        var calls = 0
        val flaky: IO<String, Int> = Effect.sync { ++calls }.flatMap { if (it < 3) Effect.fail("again") else Effect.succeed(it) }
        assertEquals(3, flaky.retry(5).runBlocking())

        calls = 0
        val dying = Effect.sync { calls++ }.andThen(Effect.die(boom)).retry(5).runBlockingExit()
        assertEquals(1, calls)
        assertSame(boom, ((dying as Exit.Failure).cause as Cause.Die).defect)
    }

    @Test
    fun `scoped finalizers see the typed failure`() {
        var released: Exit<*, *>? = null
        val eff: IO<String, Int> = Effect.scoped {
            Effect.addFinalizer<Any?> { exit -> Effect.sync { released = exit } }.bind()
            fail("e")
        }
        assertEquals(Exit.Failure(Cause.Fail("e")), eff.runBlockingExit())
        assertEquals(Exit.Failure(Cause.Fail("e")), released)
    }

    @Test
    fun `failCause round-trips a composite cause`() {
        val cause = Cause.Both(Cause.Fail("a"), Cause.Die(boom))
        assertEquals(Exit.Failure(cause), Effect.failCause(cause).runBlockingExit())
    }
}
