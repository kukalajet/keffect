package demo

import keffect.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// ============================================================================
// 1. Errors: a sealed hierarchy stands in for TypeScript's error unions.
// ============================================================================

sealed interface AppError
data class UserNotFound(val id: Int) : AppError
data class NetworkError(val url: String, val attempt: Int) : AppError
data object Timeout : AppError

data class User(val id: Int, val name: String)

// ============================================================================
// 2. Services. Each service is an interface; each "slot" in the environment is
//    a Has* interface. This plays the role of Context.Tag.
// ============================================================================

class Config(val dbUrl: String, val flakyFailures: Int)

class Connection(val url: String) {
    fun select(id: Int): User? = mapOf(1 to "Ada", 2 to "Grace", 3 to "Linus", 4 to "Barbara", 5 to "Edsger")[id]
        ?.let { User(id, it) }
}

interface UserRepo {
    fun find(id: Int): IO<UserNotFound, User>
}

interface Http {
    fun get(url: String): IO<NetworkError, String>
}

interface HasUsers { val users: UserRepo }
interface HasHttp { val http: Http }
interface AppEnv : HasUsers, HasHttp

// Accessors — like `const users = yield* UserRepo`
fun findUser(id: Int): Effect<HasUsers, UserNotFound, User> =
    Effect.serviceWithEffect { it.users.find(id) }

fun httpGet(url: String): Effect<HasHttp, NetworkError, String> =
    Effect.serviceWithEffect { it.http.get(url) }

// ============================================================================
// 3. Programs, written with the gen-style builder.
// ============================================================================

val retryPolicy: Schedule<AppError> =
    (Schedule.exponential(20.milliseconds) and Schedule.recurs(5))
        .whileInput { it is NetworkError }          // only retry transient errors
        .onDecision { n, e, d -> println("    retry #$n after $e (waiting $d)") }

fun profile(id: Int): Effect<AppEnv, AppError, String> = effect {
    val user = findUser(id).bind()
    val bio = httpGet("https://example.com/bio/${user.id}").retry(retryPolicy).bind()
    Effect.log("loaded profile for ${user.name}").bind()
    "${user.name} — $bio"
}

// Requirements combine through variance. Inside a function body Kotlin infers the
// environment as the intersection `HasUsers & HasHttp`. Intersections can't be written
// in Kotlin signatures, so public declarations name a type that has both (AppEnv).
fun userAndHomepage(id: Int): Effect<AppEnv, AppError, Pair<User, String>> =
    findUser(id).zipPar(httpGet("https://example.com/"))

// ============================================================================
// 4. Layers: how services are built, with dependencies and resources.
// ============================================================================

val ConfigLive: Layer<Any?, Nothing, Config> = Layer.effect(
    Effect.log("building Config (should happen once)").asValue(Config("jdbc:demo", flakyFailures = 2)),
    name = "Config",
)

val ConnectionLive: Layer<Config, Nothing, Connection> = Layer.effect(
    Effect.acquireRelease(
        acquire = effect<Config, Nothing, Connection> {
            Effect.log("opening connection to ${env.dbUrl}").bind()
            Connection(env.dbUrl)
        },
        release = { conn -> Effect.log("closing connection to ${conn.url}") },
    ),
    name = "Connection",
)

val UserRepoLive: Layer<Connection, Nothing, UserRepo> = Layer.gen("UserRepo") {
    val conn = env
    object : UserRepo {
        override fun find(id: Int): IO<UserNotFound, User> =
            Effect.sync { conn.select(id) }.flatMap { Effect.fromNullable(it) { UserNotFound(id) } }
    }
}

val HttpLive: Layer<Config, Nothing, Http> = Layer.gen("Http") {
    val calls = ConcurrentHashMap<String, AtomicInteger>()
    val failures = env.flakyFailures
    object : Http {
        override fun get(url: String): IO<NetworkError, String> = effect {
            val n = calls.getOrPut(url) { AtomicInteger() }.incrementAndGet()
            Effect.sleep(30.milliseconds).bind()
            ensure(n > failures) { NetworkError(url, n) }   // flaky: first calls fail
            "bio from $url"
        }
    }
}

val AppLive: Layer<Any?, Nothing, AppEnv> =
    ConfigLive.andThen(ConnectionLive).andThen(UserRepoLive)
        .zipWith(ConfigLive.andThen(HttpLive)) { repo, client ->
            object : AppEnv {
                override val users = repo
                override val http = client
            }
        }

// ============================================================================
// 5. Running it.
// ============================================================================

fun section(title: String) = println("\n=== $title ===")

fun main() {
    section("1. Typed errors + retry + layers (resources released at the end)")
    val p1 = profile(1).provide(AppLive)
    println("result: ${p1.runBlocking()}")

    // profile(1).runBlocking()   // <- does not compile: AppEnv is not provided

    section("2. catchTag: recover from one error type")
    val p2 = profile(42)
        .catchTag(UserNotFound::class) { e -> Effect.succeed("guest (no user ${e.id})") }
        .provide(AppLive)
    println("result: ${p2.runBlocking()}")

    section("3. either(): errors as values")
    println("result: ${profile(42).either().provide(AppLive).runBlocking()}")

    section("4. Effect.forEach with bounded concurrency")
    val p4 = Effect.forEach(1..5, Concurrency(3)) { id -> profile(id) }.timed().provide(AppLive)
    val (took, profiles) = p4.runBlocking()
    profiles.forEach { println("  $it") }
    println("took ~${took.inWholeMilliseconds} ms")

    section("5. Requirements combine: HasUsers & HasHttp")
    val local = findUser(3).zipPar(httpGet("https://example.com/"))   // inferred: HasUsers & HasHttp
    println("local:    ${local.retry(3).provide(AppLive).runBlocking()}")
    println("declared: ${userAndHomepage(2).retry(3).provide(AppLive).runBlocking()}")

    section("6. timeoutFail")
    val slow: IO<AppError, String> = Effect.sleep(2.seconds).asValue("too late")
    println("exit: ${slow.timeoutFail(100.milliseconds) { Timeout }.runBlockingExit()}")

    section("7. race: the loser is interrupted")
    val tortoise = Effect.sleep(1.seconds).asValue("tortoise")
        .onInterrupt(Effect.log("tortoise was interrupted"))
    val hare = Effect.sleep(50.milliseconds).asValue("hare")
    println("winner: ${tortoise.race(hare).runBlocking()}")

    section("8. Fibers: fork, interrupt, finalizers")
    val ticker = Effect.log("tick").repeat(Schedule.spaced(40.milliseconds))
        .ensuring(Effect.log("ticker finalizer ran"))
    val p8 = effect<Any?, Nothing, Exit<Nothing, Unit>> {
        val fiber = ticker.fork().bind()
        Effect.sleep(130.milliseconds).bind()
        fiber.interrupt.bind()
    }
    println("ticker exit: ${p8.runBlocking()}")

    section("9. Child fibers are interrupted when the parent ends")
    val parent = effect<Any?, Nothing, String> {
        Effect.log("child running").repeat(Schedule.spaced(30.milliseconds))
            .onInterrupt(Effect.log("child interrupted because parent finished"))
            .fork().bind()
        Effect.sleep(70.milliseconds).bind()
        "parent done"
    }
    println("result: ${parent.runBlocking()}")

    section("10. Defects vs typed failures")
    val boom: IO<AppError, Int> = Effect.sync { error("boom") }
    println("defect exit: ${boom.runBlockingExit().let { (it as Exit.Failure).cause.pretty() }}")
    println("typed exit:  ${Effect.fail(Timeout).runBlockingExit()}")
    println("catchAll does not catch defects: " +
        boom.catchAll { Effect.succeed(0) }.runBlockingExit().let { (it as Exit.Failure).cause.pretty() })

    section("11. Scoped resources outside layers")
    val p11 = Effect.scoped<Any?, Nothing, String> {
        val a = Effect.acquireRelease(Effect.log("acquire A").asValue("A")) { Effect.log("release A") }.bind()
        val b = Effect.acquireRelease(Effect.log("acquire B").asValue("B")) { Effect.log("release B") }.bind()
        Effect.log("using $a and $b").bind()
        a + b
    }
    println("result: ${p11.runBlocking()}")
}
