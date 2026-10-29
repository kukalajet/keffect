# keffect

A small Effect-TS–style library for Kotlin, built on coroutines (~1,000 lines, one dependency: `kotlinx-coroutines-core`).

```kotlin
fun profile(id: Int): Effect<AppEnv, AppError, String> = effect {
    val user = findUser(id).bind()
    val bio = httpGet("/bio/${user.id}")
        .retry(Schedule.exponential(20.milliseconds) and Schedule.recurs(5))
        .bind()
    Effect.log("loaded ${user.name}").bind()
    "${user.name}: $bio"
}

fun main() {
    profile(1).provide(AppLive).runBlocking()
    // profile(1).runBlocking()   // does not compile: AppEnv not provided
}
```

Run the demo: `./gradlew run` (it walks through every feature, see `src/example/kotlin/demo/Main.kt`).

## The core type

```kotlin
class Effect<in R, out E, out A>
```

| | Effect-TS | keffect |
|---|---|---|
| success | `A` | `A` |
| typed error | `E` (union) | `E` (sealed hierarchy; `out` variance widens to the common parent) |
| requirements | `R` (union of Tags) | `R` (environment interface; `in` variance, effects needing nothing have `R = Any?`) |

Typealiases: `IO<E, A>` (no requirements), `UIO<A>` (no requirements, can't fail).

## API mapping

| Effect-TS | keffect |
|---|---|
| `Effect.succeed / fail / die / void` | `Effect.succeed / fail / die / unit` |
| `Effect.sync / promise` | `Effect.sync / promise` |
| `Effect.try / tryPromise` | `Effect.attempt(catch = { ... }) { ... }` |
| `Effect.suspend` | `Effect.defer` |
| `Effect.gen(function* () { yield* x })` | `effect { x.bind() }` or `Effect.gen { !x }` |
| `map / flatMap / andThen / tap / zip` | same names |
| `mapError / catchAll / catchSome / catchTag` | same; `catchTag(NotFound::class) { ... }` |
| `catchAllCause / orElse / orDie / either / exit` | same |
| `Effect.ensuring / onExit / onInterrupt / uninterruptible` | same |
| `timeout / timeoutFail / timed / delay` | same |
| `Context.Tag` + `yield* Tag` | `Has*` interfaces + `Effect.service { it.db }` / `effect { env.db }` |
| `Layer.succeed / effect / scoped` | `Layer.succeed / effect / gen` (resources via `acquireRelease`) |
| `Layer.provide`, `Layer.merge` | `layer.andThen(next)`, `a.zipWith(b) { x, y -> ... }` / `zipWithPar` |
| `Effect.provide(layer)` | `.provide(layer)` (memoized; resources released after the effect) |
| `Effect.acquireRelease / scoped / addFinalizer` | same; `acquireReleaseExit` if the release needs the `Exit` |
| `Effect.acquireUseRelease` | same |
| `Effect.fork / forkDaemon / forkScoped` | same; `fiber.join / await / interrupt` |
| `Effect.all / forEach` with `{ concurrency }` | `Effect.all(..., Concurrency(3))`, `Effect.forEach(items, Concurrency.Unbounded) { }` |
| `Effect.zip(..., { concurrent: true })` | `zipPar / zipWithPar` |
| `Effect.race / raceAll` | same |
| `Schedule.recurs / spaced / exponential / fibonacci` | same |
| `Schedule.intersect / union` | `a and b`, `a or b` |
| `Schedule.whileInput / upTo / jittered` | same, plus `capDelay`, `onDecision` |
| `Effect.retry / repeat / schedule` | same; `retry(times = 3)` |
| `Effect.runPromise / runPromiseExit` | `run() / runExit()` (suspend) |
| `Effect.runSync` | `runBlocking() / runBlockingExit()` |
| `Exit`, `Cause.Fail / Die / Interrupt` | same |
| `Effect.log` | `Effect.log` (println with fiber id) |

## How it works

* An `Effect` wraps `suspend (R) -> A`. Nothing runs until `run*`.
* Typed errors travel as an internal stackless exception and become `Cause.Fail`.
  Other exceptions are defects (`Cause.Die`); `CancellationException` is interruption.
* Fibers are coroutines. Like Effect-TS, `fork` is supervised by the parent *fiber*:
  children are interrupted when the parent finishes. `forkDaemon` outlives it.
* `Scope` and the layer memo map live in the coroutine context.

## Differences and limitations (honest list)

* **No union types.** Errors combine to their common supertype, so model them as a sealed
  hierarchy. Handling one case (`catchTag`) does not remove it from `E`.
* **Requirements are interfaces, not a type-indexed map.** Inside a function Kotlin infers
  intersections (`HasUsers & HasHttp`), but public signatures can't name them: declare an
  environment interface (`interface AppEnv : HasUsers, HasHttp`) and use it.
  Leaving the return type of a public function off can make Kotlin approximate `R` to
  `Nothing`, which makes the effect impossible to run — always annotate public effects.
* **Scope is not in the type.** `acquireRelease` uses the nearest scope at runtime (the
  `run*` root scope, `Effect.scoped`, or the `provide(layer)` scope).
* **Inside extension functions on `Effect`** a bare `delay(...)` resolves to `Effect.delay`.
  The library calls `kotlinx.coroutines.delay` fully qualified for that reason; be aware of
  it if you add operators.
* Not included: Schema, STM, streams, metrics/tracing, a test clock, `Config`.
  For production use, consider [Arrow](https://arrow-kt.io) (`Raise`, `Resource`, `Schedule`, STM),
  which covers much of this in direct style.

## Versions

Verified with Kotlin 2.0.21 + kotlinx-coroutines 1.6.4. The Gradle build uses Kotlin 2.2.21
and coroutines 1.10.2; bump them freely, the code only uses stable APIs.
# keffect
