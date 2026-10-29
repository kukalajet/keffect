plugins {
    kotlin("jvm") version "2.2.21"
    application
}

group = "keffect"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}

tasks.test {
    useJUnitPlatform()
}

// The demo lives next to the library so `./gradlew run` just works.
sourceSets {
    main {
        kotlin.srcDir("src/example/kotlin")
    }
}

application {
    mainClass.set("demo.MainKt")
}
