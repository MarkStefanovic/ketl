plugins {
    kotlin("jvm") version "1.5.31"
    id("org.jmailen.kotlinter") version "3.4.5"
}

group = "me.mes"
version = "1.0-SNAPSHOT"

val kotlinVersion: String = "1.5.31"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2-native-mt")

    testImplementation("org.jetbrains.kotlin:kotlin-test:$kotlinVersion")
}

kotlinter {
    indentSize = 2
}

tasks.test {
    useJUnitPlatform()
}

tasks.compileKotlin {
    kotlinOptions.jvmTarget = "1.8"
}
