import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val exposedVersion = "0.35.1"

plugins {
    kotlin("jvm") version "1.5.21"
    id("org.jmailen.kotlinter") version "3.4.5"
}

group = "me.mes"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.jetbrains.kotlin:kotlin-test:1.5.21")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2-native-mt")

    // database
    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-java-time:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")
    implementation("org.xerial:sqlite-jdbc:3.36.0.2")
    implementation("org.slf4j:slf4j-nop:1.7.32")
    implementation("com.zaxxer:HikariCP:5.0.0")
}

kotlinter {
    indentSize = 2
}

tasks.check {
    dependsOn("installKotlinterPrePushHook")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
