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

    testImplementation("org.xerial:sqlite-jdbc:3.36.0.2")

    testImplementation("org.postgresql", "postgresql", "42.2.16")

    testImplementation("com.zaxxer:HikariCP:5.0.0")
}

kotlinter {
    indentSize = 2
}

tasks.test {
    useJUnitPlatform()
}

tasks.compileJava {
    targetCompatibility = "16"
}

tasks.compileKotlin {
    kotlinOptions.jvmTarget = "16"
}

tasks.compileTestJava {
    targetCompatibility = "16"
}

tasks.compileTestKotlin {
    kotlinOptions.jvmTarget = "16"
}