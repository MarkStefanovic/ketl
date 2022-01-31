plugins {
    kotlin("jvm") version "1.6.10"
    id("org.jmailen.kotlinter") version "3.8.0"
    id( "org.jetbrains.kotlin.plugin.serialization") version "1.6.10"
    id("com.github.ben-manes.versions") version "0.41.0"
}

group = "me.mes"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0")

    testImplementation("org.jetbrains.kotlin:kotlin-test:1.6.10")

    testImplementation("org.xerial:sqlite-jdbc:3.36.0.3")

    testImplementation("org.postgresql:postgresql:42.3.1")

    testImplementation("com.zaxxer:HikariCP:5.0.1")

    testImplementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.2")
}

kotlinter {
    indentSize = 2
}

tasks {
    test {
        useJUnitPlatform()
    }

    compileJava {
        targetCompatibility = "16"
    }

    compileKotlin {
        kotlinOptions.jvmTarget = "16"
    }

    compileTestJava {
        targetCompatibility = "16"
    }

    compileTestKotlin {
        kotlinOptions.jvmTarget = "16"
    }
}