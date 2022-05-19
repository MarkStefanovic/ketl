import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask

plugins {
    kotlin("jvm") version "1.6.21"
    id("org.jmailen.kotlinter") version "3.10.0"
    id( "org.jetbrains.kotlin.plugin.serialization") version "1.6.21"
    id("com.github.ben-manes.versions") version "0.42.0"
}

group = "me.mes"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1")

    implementation("org.jetbrains.kotlin:kotlin-serialization:1.6.21")

    testImplementation("org.jetbrains.kotlin:kotlin-test:1.6.21")

    testImplementation("org.xerial:sqlite-jdbc:3.36.0.3")

    testImplementation("org.postgresql:postgresql:42.3.5")

    testImplementation("com.zaxxer:HikariCP:5.0.1")

    testImplementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.3")
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

    withType<DependencyUpdatesTask> {
      rejectVersionIf {
        isNonStable(candidate.version)
      }
    }
}

fun isNonStable(version: String): Boolean {
  val stableKeyword = listOf("RELEASE", "FINAL", "GA").any { version.toUpperCase().contains(it) }
  val regex = "^[0-9,.v-]+(-r)?$".toRegex()
  val isStable = stableKeyword || regex.matches(version)
  return isStable.not()
}
