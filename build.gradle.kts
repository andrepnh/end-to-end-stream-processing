import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version("2.0.4")
}

group = "com.github.andrepnh"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val test by tasks.getting(Test::class) {
    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).takeIf { it > 0 } ?: 1
    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
        events(TestLogEvent.STARTED, TestLogEvent.FAILED, TestLogEvent.PASSED)
    }
}

project.tasks.withType(JavaCompile::class.java) {
    // This javac arg combined with Jackson's parameter names module allow us to avoid @JsonCreator
    options.compilerArgs.add("-parameters")
}

dependencies {
    compile("org.apache.kafka:kafka-clients:2.0.0")
    compile("com.google.guava:guava:25.1-jre")
    compile("org.eclipse.collections:eclipse-collections-api:9.2.0")
    compile("org.eclipse.collections:eclipse-collections:9.2.0")
    compile("org.apache.kafka:kafka-streams:2.0.0")
    compile("com.fasterxml.jackson.core:jackson-core:2.9.6")
    compile("org.slf4j:slf4j-simple:1.7.25")
    compile( "com.fasterxml.jackson.module:jackson-module-parameter-names:2.9.6")
    compile("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.9.6")

    testCompile("junit", "junit", "4.12")
    testCompile("org.reflections", "reflections", "0.9.11")
    testCompile("org.apache.kafka:kafka-streams-test-utils:2.0.0")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_10
}

configure<ApplicationPluginConvention> {
    mainClassName = "com.github.andrepnh.kafka.playground.Main"
}