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

dependencies {
    compile("com.google.guava:guava:25.1-jre")
    compile("org.eclipse.collections:eclipse-collections-api:9.2.0")
    compile("org.eclipse.collections:eclipse-collections:9.2.0")
    compile("org.postgresql:postgresql:42.2.4")
    compile("com.fasterxml.jackson.core:jackson-core:2.9.6")
    compile("org.slf4j:slf4j-simple:1.7.25")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_10
}

configure<ApplicationPluginConvention> {
    mainClassName = "com.github.andrepnh.kafka.playground.Main"
}