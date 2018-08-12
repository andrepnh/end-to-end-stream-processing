plugins {
    java
}

group = "com.github.andrepnh"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compile("org.apache.kafka:kafka-clients:2.0.0")
    compile("com.google.guava:guava:25.1-jre")
    compile("org.eclipse.collections:eclipse-collections-api:9.2.0")
    compile("org.eclipse.collections:eclipse-collections:9.2.0")
    testCompile("junit", "junit", "4.12")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_10
}