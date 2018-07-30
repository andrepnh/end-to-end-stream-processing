plugins {
    java
    id("com.avast.gradle.docker-compose") version "0.8.2"
}

group = "com.github.andrepnh"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testCompile("junit", "junit", "4.12")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_10
}

val test = getTasksByName("test", false).first() as Test

dockerCompose.isRequiredBy(test)
dockerCompose {
    val dockerHostIp = System.getenv("DOCKER_HOST_IP")
    require(!dockerHostIp.isNullOrBlank()) {"Please set environment variable DOCKER_HOST_IP; got $dockerHostIp"}
    println("Using $dockerHostIp as KAFKA_ADVERTISED_HOST_NAME")
    scale = mapOf("kakfa" to 3)
    captureContainersOutput = true
    dockerComposeWorkingDirectory = "./testbed"
    environment["KAFKA_ADVERTISED_HOST_NAME"] = dockerHostIp
}

test.doFirst {
    // exposes "${serviceName}_HOST" and "${serviceName}_TCP_${exposedPort}" environment variables
    // for example exposes "WEB_HOST" and "WEB_TCP_80" environment variables for service named `web` with exposed port `80`
    // if service is scaled using scale option, environment variables will be exposed for each service instance like "WEB_1_HOST", "WEB_1_TCP_80", "WEB_2_HOST", "WEB_2_TCP_80" and so on
    dockerCompose.exposeAsEnvironment(test)
    // exposes "${serviceName}.host" and "${serviceName}.tcp.${exposedPort}" system properties
    // for example exposes "web.host" and "web.tcp.80" system properties for service named `web` with exposed port `80`
    // if service is scaled using scale option, environment variables will be exposed for each service instance like "web_1.host", "web_1.tcp.80", "web_2.host", "web_2.tcp.80" and so on
    dockerCompose.exposeAsSystemProperties(test)
//    // get information about container of service `web` (declared in docker-compose.yml)
//    def webInfo = dockerCompose.servicesInfos.web.firstContainer
//            // in case scale option is used, dockerCompose.servicesInfos.containerInfos will contain information about all running containers of service. Particular container can be retrieved either by iterating the values of containerInfos map (key is service instance name, for example 'web_1')
//            def webInfo = dockerCompose.servicesInfos.web.'web_1'
//    // pass host and exposed TCP port 80 as custom-named Java System properties
//    systemProperty 'myweb.host', webInfo.host
//    systemProperty 'myweb.port', webInfo.ports[80]
}