plugins {
    id("kpulse.java-conventions")
}

// Embedded-broker integration tests: an in-process PulsarService with the kpulse handler on the
// test classpath, driven by real org.apache.kafka.clients producers/consumers. Filled out in M1.
dependencies {
    testImplementation(project(":kpulse-protocol-handler"))
    testImplementation(project(":kpulse-common"))
    testImplementation(libs.pulsar.broker)
    testImplementation(libs.pulsar.testmocks)
    testImplementation(libs.kafka.clients)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.awaitility)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
