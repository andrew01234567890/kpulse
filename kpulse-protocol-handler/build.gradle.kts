plugins {
    id("kpulse.java-conventions")
    alias(libs.plugins.nar)
}

dependencies {
    // The Pulsar broker provides these at runtime via its own classloader — compile against
    // them but do NOT bundle them in the NAR.
    compileOnly(libs.pulsar.broker)
    compileOnly(libs.slf4j.api)

    // kafka-clients carries the Kafka wire protocol schemas + record codec. It IS bundled inside
    // the NAR; the isolated NarClassLoader keeps it from colliding with the broker's classpath.
    implementation(libs.kafka.clients)
    implementation(project(":kpulse-common"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.pulsar.broker)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Keep Pulsar platform modules, BookKeeper, protobuf, and slf4j out of the NAR's
// bundled-dependencies — the broker supplies them. Mirrors Pulsar 5.0's nar-conventions.
val pulsarPlatformModules = listOf(
    "pulsar-client-api", "pulsar-client-admin-api", "pulsar-client", "pulsar-client-original",
    "pulsar-common", "pulsar-config-validation", "pulsar-functions-api", "pulsar-functions-instance",
    "pulsar-functions-proto", "pulsar-functions-secrets", "pulsar-functions-utils",
    "pulsar-io-core", "pulsar-metadata", "pulsar-opentelemetry", "managed-ledger", "pulsar-package-core",
)

configurations.named("runtimeClasspath") {
    exclude(group = "org.apache.bookkeeper")
    exclude(group = "com.google.protobuf")
    exclude(group = "org.slf4j")
    pulsarPlatformModules.forEach { exclude(group = "org.apache.pulsar", module = it) }
}
