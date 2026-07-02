plugins {
    id("kpulse.java-conventions")
    application
}

// Real Kafka producer/consumer CLI staged into the e2e broker image (see e2e/Dockerfile) and driven
// via `kubectl exec` for the KIND e2e round-trip test — the deployed broker only advertises
// 127.0.0.1:9092, so the client must run inside the broker pod's network namespace.
application {
    mainClass.set("dev.kpulse.e2e.KafkaRoundTripClient")
}

dependencies {
    implementation(libs.kafka.clients)
}
