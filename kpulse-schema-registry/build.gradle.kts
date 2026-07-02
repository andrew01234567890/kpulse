plugins {
    id("kpulse.java-conventions")
}

// Confluent-compatible schema registry HTTP API (milestone M8). Placeholder module for now.
dependencies {
    compileOnly(libs.slf4j.api)
    implementation(project(":kpulse-common"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
