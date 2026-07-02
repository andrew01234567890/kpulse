pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
    includeBuild("build-logic")
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

rootProject.name = "kpulse"

include(
    "kpulse-common",
    "kpulse-protocol-handler",
    "kpulse-schema-registry",
    "kpulse-tests",
)
