import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
    application
}

group = "me.williamsakata"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven")
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:2.8.1")
    implementation("org.slf4j:slf4j-simple:1.7.32")
    implementation("org.apache.kafka:kafka-streams:3.1.0")
    implementation("io.confluent:kafka-streams-avro-serde:7.0.1")
    implementation("io.confluent:kafka-schema-registry-client:7.0.1")

    testImplementation(kotlin("test"))
    implementation(kotlin("stdlib-jdk8"))
}

tasks.test {
    useJUnit()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "1.8"
}

application {
    mainClass.set("MainKt")
}
val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "1.8"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "1.8"
}