plugins {
    id("java")
}

group = "ca.sheridancollege.bautisse"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.hadoop:hadoop-client:3.3.6")
}

tasks.test {
    useJUnitPlatform()
}

java {
    setTargetCompatibility(8)
    setSourceCompatibility(8)
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "ca.sheridancollege.bautisse.Main"
    }
}