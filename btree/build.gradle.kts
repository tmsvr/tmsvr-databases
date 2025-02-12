group = "com.tmsvr.databases"
version = "1.0-SNAPSHOT"

dependencies {
    implementation(project(":interfaces"))

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}