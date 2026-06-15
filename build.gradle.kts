subprojects {
    // java-library adds the `api` configuration for transitive dependency exposure.
    apply(plugin = "java-library")
    group   = "com.oms"
    version = "0.1.0-SNAPSHOT"

    repositories { mavenCentral() }

    tasks.withType<JavaCompile> {
        // JDK 21 not available on this machine; targeting 17.
        // TODO(POC): bump to 21 once JDK 21 is installed.
        sourceCompatibility = "17"
        targetCompatibility = "17"
    }
}
