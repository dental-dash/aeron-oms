plugins {
    application
}

dependencies {
    implementation(project(":oms-common"))
    implementation(project(":oms-sequencer"))
    implementation(project(":oms-command-handlers:oms-aggregate-client-order"))
    implementation(project(":oms-command-handlers:oms-aggregate-fix-order"))
    implementation(project(":oms-event-handlers"))
    implementation(project(":oms-read-model:oms-read-model-database"))
    implementation(project(":oms-read-model:oms-read-model-viewserver"))
    implementation(project(":oms-api"))
    implementation(libs.aeron.driver)
    implementation(libs.aeron.client)
    implementation(libs.aeron.archive)
    runtimeOnly(libs.gflog.core)
}

application {
    mainClass.set("com.oms.app.OmsApp")
    // Agrona's TransportPoller accesses sun.nio.ch internals via reflection on JDK 9+.
    // These opens are required for Aeron to function on the module system.
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"   // M5: Undertow XNIO
    )
}
