plugins {
    application
}

dependencies {
    implementation(project(":oms-sbe:fix-schema"))
    implementation(project(":oms-sbe:oms-schema"))
    implementation(project(":oms-command-handlers:oms-fix-order"))
    implementation(project(":oms-common"))
    implementation(libs.artio.core)
    implementation(libs.artio.session.codecs)
    implementation(libs.aeron.client)
    implementation(libs.aeron.driver)
    implementation(libs.aeron.archive)
    implementation(libs.agrona)
    implementation(libs.gflog.api)
}

application {
    mainClass.set("com.oms.fix.acceptor.FixAcceptorMain")
    // Artio / Aeron require the same JDK opens as the main OMS app.
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}
