plugins { application }

dependencies {
    implementation(libs.aeron.driver)
    implementation(libs.aeron.archive)
    implementation(libs.agrona)
    runtimeOnly(libs.gflog.core)
}

application {
    mainClass.set("com.oms.driver.OmsMediaDriverMain")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED"
    )
}
