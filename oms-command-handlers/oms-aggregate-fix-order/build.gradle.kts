dependencies {
    implementation(project(":oms-fix-client-gateway:fix-sbe"))
    implementation(project(":oms-sbe"))
    implementation(libs.aeron.client)
    implementation(libs.agrona)
    implementation(libs.gflog.api)
    implementation(libs.dfp)
}
