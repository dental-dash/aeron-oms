dependencies {
    implementation(project(":oms-sbe:fix-schema"))
    implementation(project(":oms-common"))
    implementation(project(":oms-sbe:oms-schema"))
    implementation(libs.aeron.client)
    implementation(libs.aeron.archive)
    implementation(libs.agrona)
    implementation(libs.gflog.api)
    implementation(libs.dfp)
}
