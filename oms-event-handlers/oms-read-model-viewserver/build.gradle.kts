dependencies {
    implementation(project(":oms-common"))
    implementation(project(":oms-sbe:oms-schema"))
    implementation(libs.aeron.client)
    implementation(libs.aeron.archive)
}
