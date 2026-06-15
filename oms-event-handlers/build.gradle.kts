dependencies {
    implementation(project(":oms-common"))
    implementation(project(":oms-fix-client-gateway:fix-sbe"))
    implementation(libs.aeron.client)
}
