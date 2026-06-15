dependencies {
    implementation(project(":oms-common"))
    implementation(project(":oms-fix-client-gateway:fix-sbe"))  // for PlaceOrderCommand (templateId=20)
    implementation(libs.aeron.client)
    implementation(libs.aeron.archive)
    implementation(libs.dfp)
}
