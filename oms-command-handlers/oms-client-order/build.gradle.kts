dependencies {
    implementation(project(":oms-common"))
    implementation(project(":oms-event-handlers:oms-fix-order-event"))
    implementation(project(":oms-sbe:oms-schema"))
    implementation(project(":oms-sbe:fix-schema"))  // for PlaceOrderCommand (templateId=20)
    implementation(libs.aeron.client)
    implementation(libs.aeron.archive)
    implementation(libs.dfp)
}
