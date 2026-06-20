dependencies {
    implementation(project(":oms-common"))
    implementation(project(":oms-sbe:oms-schema"))
    implementation(project(":oms-sbe:fix-schema"))
    implementation(libs.aeron.client)
}
