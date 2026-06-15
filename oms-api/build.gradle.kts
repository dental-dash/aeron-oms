dependencies {
    implementation(project(":oms-common"))
    implementation(project(":oms-read-model:oms-read-model-viewserver"))
    implementation(libs.undertow.core)
    implementation(libs.jackson.databind)
    implementation(libs.gflog.api)
}
