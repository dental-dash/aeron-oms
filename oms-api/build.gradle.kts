dependencies {
    implementation(project(":oms-common"))
    implementation(project(":oms-event-handlers:oms-read-model-viewserver"))
    implementation(libs.undertow.core)
    implementation(libs.jackson.databind)
    implementation(libs.gflog.api)
}
