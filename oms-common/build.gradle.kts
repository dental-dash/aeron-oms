dependencies {
    // api scope: Agrona, SBE codecs, and GFLogger api propagate transitively to all consumers
    api(project(":oms-sbe"))
    api(libs.agrona)
    api(libs.gflog.api)
    implementation(libs.dfp)
    runtimeOnly(libs.gflog.core)
}
