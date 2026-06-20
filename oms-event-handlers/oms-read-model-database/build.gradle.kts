dependencies {
    implementation(project(":oms-common"))
    implementation(project(":oms-sbe:oms-schema"))
    implementation(libs.aeron.client)
    // TODO(POC): add H2/JDBC dependencies for DatabaseReadModel in Milestone 2+
}
