dependencies {
    implementation(project(":oms-common"))
    implementation(libs.aeron.client)
    // TODO(POC): add H2/JDBC dependencies for DatabaseReadModel in Milestone 2+
}
