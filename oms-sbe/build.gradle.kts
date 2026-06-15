val sbeCodegen: Configuration by configurations.creating

dependencies {
    sbeCodegen(libs.sbe.tool)
    // SBE generated code references Agrona buffers — must be on compile classpath
    implementation(libs.agrona)
    implementation(libs.dfp)
}

val sbeOutputDir = layout.buildDirectory.dir("generated/sources/sbe/main/java")

tasks.register<JavaExec>("generateSbeCodecs") {
    group       = "generate"
    description = "Generate SBE codec classes from oms-messages.xml"
    classpath   = sbeCodegen
    mainClass.set("uk.co.real_logic.sbe.SbeTool")

    val schemaFile = file("src/main/resources/oms-messages.xml")
    inputs.file(schemaFile)
    outputs.dir(sbeOutputDir)

    systemProperty("sbe.output.dir",               sbeOutputDir.get().asFile.absolutePath)
    systemProperty("sbe.target.language",           "Java")
    systemProperty("sbe.java.generate.interfaces",  "true")

    args(schemaFile.absolutePath)
    doFirst { sbeOutputDir.get().asFile.mkdirs() }
}

sourceSets.main {
    java.srcDir(sbeOutputDir)
}

tasks.compileJava { dependsOn("generateSbeCodecs") }
