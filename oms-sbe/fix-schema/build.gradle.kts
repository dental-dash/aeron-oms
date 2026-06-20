val sbeCodegen: Configuration by configurations.creating

dependencies {
    sbeCodegen(libs.sbe.tool)
    // SBE generated code references Agrona buffers
    implementation(libs.agrona)
    implementation(libs.gflog.api)
    implementation(libs.dfp)
}

val sbeOutputDir = layout.buildDirectory.dir("generated/sources/sbe/main/java")

tasks.register<JavaExec>("generateSbeCodecs") {
    group       = "generate"
    description = "Generate FIX SBE codec classes from fix-messages.xml"
    classpath   = sbeCodegen
    mainClass.set("uk.co.real_logic.sbe.SbeTool")

    val schemaFile = file("src/main/resources/fix-messages.xml")
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
