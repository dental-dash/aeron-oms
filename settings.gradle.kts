rootProject.name = "oms-poc"

include(
    "oms-sbe",
    "oms-common",
    "oms-sequencer",
    "oms-event-handlers",
    "oms-read-model:oms-read-model-database",
    "oms-read-model:oms-read-model-viewserver",
    "oms-api",
    "oms-app",
    "oms-media-driver",
    "oms-fix-client-gateway:fix-acceptor",
    "oms-fix-client-gateway:fix-client",
    "oms-fix-client-gateway:fix-codecs",
    "oms-fix-client-gateway:fix-sbe",
    "oms-command-handlers:oms-aggregate-fix-order",
    "oms-command-handlers:oms-aggregate-client-order"
)
