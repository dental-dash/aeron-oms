rootProject.name = "aeron-oms"

include(
    "oms-sbe:oms-schema",
    "oms-sbe:fix-schema",
    "oms-common",
    "oms-sequencer",
    "oms-event-handlers:oms-fix-order-event",
    "oms-event-handlers:oms-read-model-database",
    "oms-event-handlers:oms-read-model-viewserver",
    "oms-api",
    "oms-app",
    "oms-media-driver",
    "oms-fix-client",
    "oms-fix-acceptor:fix-acceptor",
    "oms-fix-acceptor:fix-codecs",
    "oms-fix-acceptor:fix-sbe",
    "oms-command-handlers:oms-fix-order",
    "oms-command-handlers:oms-client-order"
)
