# feddi API Usage Client

Java 21 modules for sending protobuf usage reports to the feddi Platform.

## Modules

- `usage-proto`: protobuf schema and generated Java contract classes.
- `usage-client`: reactive client facade and pluggable reactive HTTP transport API.

The backend depends only on `usage-proto`. Applications that want the Java
helper client should depend on `usage-client` and provide their own
`ReactiveHttpClient` implementation.
