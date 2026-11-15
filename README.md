# Amplifier session catalog

Find Amplifier conversations and workspaces without loading an agent or reading
conversation contents. The catalog builds a searchable SQLite index from native
metadata and serves paginated results to hosts and clients.

This is a local integration component, not a chat app or a network service.
Native history remains authoritative; the catalog can be rebuilt independently.

## Get started

Use Python 3.11+, uv and Git. From a checkout:

```sh
git clone https://github.com/microsoft/amplifier-session-catalog.git
cd amplifier-session-catalog
uv sync
uv run amplifier-session-catalog serve \
  --db /absolute/private/catalog.sqlite \
  --home /absolute/native-amplifier-home --scan-on-start
```

Point `--home` at an existing native Amplifier home. Repeat it to index several
configured homes. Keep the index in a separate private location. Discovery runs
in the background; ordinary list and lookup requests use the index.

The service reads JSON-RPC 2.0 lines from standard input and writes responses to
standard output. For example:

```json
{"jsonrpc":"2.0","id":1,"method":"list","params":{"connectionId":"my-client","limit":25}}
```

Default results show root conversations in confirmed existing workspaces. Child
conversations, archived records and broader library queries are explicit choices.

## Integrate with a host

The embedding host must authorize results and supply the allowed workspace roots.
An indexed conversation is not proof that its runtime is idle or its native lock
is free. Reconnect with a new cursor, and treat incomplete discovery as incomplete.
The catalog never deletes native history or starts work.

- [Integration and stdio reference](docs/integration.md): queries, metadata updates and recovery boundaries
- [Library queries](contracts/library-query.v1.md): indexed sorting, search and observed activity
- [Catalog contract](contracts/catalog.v1.md): ownership and consumer obligations
- [Working instructions](AGENTS.md)

## Contributing

Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
