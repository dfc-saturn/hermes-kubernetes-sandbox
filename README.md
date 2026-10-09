# hermes-kubernetes-sandbox

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) terminal backend plugin that runs
each session's commands in its own Kubernetes Pod — optionally with its own Docker daemon, so
agents can run container-based test suites — instead of on the gateway host.

**Status: pre-alpha. Nothing to install yet.**

## Shape

- A `TerminalEnvironmentProvider` plugin; the provider name is `kubernetes`.
- One sandbox Pod per Hermes session, reaped when idle.
- The RuntimeClass is configuration. Docker-in-sandbox is optional, and the plugin refuses to run
  a privileged Docker daemon without a VM-isolated runtime unless explicitly told to.
- Mainstream images only: Hermes's own sandbox image for the shell, the official `docker:*-dind`
  image for the daemon.
- `examples/kata/` (planned): the Kata Containers happy path — RuntimeClass sizing, admission
  policy, network policy, RBAC and node configuration, validated by a multi-hour soak.

## License

MIT
