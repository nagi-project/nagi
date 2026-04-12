# Environment Variables

Nagi runs external commands as subprocesses in three contexts: Sync stages (pre / run / post), Conditions (`type: Command`), and `dbt compile` for Origin resources. This page explains how environment variables are passed to those subprocesses.

## What Reaches the Subprocess

The subprocess environment is constructed from a platform base plus user-declared values. The parent shell's environment is not inherited directly.

### Unix

Nagi clears the parent environment and passes through only the following fixed set:

| Variable | Purpose |
| --- | --- |
| `PATH` | Executable search path |
| `HOME` | Home directory |
| `USER` | Current user name |
| `LOGNAME` | Login name |
| `LANG` | Locale setting |
| `LC_ALL` | Locale override |
| `LC_CTYPE` | Character classification locale |
| `TZ` | Timezone |
| `TMPDIR` | Temporary file directory |

This list is fixed and cannot be changed by configuration. All other parent environment variables are excluded.

### Windows

Nagi constructs the base environment using the Windows `CreateEnvironmentBlock` API. This provides the complete set of environment variables from the user's profile, including system variables and logon-synthesized variables that PowerShell and the .NET runtime require to start.

The base does not include variables that exist only in the parent shell's session. If the shell that started Nagi has custom variables set via `set` or `$env:`, those are not passed through unless declared in `env`.

If the user profile has cloud credentials registered as persistent user environment variables, those will be present in the base. To exclude them, avoid registering credentials as persistent user environment variables. Use `env` with `${VAR}` to pass credentials from the Nagi process explicitly.

## Declaring Environment Variables

Use the `env` field on Sync steps, Conditions `type: Command`, or Origin `type: DBT` to declare environment variables for the subprocess:

```yaml
kind: Sync
metadata:
  name: dbt-run
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.modelName }}"]
    env:
      GOOGLE_APPLICATION_CREDENTIALS: ${GOOGLE_APPLICATION_CREDENTIALS}
      DBT_PROFILES_DIR: /custom/profiles
```

## Referencing Parent Process Variables

To pass an environment variable from the shell that started Nagi through to a subprocess, reference it in `env` using `${VAR}` syntax:

```yaml
env:
  GOOGLE_APPLICATION_CREDENTIALS: ${GOOGLE_APPLICATION_CREDENTIALS}
  CUSTOM_PATH: ${HOME}/custom
  LITERAL_VALUE: my-fixed-value
```

### Syntax

- Only `${VAR}` syntax is supported
- `VAR` must match `[A-Za-z_][A-Za-z0-9_]*`
- Case-sensitive. `${foo}` and `${FOO}` are different variables
- Literal text and `${VAR}` references can be mixed in a single value

### Resolution

- `${VAR}` references are resolved at the moment of subprocess launch, not at YAML parse time
- If the referenced variable is not set in the Nagi process, the subprocess launch fails with an error

## Where `env` Can Be Used

| Resource | Field | Subprocess |
| --- | --- | --- |
| Sync | `spec.pre.env`<br>`spec.run.env`<br>`spec.post.env` | Sync stage command |
| Conditions (`type: Command`) | `spec[].env` | `type: Command` subprocess |
| Origin (`type: DBT`) | `spec.env` | `dbt compile` |
