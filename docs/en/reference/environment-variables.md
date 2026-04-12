# Environment Variables

Nagi runs external commands as subprocesses in three contexts: Sync stages (pre / run / post), Conditions (`type: Command`), and `dbt compile` for Origin resources. This page explains how environment variables are passed to those subprocesses.

## What Reaches the Subprocess

A subprocess receives environment variables from exactly two sources:

- OS essentials that Nagi inherits from the parent process. This is a fixed set per platform
- Values declared in the `env` field of the YAML resource

Environment variables outside these two sources are not passed to the subprocess, even if they exist in the shell that started Nagi.

### OS Essentials

Nagi passes through a minimal set of parent environment variables that are required for basic OS operation:

#### Unix

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

#### Windows

| Variable | Purpose |
| --- | --- |
| `SystemRoot` | Windows system directory |
| `SystemDrive` | System drive letter |
| `ComSpec` | Command processor path |
| `PATH` | Executable search path |
| `PATHEXT` | Executable file extensions |
| `USERPROFILE` | User profile directory |
| `HOMEDRIVE` | Home drive letter |
| `HOMEPATH` | Home directory path |
| `APPDATA` | Application data directory |
| `LOCALAPPDATA` | Local application data directory |
| `TEMP` | Temporary file directory |
| `TMP` | Temporary file directory |
| `ProgramData` | Shared application data |
| `ProgramFiles` | Program installation directory |
| `ProgramFiles(x86)` | 32-bit program installation directory |
| `CommonProgramFiles` | Shared program components |
| `CommonProgramFiles(x86)` | 32-bit shared program components |
| `COMPUTERNAME` | Machine name |
| `NUMBER_OF_PROCESSORS` | Number of processors |
| `PROCESSOR_ARCHITECTURE` | Processor architecture |

This list is fixed and cannot be changed by configuration. If you need to pass additional environment variables to a subprocess, declare them in the `env` field.

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
