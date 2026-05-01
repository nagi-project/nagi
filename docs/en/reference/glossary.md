# Glossary

## Core Concepts

Desired State
: A declaration of what state data should be in. Nagi evaluates data against this declaration and, when drift is detected, converges toward it.

Evaluation
: Determining whether data satisfies its desired state. The result is either Ready (in the desired state) or Drifted (drifted from the desired state).

Convergence
: The process of bringing drifted data back to its desired state.

Reconciliation Loop
: A loop that keeps data in its desired state by continuously repeating evaluation and convergence.

## Operations

Evaluate
: The operation that performs evaluation. It only determines the state of data — it does not modify data.

Sync
: The concrete action that performs convergence. Executes operations to bring data back to its desired state.

Halt
: Transitions all Assets into the Suspended state at once (`nagi serve halt`).

Resume
: Clears the Suspended state for selected Assets (`nagi serve resume`).

## Resources

Asset
: A unit of data whose desired state is declared and evaluated by Nagi. Runs the corresponding Sync when drift is detected.

Conditions
: A resource that groups desired state definitions. Can be referenced by multiple Assets.

Connection
: Connection information for a data warehouse.

Sync
: A convergence action definition. Can be referenced by multiple Assets.

Origin
: A resource that automatically generates Assets. Generates Assets from data structure information held by other software.

## Asset States

Ready
: All desired states of an Asset are satisfied.

Drifted
: The data has drifted from its desired state. The corresponding Sync is run.

Cooldown
: Sync initiation for an Asset is temporarily suppressed after a failure. The wait time increases exponentially with each consecutive failure. Cleared automatically when the timer expires or when a Sync succeeds.

Suspended
: Sync for an Asset has been stopped. Evaluate continues while Sync is stopped.

## Serve Architecture

Controller
: The execution unit that manages scheduling of Evaluate and Sync.

Guardrails
: Prevents further damage when Sync causes state degradation or fails repeatedly.

Graceful Shutdown
: On receiving a stop signal, waits for running Syncs to complete before exiting.
