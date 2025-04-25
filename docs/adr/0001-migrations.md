# 4. ODIN Migration Process

Date: 2025-04-23

## Status

Accepted

Open to considering to new capabilities as they come up.

## Context

We want odin to be able to execute one time "migration" operations. This process will be used for any one time operation that odin needs to execute.

  * The migration process should guarantee that actions will only be executed one time.

  * Must be able to execute individual operations in any environment. 

  * Migrations should be easy to develop and execute.


## Decision


Developed new [Super Simple Migration](../../../src/odin/migrate/process.py) process that runs at each odin application start.

## Assumptions

We need to be able to execute separate migration actions in separate application environments (dev, prod..).

Migration should be database agnostic, and store migration status in a safe location (S3).

Development of new migrations should only require the creation of a single migration file.


### Constraints

In the event of a migration failure there should be a mechanism to be notified of the failure and recover from it.


### Process


To create/run a new migration, create a new incremented python file in the folder of the ECS task/environment you want to run a migration for:
```
For migration on odin dev environment (task_name=odin-dev):
    -> odin > migrate > migrations > odin-dev > 000X.py
```

Note:
    Any files in the migration folder not following the migration file format will
    be skipped/ignored.

Inside of the 000X.py file, create a `migration()` function and accepts and returns nothing.
This is the function that will be called during the migration process.

If any exception is thrown during the migration process, the migration will be considered
to be "failed" and the ECS will enter an infinite wait state until the failure can be resolved.

If the migration completes successfully, a file will uploaded to S3 with the migration number
`000X`. Future migration attempts will skip this migration as all migrations are run in
incremental order.

## Consequences

Migrations are one-time actions that are expected to succeed. If failure occurs, ECS will enter infinte wait state and migration error must be resolved with new deployment. It is expected that application will be monitored after new migration deployment to ensure successful migration application.


## Alternatives Considered

None
