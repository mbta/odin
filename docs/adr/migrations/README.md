# Migrations

Contents:

* [Summary](#summary)
  * [Issue](#issue)
  * [Decision](#decision)
  * [Status](#status)
* [Details](#details)
  * [Assumptions](#assumptions)
  * [Constraints](#constraints)
  * [Positions](#positions)
  * [Argument](#argument)
  * [Implications ](#implications)
* [Related](#related)
  * [Related decisions](#related-decisions)
  * [Related requirements](#related-requirements)
  * [Related artifacts](#related-artifacts)
  * [Related principles](#related-principles)
* [Notes](#notes)


## Summary


### Issue

We want odin to be able to execute one time "migration" operations. This process will be used for any one time operation that odin needs to execute.

  * The migration process should guarantee that actions will only be executed one time.

  * Must be able to execute individual operations in any environment. 

  * Migrations should be easy to develop and execute.


### Decision

Develop new [Super Simple Migration](../../../src/odin/migrate/process.py) process that runs at each odin application start.


### Status

Decided. Open to considering to new capabilities as they come up.


## Details


### Assumptions

We need to be able to execute separate migration actions in separate application environments (dev, prod..).

Migration should be database agnostic, and store migration status in a safe location (S3).

Development of new migrations should only require the creation of a single migration file.


### Constraints

In the event of a migration failure there should be a mechanism to be notified of the failure and recover from it.


### Positions

The developed approach is meant to mimic alembic migrations with a simpler implementation. 
    
  * application environment separated by folder

  * migrations are executed in order by simple file naming (0001 ... 0002 ... 0003 ...)

  * migration modules are automatically imported by migration process


### Argument


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


### Implications 

It would be nice to have some way to more extensively test migrations before deployment.


## Related


### Related decisions

N/A - First decision!


### Related requirements

Any migration will need to have required devops IAM or other permissions.


### Related artifacts

Each odin process task-name will have a folder in [migrations](../../../src/odin/migrate/migrations).


### Related principles

Not easily reversible. May be impossible to test migrations before deployment.


## Notes


Example migration `/migrations/odin-dev/0001.py`:

```py
def migration() -> None:
    """Run migration on odin-dev."""
    pass
```


