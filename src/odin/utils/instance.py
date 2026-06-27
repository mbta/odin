import os

from odin.utils.aws.ecs import running_in_aws

ODIN_INSTANCES = ("alpha", "beta", "gamma")


def get_odin_instance() -> str:
    """
    Return the configured Odin instance name.

    For local development, default to the alpha assignment when the environment
    variable is not set.
    """
    instance = os.getenv("ODIN_INSTANCE")
    if instance is None:
        if not running_in_aws():
            return "alpha"
        raise RuntimeError(f"Expected ODIN_INSTANCE to be set to one of {ODIN_INSTANCES}.")

    if instance not in ODIN_INSTANCES:
        raise RuntimeError(
            f"Unsupported ODIN_INSTANCE={instance!r}. Expected one of {ODIN_INSTANCES}."
        )

    return instance
