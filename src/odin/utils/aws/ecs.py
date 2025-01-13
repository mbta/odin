import os

import boto3

from odin.utils.logger import ProcessLog


def running_in_aws() -> bool:
    """
    Check if running in/on AWS

    :return: True if in AWS, else False
    """
    return bool(os.getenv("AWS_DEFAULT_REGION"))


def check_for_parallel_tasks() -> None:
    """
    Check to verify that only one ECS instance is running in task group

    Will raise if another ECS is found running in the task group
    """
    if not running_in_aws():
        return

    logger = ProcessLog("check_for_parallel_tasks")

    ecs_client = boto3.client("ecs")
    cluster = os.environ["ECS_CLUSTER"]
    task_group = os.environ["ECS_TASK_GROUP"]

    # get all of the tasks running on the cluster
    task_arns = ecs_client.list_tasks(cluster=cluster)["taskArns"]

    # if tasks are running on the cluster, get their descriptions and check to
    # count matches the ecs task group.
    task_count = 0
    if task_arns:
        for task in ecs_client.describe_tasks(cluster=cluster, tasks=task_arns)["tasks"]:
            if task_group == task["group"]:
                task_count += 1

    # if the group matches, raise an exception that will terminate the process
    if task_count > 1:
        exception = SystemError(
            f"{task_count} ECS '{task_group}' tasks running in '{cluster}' cluster"
        )
        logger.failed(exception)
        raise exception

    logger.complete()
