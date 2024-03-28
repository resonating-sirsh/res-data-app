from task_scheduler import scheduler


def run(event, context):
    """
    A wrapper around the kafka relay method in flows, which can also be run on a schedule
    Everything below is just boilerplate for the long-running stream processor and can later be abstracted
        For example
        ```
        with FlowContext(event, context) as fc:
            fc.streaming(relay_batch_events, offset, limit)
        ```
    Planning to make a simple dispatcher that consumes some control messages or triggers tasks from a scheduler

    Times rob edited this comment to deploy the app: 7

    ... ... ... ... ... ... ... ...
    """

    scheduler.start()


if __name__ == "__main__":
    run({}, {})
