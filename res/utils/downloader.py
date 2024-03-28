# Create a content downlaoder queue (RabbitMQ??)
# This is something that other processes can make requests of
# for example we have content on box and airtable.dl that we would like to fetch
# jobs would be {source_url=,tarket_url, info=}
# this just queues up downloads from one place and puts them in another place (s3)
# we can monoitor the queue, check that we have not already downloaded the items etc.
# the target destination should be field_id/_record_id_[name].ext


def enqueue(task):
    pass


def dequeue(task):
    pass


def perform_downloads(task_or_tasks, n=None):
    if isinstance(task_or_tasks, dict):
        task_or_tasks = [task_or_tasks]

    # for task in task_or_tasks:
    #     provider = resolve_source_provider(task)
    #     with provider.open(task['source_url']) as f:
    #         with s3_provider(task['tarkget_url']) as g:
    #             f.write(f.read())
