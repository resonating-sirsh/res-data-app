from res.flows.kafka import kafka_consumer_worker, KafkaConsumerWorker
from pytest_mock.plugin import MockerFixture


def test_worker_class(capsys, mocker):
    def example_event_handler(message: dict):
        print(f'hello world: {message["message"]}')

    mocker.patch("res.flows.kafka.ResKafkaClient")
    patch_consumer = mocker.patch("res.flows.kafka.ResKafkaConsumer")
    mocker.patch("res.flows.kafka.FlowContext")
    patch_consumer.return_value.poll_avro.return_value = {"message": "resonance"}

    # patch _true so that it only returns true on the first call
    patch_true = mocker.patch("res.flows.kafka._true")
    patch_true.side_effect = [True, False]

    worker = KafkaConsumerWorker("path.to.kafka.topic", example_event_handler)
    worker.run()
    captured = capsys.readouterr()
    assert "hello world: resonance\n" in captured.out


def test_decorator(capsys, mocker):
    @kafka_consumer_worker("path.to.kafka.topic")
    def example_event_handler(message: dict):
        print(f'hello world: {message["message"]}')

    mocker.patch("res.flows.kafka.ResKafkaClient")
    patch_consumer = mocker.patch("res.flows.kafka.ResKafkaConsumer")
    mocker.patch("res.flows.kafka.FlowContext")
    patch_consumer.return_value.poll_avro.return_value = {"message": "resonance"}

    # patch _true so that it only returns true on the first call
    patch_true = mocker.patch("res.flows.kafka._true")
    patch_true.side_effect = [True, False]

    example_event_handler()
    captured = capsys.readouterr()
    assert "hello world: resonance\n" in captured.out
