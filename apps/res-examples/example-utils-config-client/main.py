import os
from res.utils import logger
from res.utils.configuration.ResConfigClient import ResConfigClient

if __name__ == "__main__":
    config_client = ResConfigClient()
    config_name = os.getenv("CONFIG_TO_ADD", "test123")
    logger.info(
        "Existing configurations: {}".format(
            str(config_client.get_config_value(config_name))
        )
    )
    logger.info("Adding configuration: {} with sample value: test")
    config_client.set_config_value(config_name, "test")
    logger.info(
        "New configurations: {}".format(
            str(config_client.get_config_value(config_name))
        )
    )
    logger.info("Changing value to an array...")
    config_client.set_config_value(config_name, ["a", {"b": 1}])
    logger.info(
        "Updated configurations: {}".format(
            str(config_client.get_config_value(config_name))
        )
    )
