import yaml

def read_config():
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config


def validate_exchange(exchange):
    valid_exchanges = read_config().keys()
    return exchange in valid_exchanges


def get_base_url(exchange):
    url = read_config()[exchange]['base_url']
    return url


