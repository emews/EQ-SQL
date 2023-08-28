import dill
import codecs
import json
from typing import Dict
from proxystore.store import Store
from proxystore.connectors.file import FileConnector

store = None


def init(name, store_dir='/tmp/proxystore-dump'):
    global store
    if store is None:
        store = Store(name, FileConnector(store_dir))


def dump_proxies(**kwargs):
    proxies = {}
    for k, v in kwargs.items():
        p = store.proxy(v)
        proxies[k] = codecs.encode(dill.dumps(p), 'base64').decode()

    return proxies


def load_proxies(proxies: Dict):
    loaded_proxies = {}
    for k, v in proxies.items():
        p = dill.loads(codecs.decode(v.encode(), 'base64'))
        loaded_proxies[k] = p
    return loaded_proxies


def app(func):
    def f(*args):
        proxy_dict = json.loads(args[0])
        proxies = load_proxies(proxy_dict)
        params = json.loads(args[1])
        f_args = {**proxies, **params}
        return func(**f_args)

    return f
