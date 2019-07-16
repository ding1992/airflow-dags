import re
import os


def _get_valid_kubernetes_resource_name(name):
    return re.sub(r'[^a-zA-Z0-9]+', '-', name.lower())


def _get_this_kubernetes_namespace():
    _namespace = os.environ.get('KUBERNETES_NAMESPACE', 'default')
    return _namespace
