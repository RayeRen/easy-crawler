import importlib
import os

# automatically import any Python files in the `proxy_pools` directory
for file in os.listdir(os.path.dirname(__file__)):
    if file.endswith('.py') and not file.startswith('_'):
        module = file[:file.find('.py')]
        importlib.import_module('proxy_pools.' + module)
