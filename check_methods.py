from SmartApi import SmartConnect
import inspect

s = SmartConnect(api_key="test")
methods = inspect.getmembers(s, predicate=inspect.ismethod)
for name, _ in methods:
    if 'login' in name.lower() or 'session' in name.lower():
        print(name)
