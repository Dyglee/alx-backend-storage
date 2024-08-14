import redis
import uuid
from typing import Union, Callable, Optional
import functools

class Cache:
    def __init__(self):
        """Initialize the Cache with a Redis client instance and flush the database."""
        self._redis = redis.Redis()
        self._redis.flushdb()

    @functools.wraps
    def store(self, data: Union[str, bytes, int, float]) -> str:
        """
        Store the data in Redis with a randomly generated key.

        Args:
            data: The data to be stored, can be of type str, bytes, int, or float.

        Returns:
            str: The generated random key used to store the data.
        """
        key = str(uuid.uuid4())
        self._redis.set(key, data)
        return key

    def get(self, key: str, fn: Optional[Callable] = None) -> Union[str, bytes, int, float, None]:
        """
        Retrieve data from Redis and apply a conversion function if provided.

        Args:
            key: The key under which the data is stored.
            fn: A callable to convert the data back to the desired format.

        Returns:
            The data from Redis, optionally converted by fn.
        """
        data = self._redis.get(key)
        if data is not None and fn is not None:
            return fn(data)
        return data

    def get_str(self, key: str) -> Optional[str]:
        """Retrieve a string from Redis."""
        return self.get(key, lambda d: d.decode("utf-8"))

    def get_int(self, key: str) -> Optional[int]:
        """Retrieve an integer from Redis."""
        return self.get(key, lambda d: int(d))

# Decorators

def count_calls(method: Callable) -> Callable:
    """
    Decorator that counts the number of times a method is called.
    """
    key = method.__qualname__

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        self._redis.incr(key)
        return method(self, *args, **kwargs)
    
    return wrapper

def call_history(method: Callable) -> Callable:
    """
    Decorator that stores the history of inputs and outputs for a function.
    """
    key = method.__qualname__

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        input_key = f"{key}:inputs"
        output_key = f"{key}:outputs"

        self._redis.rpush(input_key, str(args))
        output = method(self, *args, **kwargs)
        self._redis.rpush(output_key, str(output))

        return output
    
    return wrapper

# Applying decorators
Cache.store = count_calls(Cache.store)
Cache.store = call_history(Cache.store)

# Replay function

def replay(method: Callable):
    """
    Display the history of calls of a particular function.
    """
    key = method.__qualname__
    redis_client = redis.Redis()

    input_key = f"{key}:inputs"
    output_key = f"{key}:outputs"

    inputs = redis_client.lrange(input_key, 0, -1)
    outputs = redis_client.lrange(output_key, 0, -1)

    print(f"{key} was called {len(inputs)} times:")

    for input_val, output_val in zip(inputs, outputs):
        print(f"{key}(*{input_val.decode('utf-8')}) -> {output_val.decode('utf-8')}")
