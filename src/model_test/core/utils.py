from collections.abc import Callable

def persist_as_temp_view(func: Callable):

    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        temp_view = kwargs.get('temp_view')

        if temp_view:
            df.createOrReplaceTempView(temp_view)

        return df

    return wrapper
