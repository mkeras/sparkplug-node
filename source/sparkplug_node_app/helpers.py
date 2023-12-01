from time import time

def millis() -> int:
    return int(time() * 1000)


class Incrementor:
    def __init__(self, step: int = 1, maximum: int = None):
        self.__count = 0
        self.__min = 0
        self.__max = 4294967295 if maximum is None else maximum
        self.__step = step
        self.__previous_value = 0

    def next_value(self) -> int:
        self.__previous_value = self.__count
        if self.__count < self.__max:
            self.__count += self.__step
        else:
            self.__count = self.__min
        return self.__count

    @property
    def previous_value(self) -> int:
        return self.__previous_value
    
    @property
    def current_value(self) -> int:
        return self.__count

    def reset(self):
        self.__count = self.__min