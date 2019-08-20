from random import randint


def weight_random(objects, key="weight"):
    _sum = 0
    temp = []
    o = list(objects)
    for obj in objects:
        w = getattr(obj, key, 0)
        if w:
            _sum += w
            temp.append(_sum)

    rand = randint(0, _sum)
    for i in range(len(objects)):
        if temp[i] >= rand:
            return o[i]


if __name__ == '__main__':
    class O:
        def __init__(self, w):
            self.weights = w


    a = O(1)
    d = O(0)

    print(weight_random([a, d]).weights)
