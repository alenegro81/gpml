import math


def cosine_similarity(vectA, vectB):
    a = dot(vectA, vectB);
    b = norm(vectA) * norm(vectB);
    if b > 0:
        return a / b;
    else:
        return 0


def dot(vect_a, vect_b):
    if vect_a is None or vect_b is None:
        return 0

    dot_value = 0.0
    x_index = 0
    y_index = 0

    while True:
        if vect_a[x_index] == vect_b[y_index]:
            dot_value += 1
            x_index += 1
            y_index += 1
        elif vect_a[x_index] > vect_b[y_index]:
            y_index += 1
        else:
            x_index += 1

        if x_index == len(vect_a) or y_index == len(vect_b):
            break

    return dot_value


def norm(vect):
    return math.sqrt(dot(vect, vect))
