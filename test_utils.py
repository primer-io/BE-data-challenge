"""
Test cases for join functions
"""

from utils import merge_join, hash_join, nested_loop_join

# Tests if the join returns the correct number of rows
input_test_0 = {
    "employees": [
        {"id": 1, "user_id": 303, "name": "Mike"},
        {"id": 2, "user_id": 304, "name": "James"},
        {"id": 3, "user_id": 305, "name": "David"},
    ],
    "roles": [
        {"id": 1, "user_id": 303, "role": "Manager"},
        {"id": 2, "user_id": 304, "role": "Assistant"},
        {"id": 3, "user_id": 305, "role": "Assistant"},
    ],
    "key": "user_id",
    "expected": 3,
}

# Tests one-to-many case
input_test_1 = {
    "employees": [
        {"id": 1, "user_id": 303, "name": "Mike"},
        {"id": 2, "user_id": 304, "name": "James"},
        {"id": 3, "user_id": 305, "name": "David"},
    ],
    "roles": [
        {"id": 1, "user_id": 303, "role": "Manager"},
        {"id": 2, "user_id": 304, "role": "Assistant"},
        {"id": 3, "user_id": 305, "role": "Assistant"},
        {"id": 4, "user_id": 303, "role": "Interim Director"},
    ],
    "key": "user_id",
    "expected": 4,
}

# Tests many-to-one case
input_test_2 = {
    "absences": [
        {"id": 1, "user_id": 303, "date": "2015-03-01"},
        {"id": 2, "user_id": 303, "date": "2015-03-02"},
        {"id": 3, "user_id": 303, "date": "2015-03-03"},
        {"id": 4, "user_id": 304, "date": "2015-03-15"},
        {"id": 5, "user_id": 305, "date": "2015-03-19"},
    ],
    "employees": [
        {"id": 1, "user_id": 303, "name": "Mike"},
        {"id": 2, "user_id": 304, "name": "James"},
        {"id": 3, "user_id": 305, "name": "David"},
    ],
    "key": "user_id",
    "expected": 5,
}

# Test the correctness of the join
input_test_3 = {
    "employees": [
        {"id": 1, "user_id": 303, "name": "Mike"},
        {"id": 2, "user_id": 304, "name": "James"},
        {"id": 3, "user_id": 305, "name": "David"},
    ],
    "roles": [
        {"id": 1, "user_id": 303, "role": "Manager"},
        {"id": 2, "user_id": 304, "role": "Assistant"},
        {"id": 3, "user_id": 305, "role": "Assistant"},
    ],
    "key": "user_id",
    "expected": {"id": 1, "user_id": 303, "name": "Mike", "role": "Manager"},
}

# Tests when one of the dictionary is null
input_test_4 = {
    "employees": None,
    "roles": [
        {"id": 1, "user_id": 303, "role": "Manager"},
        {"id": 2, "user_id": 304, "role": "Assistant"},
        {"id": 3, "user_id": 305, "role": "Assistant"},
    ],
    "key": "user_id",
    "expected": []
}


def test_nested_loop_0():
    assert (
        len(
            list(
                nested_loop_join(
                    input_test_0["employees"],
                    input_test_0["roles"],
                    input_test_0["key"],
                )
            )
        )
        == input_test_0["expected"]
    )


def test_nested_loop_1():
    assert (
        len(
            list(
                nested_loop_join(
                    input_test_1["employees"],
                    input_test_1["roles"],
                    input_test_1["key"],
                )
            )
        )
        == input_test_1["expected"]
    )


def test_nested_loop_2():
    assert (
        len(
            list(
                nested_loop_join(
                    input_test_2["absences"],
                    input_test_2["employees"],
                    input_test_2["key"],
                )
            )
        )
        == input_test_2["expected"]
    )


def test_nested_loop_3():
    output = list(
        nested_loop_join(
            input_test_3["employees"], input_test_3["roles"], input_test_3["key"]
        )
    )
    assert output[0] == input_test_3["expected"]

def test_nested_loop_4():
    output = list(
        nested_loop_join(input_test_4["employees"], input_test_4["roles"], input_test_4["key"])
    )
    assert output == input_test_4["expected"]

def test_merge_0():
    assert (
        len(
            list(
                merge_join(
                    input_test_0["employees"],
                    input_test_0["roles"],
                    input_test_0["key"],
                )
            )
        )
        == input_test_0["expected"]
    )


def test_merge_1():
    assert (
        len(
            list(
                merge_join(
                    input_test_1["employees"],
                    input_test_1["roles"],
                    input_test_1["key"],
                )
            )
        )
        == input_test_1["expected"]
    )


def test_merge_2():
    assert (
        len(
            list(
                merge_join(
                    input_test_2["absences"],
                    input_test_2["employees"],
                    input_test_2["key"],
                )
            )
        )
        == input_test_2["expected"]
    )


def test_merge_3():
    output = list(
        merge_join(
            input_test_3["employees"], input_test_3["roles"], input_test_3["key"]
        )
    )
    assert output[0] == input_test_3["expected"]

def test_merge_4():
    output = list(
        merge_join(input_test_4["employees"], input_test_4["roles"], input_test_4["key"])
    )
    assert output == input_test_4["expected"]


def test_hash_0():
    assert (
        len(
            list(
                hash_join(
                    input_test_0["employees"],
                    input_test_0["roles"],
                    input_test_0["key"],
                )
            )
        )
        == input_test_0["expected"]
    )


def test_hash_1():
    assert (
        len(
            list(
                hash_join(
                    input_test_1["employees"],
                    input_test_1["roles"],
                    input_test_1["key"],
                )
            )
        )
        == input_test_1["expected"]
    )


def test_hash_2():
    assert (
        len(
            list(
                hash_join(
                    input_test_2["absences"],
                    input_test_2["employees"],
                    input_test_2["key"],
                )
            )
        )
        == input_test_2["expected"]
    )


def test_hash_3():
    output = list(
        hash_join(input_test_3["employees"], input_test_3["roles"], input_test_3["key"])
    )
    assert output[0] == input_test_3["expected"]

def test_hash_4():
    output = list(
        hash_join(input_test_4["employees"], input_test_4["roles"], input_test_4["key"])
    )
    assert output == input_test_4["expected"]