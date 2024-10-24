from typing import List, Dict, Set, Tuple
import hashlib
import json


class ValidationResult:
    def __init__(
        self, is_valid: bool, differences: List[str], statistics: Dict[str, int]
    ):
        self.is_valid = is_valid
        self.differences = differences
        self.statistics = statistics

    def print_report(self):
        """Print a formatted validation report"""
        print("\nValidation Report ")
        print(f"Overall Status: {'PASSED' if self.is_valid else 'FAILED'}")

        print("\nStatistics:")
        for key, value in self.statistics.items():
            print(f"{key}: {value}")

        if self.differences:
            print("\nDiscrepancies Found:")
            for diff in self.differences:
                print(f"- {diff}")


def generate_record_hash(record: Dict[str, str]) -> str:
    sorted_items = sorted(record.items())
    record_str = json.dumps(sorted_items)
    return hashlib.md5(record_str.encode()).hexdigest()


def validate_migration(
    source_data: List[Dict[str, str]], target_data: List[Dict[str, str]]
) -> ValidationResult:
    differences = []
    statistics = {}

    statistics["Source Records"] = len(source_data)
    statistics["Target Records"] = len(target_data)

    if len(source_data) != len(target_data):
        differences.append(
            f"Record count mismatch: Source={len(source_data)}, Target={len(target_data)}"
        )
    source_hashes = {generate_record_hash(record) for record in source_data}
    target_hashes = {generate_record_hash(record) for record in target_data}

    missing_in_target = source_hashes - target_hashes
    extra_in_target = target_hashes - source_hashes

    statistics["Missing Records"] = len(missing_in_target)
    statistics["Extra Records"] = len(extra_in_target)

    for i, (source_record, target_record) in enumerate(
        zip(source_data, target_data[: len(source_data)])
    ):
        source_keys = set(source_record.keys())
        target_keys = set(target_record.keys())

        if source_keys != target_keys:
            differences.append(
                f"Record {i}: Key mismatch - "
                f"Source keys: {source_keys}, Target keys: {target_keys}"
            )

        for key in source_keys & target_keys:
            if source_record[key] != target_record[key]:
                differences.append(
                    f"Record {i}, Key '{key}': Value mismatch - "
                    f"Source: {source_record[key]}, Target: {target_record[key]}"
                )

    return ValidationResult(
        is_valid=len(differences) == 0, differences=differences, statistics=statistics
    )


# Example test cases
def run_test_cases():
    print("Running test cases...")

    # Test Case 1: Identical data
    print("\nTest Case 1: Identical data")
    source1 = [
        {"id": "1", "name": "John", "age": "30"},
        {"id": "2", "name": "Jane", "age": "25"},
    ]
    target1 = [
        {"id": "1", "name": "John", "age": "30"},
        {"id": "2", "name": "Jane", "age": "25"},
    ]
    validate_migration(source1, target1).print_report()

    # Test Case 2: Missing record
    print("\nTest Case 2: Missing record")
    target2 = [{"id": "1", "name": "John", "age": "30"}]
    validate_migration(source1, target2).print_report()

    # Test Case 3: Value mismatch
    print("\nTest Case 3: Value mismatch")
    target3 = [
        {"id": "1", "name": "John", "age": "31"},  # Age changed
        {"id": "2", "name": "Jane", "age": "25"},
    ]
    validate_migration(source1, target3).print_report()

    # Test Case 4: Extra field
    print("\nTest Case 4: Extra field")
    target4 = [
        {"id": "1", "name": "John", "age": "30", "email": "john@example.com"},
        {"id": "2", "name": "Jane", "age": "25"},
    ]
    validate_migration(source1, target4).print_report()


if __name__ == "__main__":
    run_test_cases()
