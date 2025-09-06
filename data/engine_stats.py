import csv
import math

def read_csv_data(filename):
    """Read CSV data and return as list of dictionaries"""
    data = []
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Convert all values to float except Engine Condition
            processed_row = {}
            for key, value in row.items():
                if key != 'Engine Condition':
                    processed_row[key] = float(value)
                else:
                    processed_row[key] = int(value)
            data.append(processed_row)
    return data

def calculate_stats(values):
    """Calculate all required statistics for a list of values"""
    if not values:
        return None

    n = len(values)

    # Count
    count = n

    # Sum
    total = sum(values)

    # Mean
    mean = total / n

    # Variance and Standard Deviation
    variance = sum((x - mean) ** 2 for x in values) / n
    std_dev = math.sqrt(variance)

    # Min and Max
    min_val = min(values)
    max_val = max(values)

    # Median
    sorted_vals = sorted(values)
    if n % 2 == 0:
        median = (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2
    else:
        median = sorted_vals[n//2]

    # Q1 and Q3
    if n % 2 == 0:
        lower_half = sorted_vals[:n//2]
        upper_half = sorted_vals[n//2:]
    else:
        lower_half = sorted_vals[:n//2]
        upper_half = sorted_vals[n//2+1:]

    if len(lower_half) % 2 == 0:
        q1 = (lower_half[len(lower_half)//2 - 1] + lower_half[len(lower_half)//2]) / 2
    else:
        q1 = lower_half[len(lower_half)//2]

    if len(upper_half) % 2 == 0:
        q3 = (upper_half[len(upper_half)//2 - 1] + upper_half[len(upper_half)//2]) / 2
    else:
        q3 = upper_half[len(upper_half)//2]

    # IQR
    iqr = q3 - q1

    # Skewness
    if std_dev != 0:
        skewness = sum(((x - mean) / std_dev) ** 3 for x in values) / n
    else:
        skewness = 0

    # Kurtosis
    if std_dev != 0:
        kurtosis = sum(((x - mean) / std_dev) ** 4 for x in values) / n - 3
    else:
        kurtosis = 0

    return {
        'count': count,
        'sum': total,
        'mean': mean,
        'variance': variance,
        'std_dev': std_dev,
        'min': min_val,
        'max': max_val,
        'median': median,
        'q1': q1,
        'q3': q3,
        'iqr': iqr,
        'skewness': skewness,
        'kurtosis': kurtosis
    }

def group_data_by_condition(data):
    """Group data by Engine Condition (0 or 1)"""
    condition_0 = []
    condition_1 = []

    for row in data:
        if row['Engine Condition'] == 0:
            condition_0.append(row)
        else:
            condition_1.append(row)

    return condition_0, condition_1

def calculate_column_stats(data, columns):
    """Calculate statistics for each column in the data"""
    results = {}

    for column in columns:
        if column != 'Engine Condition':
            values = [row[column] for row in data]
            results[column] = calculate_stats(values)

    return results

def print_stats(condition, stats):
    """Print statistics in a formatted way"""
    print(f"\n=== Engine Condition {condition} ===")
    for column, stat in stats.items():
        if stat is not None:
            print(f"\n{column}:")
            print(f"  Count: {stat['count']}")
            print(f"  Sum: {stat['sum']:.6f}")
            print(f"  Mean: {stat['mean']:.6f}")
            print(f"  Variance: {stat['variance']:.6f}")
            print(f"  Standard Deviation: {stat['std_dev']:.6f}")
            print(f"  Min: {stat['min']:.6f}")
            print(f"  Max: {stat['max']:.6f}")
            print(f"  Median: {stat['median']:.6f}")
            print(f"  Q1: {stat['q1']:.6f}")
            print(f"  Q3: {stat['q3']:.6f}")
            print(f"  IQR: {stat['iqr']:.6f}")
            print(f"  Skewness: {stat['skewness']:.6f}")
            print(f"  Kurtosis: {stat['kurtosis']:.6f}")

def main():
    # Read data
    filename = 'data/engine_data.csv'
    data = read_csv_data(filename)

    # Group data by Engine Condition
    condition_0_data, condition_1_data = group_data_by_condition(data)

    # Get column names (excluding Engine Condition)
    columns = [col for col in data[0].keys() if col != 'Engine Condition']

    # Calculate statistics for each condition
    stats_0 = calculate_column_stats(condition_0_data, columns)
    stats_1 = calculate_column_stats(condition_1_data, columns)

    # Print results
    print_stats(0, stats_0)
    print_stats(1, stats_1)

if __name__ == "__main__":
    main()
