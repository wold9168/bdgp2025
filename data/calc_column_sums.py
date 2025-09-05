import csv
import sys

def calculate_column_sums(csv_file_path):
    """
    计算CSV文件中各列的总和

    参数:
    csv_file_path (str): CSV文件的路径

    返回:
    dict: 包含每列名称和对应总和的字典
    """
    sums = {}
    column_names = []

    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)

            # 读取标题行
            header = next(reader)
            column_names = header

            # 初始化每列的总和为0
            for col_name in column_names:
                sums[col_name] = 0

            # 遍历所有数据行并累加数值
            for row in reader:
                for i, value in enumerate(row):
                    if i < len(column_names):
                        try:
                            # 尝试将值转换为浮点数并累加
                            sums[column_names[i]] += float(value)
                        except ValueError:
                            # 如果转换失败，跳过该值
                            continue

    except FileNotFoundError:
        print(f"错误：找不到文件 {csv_file_path}")
        return None
    except Exception as e:
        print(f"处理文件时发生错误: {e}")
        return None

    return sums

def main():
    """
    主函数
    """
    if len(sys.argv) != 2:
        print("使用方法: python script.py <csv_file_path>")
        return

    csv_file_path = sys.argv[1]

    sums = calculate_column_sums(csv_file_path)

    if sums is not None:
        print("各列总和:")
        for column_name, total in sums.items():
            print(f"{column_name}: {total}")

if __name__ == "__main__":
    main()
