import pandas as pd
import json
import os
from datetime import datetime
from pathlib import Path
import glob


def parse_time_column(time_str, year=2022):
    """
    将时间列名从 'MMDD-HHMMSS' 格式转换为 datetime 对象
    例如: '0105-100000' -> 2022-01-05 10:00:00
    """
    try:
        month_day, time_part = time_str.split('-')
        month = month_day[:2]
        day = month_day[2:4]
        hour = time_part[:2]
        minute = time_part[2:4]
        second = time_part[4:6]

        return datetime(year, int(month), int(day), int(hour), int(minute), int(second))
    except:
        return None


def read_csv_gbk(file_path):
    """
    读取 GBK 编码的 CSV 文件
    """
    return pd.read_csv(file_path, encoding='gbk')


def extract_variable_from_csv(csv_path, variable_names, year=2022):
    """
    从 CSV 文件中提取指定变量的数据

    参数:
        csv_path: CSV 文件路径
        variable_names: 要提取的变量名称列表
        year: 年份（用于解析时间）

    返回:
        DataFrame，格式为 {时间: [值1, 值2, ...], 变量1: [...], 变量2: [...]}
    """
    df = read_csv_gbk(csv_path)

    # 获取时间列（从第5列开始，即索引4）
    time_columns = df.columns[4:]

    # 解析时间
    timestamps = [parse_time_column(col, year) for col in time_columns]

    # 过滤掉解析失败的时间
    valid_indices = [i for i, t in enumerate(timestamps) if t is not None]
    timestamps = [timestamps[i] for i in valid_indices]
    time_columns = [time_columns[i] for i in valid_indices]

    # 提取指定变量的数据
    result_data = {'时间': timestamps}

    for var_name in variable_names:
        # 在"描述"列中查找变量
        matching_rows = df[df['描述'] == var_name]

        if not matching_rows.empty:
            # 获取该变量的所有时间序列数据
            values = matching_rows.iloc[0][time_columns].values
            result_data[var_name] = values
        else:
            # 如果找不到变量，尝试在"点名"列中查找
            matching_rows = df[df['点名'] == var_name]
            if not matching_rows.empty:
                values = matching_rows.iloc[0][time_columns].values
                result_data[var_name] = values
            else:
                print(f"警告: 在 {csv_path} 中未找到变量 '{var_name}'")
                result_data[var_name] = [None] * len(timestamps)

    return pd.DataFrame(result_data)


def process_all_data(base_path, json_config_path, year=2022):
    """
    处理所有数据文件并合并

    参数:
        base_path: 数据文件的基础路径
        json_config_path: 站点变量.json 的路径
        year: 年份

    返回:
        合并后的 DataFrame
    """
    base_path = Path(base_path)

    # 读取站点变量配置
    with open(json_config_path, 'r', encoding='utf-8') as f:
        site_config = json.load(f)

    # 使用字典按变量分组收集数据
    variable_data = {}

    # 处理"功率"文件夹中的目标变量
    power_folder = base_path / '功率'
    target_variable = '发电机有功功率输出1'

    print(f"处理功率文件夹: {power_folder}")
    power_files = sorted(glob.glob(str(power_folder / '*.csv')))  # 排序确保时间顺序

    power_dfs = []
    for power_file in power_files:
        print(f"  读取文件: {power_file}")
        df_power = extract_variable_from_csv(power_file, [target_variable], year)
        power_dfs.append(df_power)

    # 将同一变量的多个时间段数据连接起来
    if power_dfs:
        combined_power = pd.concat(power_dfs, ignore_index=True)
        # 去重（以防时间重叠）并排序
        combined_power = combined_power.drop_duplicates(subset=['时间']).sort_values('时间').reset_index(drop=True)
        variable_data[target_variable] = combined_power

    # 处理"模入量"文件夹中的各站点数据
    analog_folder = base_path / '模入量'

    print(f"\n处理模入量文件夹: {analog_folder}")

    for site_name, variables in site_config.items():
        print(f"\n  处理站点: {site_name}")
        print(f"    需要提取的变量: {variables}")

        # 查找该站点的所有CSV文件并排序
        site_files = sorted(glob.glob(str(analog_folder / f'{site_name}_*.csv')))

        # 为每个变量收集数据
        site_dfs = []
        for site_file in site_files:
            print(f"    读取文件: {site_file}")
            df_site = extract_variable_from_csv(site_file, variables, year)
            site_dfs.append(df_site)

        # 连接同一站点的多个时间段数据
        if site_dfs:
            combined_site = pd.concat(site_dfs, ignore_index=True)
            # 去重并排序
            combined_site = combined_site.drop_duplicates(subset=['时间']).sort_values('时间').reset_index(drop=True)

            # 将这个站点的数据加入字典
            for var in variables:
                if var in combined_site.columns:
                    if var not in variable_data:
                        variable_data[var] = combined_site[['时间', var]]
                    else:
                        # 如果变量已存在，合并数据
                        existing_df = variable_data[var]
                        new_df = combined_site[['时间', var]]
                        merged = pd.merge(existing_df, new_df, on='时间', how='outer', suffixes=('', '_new'))
                        # 如果有重复列，优先使用非空值
                        if f'{var}_new' in merged.columns:
                            merged[var] = merged[var].combine_first(merged[f'{var}_new'])
                            merged = merged.drop(columns=[f'{var}_new'])
                        variable_data[var] = merged

    # 合并所有变量的数据
    print("\n合并所有变量数据...")
    if not variable_data:
        raise ValueError("没有找到任何数据")

    # 先获取所有时间点
    all_times = set()
    for var_name, df in variable_data.items():
        all_times.update(df['时间'].tolist())

    all_times = sorted(list(all_times))
    merged_df = pd.DataFrame({'时间': all_times})

    # 逐个合并变量
    for var_name, df in variable_data.items():
        print(f"  合并变量: {var_name}")
        merged_df = pd.merge(merged_df, df, on='时间', how='left')

    # 按时间排序
    merged_df = merged_df.sort_values('时间').reset_index(drop=True)

    # 将目标变量移到最后一列
    if target_variable in merged_df.columns:
        cols = [col for col in merged_df.columns if col != target_variable]
        cols.append(target_variable)
        merged_df = merged_df[cols]
        print(f"\n'{target_variable}' 已移至最后一列")

    print(f"\n合并完成! 共 {len(merged_df)} 行, {len(merged_df.columns)} 列")
    print(f"列名: {list(merged_df.columns)}")

    return merged_df


def main():
    """
    主函数
    """
    # 配置路径
    base_path = '/aaa'  # 修改为你的数据文件夹路径
    json_config_path = '/aaa/站点变量.json'  # 修改为你的JSON配置文件路径
    output_path = '/aaa/merged_timeseries_data.csv'  # 输出文件路径
    year = 2022  # 数据年份

    print("=" * 60)
    print("时间序列数据处理脚本")
    print("=" * 60)

    # 检查路径是否存在
    if not os.path.exists(json_config_path):
        print(f"错误: 配置文件不存在 - {json_config_path}")
        return

    if not os.path.exists(base_path):
        print(f"错误: 数据文件夹不存在 - {base_path}")
        return

    # 处理数据
    try:
        merged_df = process_all_data(base_path, json_config_path, year)

        # 保存为 CSV
        print(f"\n保存结果到: {output_path}")
        merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')

        # 显示前几行
        print("\n数据预览:")
        print(merged_df.head())

        print("\n数据统计:")
        print(f"  总行数: {len(merged_df)}")
        print(f"  总列数: {len(merged_df.columns)}")
        print(f"  时间范围: {merged_df['时间'].min()} 到 {merged_df['时间'].max()}")
        print(f"  缺失值统计:")
        print(merged_df.isnull().sum())

        print("\n处理完成!")

    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
