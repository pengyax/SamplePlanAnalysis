import pandas as pd
from functools import reduce

# 从datasource.xlsx读取指定的sheet
xls_datasource = pd.ExcelFile('../data_input/datasource.xlsx')
df = xls_datasource.parse(0)  # 基础数据DataFrame
df_normal = xls_datasource.parse(1)  # 正常检验标准DataFrame
df_reduce = xls_datasource.parse(2)  # 缩减检验标准DataFrame

# 从Sampling Plan information.xlsx读取特定的sheet
df_sample_plan = pd.read_excel('../data_input/Sampling Plan information.xlsx')

# 重命名列并调整Sampling Plan信息
df_sample_plan.rename(columns={'Lot Size (from)': 'Min', 'Lot Size (to)': 'Max'}, inplace=True)
df_sample_plan.loc[df_sample_plan.groupby(['Sampling Plan ID'])['Max'].idxmax(), 'Max'] = 99999999

# 简化DataFrame切片操作的函数
def get_criteria_df(criteria_df, criteria_type):
    return criteria_df[criteria_df['Criteria Type'] == criteria_type][['PIC', 'Sampling Plan ID']]

# 函数化重复逻辑
def create_sample_size_df(base_df, criteria_df, criteria_name):
    merged_df = pd.merge(base_df, criteria_df, on='PIC', how='left')
    merged_df[f"{criteria_name} Sample Size"] = merged_df.apply(lambda row: get_sample_size(row, df_sample_plan), axis=1)
    return merged_df.sort_values(by=f"{criteria_name} Sample Size", ascending=False).drop_duplicates('key')[['key', f"{criteria_name} Sample Size"]]

def get_sample_size(row, df_sample_plan):
    sample_sizes = df_sample_plan[(df_sample_plan['Sampling Plan ID'] == row['Sampling Plan ID']) & 
                                  (df_sample_plan['Min'] <= row['Qty EA']) & 
                                  (df_sample_plan['Max'] >= row['Qty EA'])]['Sample Size']
    return sample_sizes.max() if sample_sizes.notna().any() else None

# 处理基础DataFrame
df_base = (
    df
    .assign(Inspection_Date=lambda d: pd.to_datetime(d['Inspection Date']))
    .assign(key=lambda d: d.apply(lambda s: f"{s['Lot Number']}|{s['Inspection Date'].date()}|{s['Inspector']}|{s['Item Number']}" 
                                   if s['Path'] != 'QIM' 
                                   else s['ID'], axis=1))
    .query('PIC != "Missing in ETQ"')
    .groupby(['key', 'PIC'], as_index=False)['Qty EA'].sum()
)

# 直接构建dfs_by_type，避免使用eval或locals
criteria_types = ['Functional', 'Dimensional', 'Visual']
dfs_by_type = {
    f"normal_{criteria}": get_criteria_df(df_normal, criteria) 
    for criteria in criteria_types
}
dfs_by_type.update({
    f"reduce_{criteria}": get_criteria_df(df_reduce, criteria) 
    for criteria in criteria_types
})

# 使用函数创建每个类型的样本大小DataFrame
sample_size_dfs = [create_sample_size_df(df_base, dfs_by_type[key], key) for key in dfs_by_type.keys()]

# 合并所有样本大小DataFrame
final_df = reduce(lambda left, right: pd.merge(left, right, on='key', how='left'), [df_base] + sample_size_dfs)

# 输出到Excel
final_df.to_excel('./out_pic_qty.xlsx', index=False)

df_gy = (
    final_df.groupby('PIC',as_index=False)[[
        'normal_Functional Sample Size', 'normal_Dimensional Sample Size', 'normal_Visual Sample Size',
        'reduce_Functional Sample Size', 'reduce_Dimensional Sample Size', 'reduce_Visual Sample Size'
    ]]
    .sum()
    .rename(columns=lambda x: x.replace(' Sample Size', ''))
)

(
    df_gy
    .assign(Functional_decreasing_qty = lambda d : d.normal_Functional - d.reduce_Functional,
            Dimensional_decreasing_qty = lambda d : d.normal_Dimensional - d.reduce_Dimensional,
            Visual_decreasing_qty = lambda d : d.normal_Visual - d.reduce_Visual,
            Functional_decreasing_per = lambda d : d.reduce_Functional / d.normal_Functional - 1,
            Dimensional_decreasing_per = lambda d : d.reduce_Dimensional /  d.normal_Dimensional - 1,
            Visual_decreasing_per = lambda d : d.reduce_Visual / d.normal_Visual - 1
            )
    .fillna(0)
).to_excel('final.xlsx',index=False)