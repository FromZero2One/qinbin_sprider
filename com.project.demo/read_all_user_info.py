import glob

import pandas as pd

import main

all_files = glob.glob(main.info_path + "*.csv")
# 创建一个空的 DataFrame 列表
li_df = []
# 遍历所有文件，并将它们添加到列表中
for filename in all_files:
    print(f"filename==={filename}")
    df1 = pd.read_csv(filename,
                      converters={'身份证号码': str,
                                  '手机号码': str,
                                  '户籍地址': str,
                                  '总欠款金额': str,
                                  '本金欠款': str,
                                  '催收员': str})  # 都按字符串读入
    li_df.append(df1)
# 将所有 DataFrame 合并到一个 DataFrame 中
df_all = pd.concat(li_df, axis=0, ignore_index=True)

print(df_all)
