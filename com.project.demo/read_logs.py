import os.path

import main as m
import pandas as pd

file = m.error_log_file


def read_logs():
    if os.path.exists(file):
        # 读取log文件
        with open(file, 'r') as f:
            lines = f.readlines()
            cvs = []
            for line in lines:
                split = line.split("==")
                c = split[1].strip()
                col = c.split(",")
                cv = [{
                    '客户姓名': str(col[0]).split(' ')[0],
                    '身份证号码': '\t'+str(col[1]),
                    '手机号码': '\t'+str(col[2]),
                    '户籍地址': str(col[3]),
                    '生日': '\t'+str(col[4]),
                    '年龄': str(col[5]),
                    '催收员': str(col[6])
                }]
                cvs.extend(cv)
            # 写入文件
            df3 = pd.DataFrame(cvs,
                               columns=['客户姓名', '身份证号码', '手机号码', '户籍地址', '生日', '年龄', '催收员'])
            # 去重
            duplicates = df3.drop_duplicates()
            # index=False 列前面不加序列号
            duplicates.to_csv(m.data_path + '/read_logs_user_info.csv', index=False)


if __name__ == '__main__':
    read_logs()
