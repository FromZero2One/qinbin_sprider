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
                    '客户姓名': col[0],
                    '身份证号码': col[1],
                    '手机号码': col[2],
                    '户籍地址': col[3],
                    '生日': col[4],
                    '年龄': col[5],
                    '催收员': col[6]
                }]
                cvs.extend(cv)
            # 写入文件
            df3 = pd.DataFrame(cvs,
                               columns=['客户姓名', '身份证号码', '手机号码', '户籍地址', '生日', '年龄', '催收员'])
            # index=False 列前面不加序列号
            df3.to_csv(m.data_path + '/read_logs_user_info.csv', index=False)


if __name__ == '__main__':
    read_logs()
