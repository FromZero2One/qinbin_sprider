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
                    'userId': str(col[0]),
                    '客户姓名': str(col[1]),
                    "性别": str(col[2]),
                    "年龄": str(col[3]),
                    '身份证号码': '\t' + str(col[4]),
                    '手机号码': '\t' + str(col[5]),
                    '户籍地址': str(col[6]),
                    '总欠款金额': str(col[7]),
                    '本金欠款': str(col[8]),
                    "利息欠款": str(col[9]),
                    "逾期利息(不含罚息)": str(col[10]),
                    "当前逾期天数": str(col[11]),
                    '催收员': str(col[12]),
                    "产品名称": str(col[13]),
                    "合同号": '\t' + str(col[14])
                }]
                cvs.extend(cv)
            # 写入文件
            df3 = pd.DataFrame(cvs,
                               columns=[
                                   'userId',
                                   '客户姓名',
                                   "性别",
                                   "年龄",
                                   '身份证号码',
                                   '手机号码',
                                   '户籍地址',
                                   '总欠款金额',
                                   '本金欠款',
                                   "利息欠款",
                                   "逾期利息(不含罚息)",
                                   "当前逾期天数",
                                   '催收员',
                                   "产品名称",
                                   "合同号"])
            # 去重
            duplicates = df3.drop_duplicates()
            # index=False 列前面不加序列号
            duplicates.to_csv(m.data_path + '/read_logs_user_info.csv', index=False)
            print("抓取logs数据成功")


if __name__ == '__main__':
    read_logs()
