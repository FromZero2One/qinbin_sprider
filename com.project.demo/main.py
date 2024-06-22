import os.path
import random
import sys
import time
import logging.config
import logging
import pandas as pd
import requests
from pandas import DataFrame
import json
import glob

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 "
                  "Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Accept-Encoding": "gzip",
    "Connection": "keep-alive",
    "X-Access-Token": "token"
}

params = {
    "specialFields": {}, "isFixed": 0,
    "queryType": 0,
    "column": "create_time", "order": "desc",
    "field": "id,batchNum,userId,creditors_dictText,contractId,label,caseType,debtorName,type,caseStatus_dictText,"
             "collectionStatus_dictText,assignmentTime,dropCaseTime,assignmentAmt,arrearsAmt,assignmentPayment,"
             "recentlyTime,allocTime,groupName,collectionPeople,reductionStatus_dictText,agreeReductionAmt,action",
    "pageNo": 1, "pageSize": 1}

token = ""
config = {}
time_str = time.strftime("%Y%m%d")
default_path = "c://opt/"
today_path = default_path + time_str
config_path = default_path + "/config.txt"
logs_path = today_path + "/logs"
data_path = today_path + "/data"
info_path = data_path + "/info_list/"
user_list_path = data_path + "/user_list.cvs"
user_info_path = data_path + "/user_info_list.cvs"

list_url = "https://saas.qingcongai.com/qczn-cz/business/caseManage/adminList"

# 配置日志
error_log_file = logs_path + "/logs_error_" + time_str + ".txt"
info_log_file = logs_path + "/logs_info_" + time_str + ".txt"

# 创建日志目录和文件
if not os.path.exists(logs_path):
    os.makedirs(logs_path)
    with open(error_log_file, 'w') as f1:
        pass
    with open(info_log_file, 'w') as f2:
        pass
# 创建用户列表目录
if not os.path.exists(info_path):
    os.makedirs(info_path)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
    },
    'handlers': {
        'file1': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': info_log_file,  # 第一个日志文件
            'formatter': 'verbose',
        },
        'file2': {
            'level': 'ERROR',
            'class': 'logging.FileHandler',
            'filename': error_log_file,  # 第二个日志文件
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'my_logger': {  # 定义一个名为my_logger的logger
            'handlers': ['file1', 'file2'],  # 使用上面定义的handler
            'level': 'INFO',
            'propagate': False,  # 是否将日志消息传递给上级logger
        },
    },
    'root': {
        'handlers': [],
        'level': 'INFO'
    },
}

# 使用字典配置日志
logging.config.dictConfig(LOGGING)
# 获取logger对象
logger = logging.getLogger('my_logger')


# 获取总条数
def get_page_total_count() -> int:
    try:
        headers['X-Access-Token'] = token
        res = requests.post(list_url, headers=headers, json=params)
        res_json = res.json()
        code_ = res_json['code']
        if code_ != 200:
            write_logs('---error--' + str(res_json))
            return 0
        result = res_json['result']
        total_ = result['total']
    except Exception as e0:
        write_logs('---error--' + str(e0))
        return 0
    return total_


# 分页获取用户列表
def get_page_user_list(page_no=1, page_size=100) -> list:
    headers['X-Access-Token'] = token
    params['pageNo'] = page_no
    params['pageSize'] = page_size
    try:
        res = requests.post(list_url, headers=headers, json=params)
        res_json = res.json()
        if res_json['code'] != 200:
            write_logs('---error--' + str(res_json))
            return []
        result = res_json['result']
        records = result['records']
        vos = []
        for record in records:
            vos.append({"debtorName": record["debtorName"], "contractId": record["contractId"],
                        "contractIdMd5": record["contractIdMd5"], "collectionPeople": record['collectionPeople']})
        return vos
    except Exception as e1:
        write_logs('---error--' + str(e1))
        return []


# 获取所有用户列表
def get_all_user_list() -> list:
    page_no = 1
    page_size = 100
    total_page = int(total_count / page_size)
    write_logs(f'total_page==={total_page}')
    vos = []
    # 分页获取
    while page_no <= total_page:
        print(f'pageNo===={page_no} pageSize===={page_size}')
        vo = get_page_user_list(page_no, page_size)
        if len(vo) == 0:
            break
        vos.extend(vo)
        page_no += 1
    # 根据debtorName去重，并且不关心顺序
    debtor_name_list = set(obj['debtorName'] for obj in vos)
    # 去重后的用户列表
    unique_vos = [next(obj for obj in vos if obj['debtorName'] == debtorName) for debtorName in debtor_name_list]
    return unique_vos


# user_list 写入文件
def write_to_file(vos) -> None:
    df0 = pd.DataFrame(vos, columns=['debtorName', 'contractId', 'contractIdMd5', 'collectionPeople'])
    # index=False 列前面不加序列号
    df0.to_csv(user_list_path, index=False)
    write_logs(f'user_list 写入文件成功，共{len(vos)}条')


def write_batch_num_to_config() -> None:
    config['batch_num'] = str(batch_num)
    with open(config_path, "w") as f:
        for key, value in config.items():
            f.write(key + "=" + value + "\n")


# 读取配置文件
def get_config() -> dict:
    if not os.path.exists(config_path):
        write_logs("配置文件不存在，在c://opt 下面添加config.txt")
        return {}
    else:
        with open(config_path, "r") as f:
            config_list = f.readlines()
            config_dict = {}
            for cof in config_list:
                config_dict[cof.split("=")[0]] = cof.split("=")[1].strip()
            return config_dict


def write_logs(logs) -> None:
    print("---info----" + logs)
    logger.info("----" + logs)


def write_user_info_logs(list_info) -> None:
    cvs = str(batch_num) + "-" + str(user_count) + "==" + list_info[0]['客户姓名'] + "," + list_info[0][
        '身份证号码'] + "," + list_info[0][
              '手机号码'] + "," + list_info[0]['户籍地址'] + "," + list_info[0][
              '生日'] + "," + list_info[0]['年龄'] + "," + list_info[0]['催收员']
    # print(f'---error----{cvs}')
    logger.error(cvs)


# 获取用户详情
def get_user_info(this_row) -> list:
    cur_contract_id = this_row[2]
    md5 = this_row[3]
    headers['X-Access-Token'] = token
    url = "https://saas.qingcongai.com/qczn-cz/business/caseManage/getDetailInfo?contractId=" + str(
        cur_contract_id) + "&md5=" + str(md5)
    randint = random.randint(1, 2)
    time.sleep(1)
    res = requests.get(url, headers=headers)
    res_json = res.json()
    result = res_json['result']
    case_detail_ = result['caseDetail']
    detail__ = case_detail_[0]
    detail_user = detail__['客户基本信息']
    # 字符串转json
    json_obj = json.loads(detail_user)
    vos = [{
        '客户姓名': json_obj['客户姓名'],
        '身份证号码': json_obj['身份证号码'],
        '手机号码': json_obj['手机号码'],
        '户籍地址': json_obj['户籍地址'],
        '生日': json_obj['生日'],
        '年龄': json_obj['年龄'],
        '催收员': this_row[4]
    }]
    return vos


# 批量写入用户详情到文件 每批100条
def write_info_to_file(vos) -> None:
    df3 = pd.DataFrame(vos, columns=['客户姓名', '身份证号码', '手机号码', '户籍地址', '生日', '年龄', '催收员'])
    # index=False 列前面不加序列号
    df3.to_csv(info_path + str(batch_num) + '_user_info.csv', index=False)


# 读取已经保存的批次文件
def read_cvs_info_count() -> int:
    if os.path.exists(info_path):
        listdir = os.listdir(info_path)
        return len(listdir)
    else:
        os.makedirs(info_path)
    return 0


def file_is_exists(file_path) -> bool:
    return os.path.exists(file_path)


# 读取去重后的用户列表
def read_list_from_file() -> DataFrame:
    global df
    df = pd.read_csv(user_list_path)
    return df


def merge_all_cvs() -> None:
    write_logs("开始合并文件")
    # 使用 glob 模块找到所有 CSV 文件
    all_files = glob.glob(info_path + "*.csv")
    # 创建一个空的 DataFrame 列表
    li_df = []
    # 遍历所有文件，并将它们添加到列表中
    for filename in all_files:
        print(f"filename==={filename}")
        df1 = pd.read_csv(filename, index_col=None, header=0)  # 假设所有文件都有相同的列名和行头
        li_df.append(df1)
    # 将所有 DataFrame 合并到一个 DataFrame 中
    df_all = pd.concat(li_df, axis=0, ignore_index=True)
    # 将合并后的 DataFrame 写入新的 CSV 文件
    df_all.to_csv(user_info_path, index=False, encoding='utf-8-sig')
    write_logs("合并完成")


if __name__ == '__main__':
    config = get_config()
    if config is None:
        write_logs('exit---->  config is null')
        sys.exit("exit---->  config is null")

    token = config.get("X-Access-Token").rstrip()
    write_logs(f'X-Access-Token===={token}')
    config_batch_num = '0'
    config_batch_size = '5'  # 假设你想要100个批次
    if config.get('batch_num') is not None:
        config_batch_num = config.get('batch_num')
    batch_num = int(config_batch_num)
    if config.get('batch_size') is not None:
        config_batch_size = config.get('batch_size')
    batch_size = int(config_batch_size)
    write_logs(f'batch_num===={batch_num}')
    write_logs(f'batch_size===={batch_size}')

    #  总条数
    total_count = get_page_total_count()
    write_logs(f'total_count===={total_count}')
    if total_count == 0:
        write_logs('"exit---->  total_count =0 =0')
        sys.exit("exit----> total_count =0 ")

    # 获取所有用户列表
    if file_is_exists(user_list_path) is False:
        user_list = get_all_user_list()
        write_logs(f'user_list===={len(user_list)}')
        if len(user_list) == 0:
            write_logs('"exit----> user_list =0')
            sys.exit("exit----> user_list =0 ")
        # 写入文件
        write_to_file(user_list)

    # 读取list 根据配置文件分批
    df = read_list_from_file()
    batch_list = []
    for batch in range(0, len(df), batch_size):
        one_batch = df.iloc[batch:batch + batch_size]
        batch_list.append(one_batch)

    # 已经保存文件数
    info_count = read_cvs_info_count()
    batch_num = info_count
    # 按批次获取用户详情
    count = 0
    for batch in batch_list:
        count += 1
        # 断点续爬
        if batch_num >= count:
            continue
        batch_num += 1
        batch_user_infos = []
        write_logs(f'开始获取第{batch_num}批次用户详情')
        user_count = 0
        for row in batch.itertuples():
            try:
                info = get_user_info(row)
                user_count += 1
                write_logs(f'获取第{batch_num}批---第{user_count}个用户详情成功')
                write_user_info_logs(info)
                batch_user_infos.extend(info)
            except Exception as e:
                write_logs('---获取用户详情失败---' + str(e))
                write_batch_num_to_config()  # 将批次写入配置文件
                sys.exit("exit----> 获取用户详情失败")
        # 写入一批
        write_logs(f"获取第{batch_num}批次用户详情成功")
        write_info_to_file(batch_user_infos)
        write_logs(f"写入第{batch_num}批次用户详情成功")
        write_batch_num_to_config()  # 将批次写入配置文件

    # 合并 user_info 文件
    merge_all_cvs()
