import os.path
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
user_list_path = data_path + "/user_list.csv"
user_info_path = data_path + "/all_user_info_list.csv"
merge_user_info_path = data_path + "/merge_all_user_info_list.csv"

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
            vos.append(
                {"userId": record["userId"],
                 "debtorName": record["debtorName"],  # 姓名
                 "contractId": '\t' + record["contractId"],  # 合同号
                 "contractIdMd5": '\t' + record["contractIdMd5"],
                 "collectionPeople": record['collectionPeople']})  # 催收人
        return vos
    except Exception as e1:
        write_logs('---error--' + str(e1))
        return []


# 获取所有用户列表
def get_all_user_list() -> list:
    page_no = 1
    page_size = 100
    total_page = int(total_count / page_size) + 1  # 最后一个非整100页
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
    # # 根据userId去重，并且不关心顺序
    # debtor_name_list = set(obj['userId'] for obj in vos)
    # # 去重后的用户列表
    # unique_vos = [next(obj for obj in vos if obj['userId'] == debtorName) for
    #               debtorName in debtor_name_list]
    return vos


# user_list 写入文件
def write_to_file(vos) -> None:
    df0 = pd.DataFrame(vos, columns=['userId', 'debtorName', 'contractId', 'contractIdMd5', 'collectionPeople'])
    # debtorName 排序
    sorted_df = df0.sort_values('debtorName')
    # index=False 列前面不加序列号
    sorted_df.to_csv(user_list_path, index=False)
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
    cvs = (str(batch_num) + "-" + str(user_count) + "=="
           + list_info[0]['userId'] + ','
           + list_info[0]['客户姓名'] + ","
           + list_info[0]['性别'] + ","
           + list_info[0]['年龄'] + ","
           + list_info[0]['身份证号码'] + ","
           + list_info[0]['手机号码'] + ","
           + list_info[0]['户籍地址'] + ","
           + list_info[0]['总欠款金额'] + ","
           + list_info[0]['本金欠款'] + ","
           + list_info[0]['利息欠款'] + ","
           + list_info[0]['逾期利息(不含罚息)'] + ","
           + list_info[0]['当前逾期天数'] + ","
           + list_info[0]['产品名称'] + ","
           + list_info[0]['催收员'] + ","
           + list_info[0]['合同号']
           )
    # print(f'---error----{cvs}')
    logger.error(cvs)


# 获取用户详情
def get_user_info(this_row) -> list:
    cur_contract_id = this_row[3]
    md5 = this_row[4]
    headers['X-Access-Token'] = token
    url = "https://saas.qingcongai.com/qczn-cz/business/caseManage/getDetailInfo?contractId=" + str(
        cur_contract_id).lstrip() + "&md5=" + str(md5).lstrip()
    sleep_time = 0.3
    time.sleep(sleep_time)
    res = requests.get(url, headers=headers)
    res_json = res.json()
    result = res_json['result']
    case_detail_ = result['caseDetail']
    detail_0 = case_detail_[0]  # 客户基本信息
    detail_1 = case_detail_[1]  # 案件信息
    detail_2 = case_detail_[2]  # 逾期信息
    detail_3 = case_detail_[3]  # 还款账户信息
    detail_cash = detail_2['逾期信息']
    json_cash = json.loads(detail_cash)
    total_cash = 0
    if '总欠款金额' in json_cash:
        total_cash = json_cash['总欠款金额']
    this_cash = 0
    if '本金欠款' in json_cash:
        this_cash = json_cash['本金欠款']
    yuqi_cash = 0
    if '逾期利息(不含罚息)' in json_cash:
        yuqi_cash = json_cash['逾期利息(不含罚息)']
    yuqi_day = 0
    if '当前逾期天数' in json_cash:
        yuqi_day = json_cash['当前逾期天数']
    qiankuan_cash = 0
    if '利息欠款' in json_cash:
        qiankuan_cash = json_cash['利息欠款']

    detail_user = detail_0['客户基本信息']
    # 字符串转json
    json_obj = json.loads(detail_user)
    userId = this_row[1]
    if '用户ID' in json_obj:
        userId = json_obj['用户ID']
    user_name = ""
    if '客户姓名' in json_obj:
        user_name = str(json_obj['客户姓名']).split(' ')[0]
    id_card = ""
    if '身份证号码' in json_obj:
        id_card = json_obj['身份证号码']
    elif '身份证号' in json_obj:
        id_card = json_obj['身份证号']
    phone = ""
    if '手机号码' in json_obj:
        phone = json_obj['手机号码']
    address = ""
    if '户籍地址' in json_obj:
        address = json_obj['户籍地址']
    sex = ""
    if '性别' in json_obj:
        sex = json_obj['性别']
    age = ""
    if '年龄' in json_obj:
        age = json_obj['年龄']
    contract_num = cur_contract_id
    if '合同号' in json_obj:
        contract_num = json_obj['合同号']
    case_detail = detail_1['案件信息']
    json_case = json.loads(case_detail)
    prod_name = ""
    if '产品名称' in json_case:
        prod_name = json_case['产品名称']
    vos = [{
        'userId': str(userId),
        '客户姓名': user_name,
        "性别": sex,
        "年龄": age,
        '身份证号码': '\t' + str(id_card),
        '手机号码': '\t' + str(phone),
        '户籍地址': str(address),
        '总欠款金额': str(total_cash),
        '本金欠款': str(this_cash),
        "利息欠款": str(qiankuan_cash),
        "逾期利息(不含罚息)": str(yuqi_cash),
        "当前逾期天数": str(yuqi_day),
        '催收员': str(this_row[5]),
        "产品名称": str(prod_name),
        "合同号": '\t' + str(contract_num)
    }]
    return vos


# 批量写入用户详情到文件 每批100条
def write_info_to_file(vos) -> None:
    df3 = pd.DataFrame(vos,
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
    print(f'----count of  user info file==={len(df_all)}')
    df_all.to_csv(user_info_path, index=False, encoding='utf-8-sig')
    write_logs("合并完成")


def merge_all_cvs2():
    all_data = []
    for filename in os.listdir(info_path):
        if filename.endswith(".csv"):  # 检查文件是否是CSV
            # 构建文件的完整路径
            file_path = os.path.join(info_path, filename)
            # 读取CSV文件
            df = pd.read_csv(file_path)
            df['身份证号码'] = df['身份证号码'].astype(str).apply(lambda x: '\t' + x)
            df['手机号码'] = df['手机号码'].astype(str).apply(lambda x: '\t' + x)
            # 将DataFrame添加到列表中
            all_data.append(df)
        # 使用pd.concat()合并所有的DataFrame
    merged_data = pd.concat(all_data, ignore_index=True)
    # 保存合并后的数据到新的CSV文件
    merged_data.to_csv(merge_user_info_path, index=False)
    write_logs("手动合并完成")


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
    write_logs(f'use time ===={total_count/(3*60)} min')
    total_page = int(total_count / batch_size) + 1
    write_logs(f'total_page==={total_page}')
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

    # 读取用户列表user_list 将df读入的所有user_list中的数据按batch_size大小分批
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
                write_logs(f'获取第{batch_num}批---第{user_count}个用户合同号[ {info[0]["合同号"]} ] 详情成功')
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
