from main import data_path

"""
    比较两个文件，找出两个文件不同的部分 并将数据落库到csv中
"""


def compare_and_merge():
    user_list = data_path + '/all_user_info_list.csv'
    user_list1 = data_path + '/all_user_info_list1.csv'
    # 上面两个文件中用户的差集
    append_user_list = data_path + '/append_user_info_list.csv'

    users = []
    users_1 = []
    with open(user_list, 'r', encoding='utf-8') as f:
        for line in f:
            users.append(line)

    with open(user_list1, 'r', encoding='utf-8') as f1:
        for line in f1:
            users_1.append(line)

    merge_users = []
    if len(users) > len(users_1):
        for i in users:
            if i not in users_1:
                merge_users.append(i)
    else:
        for i in users_1:
            if i not in users:
                merge_users.append(i)

    print(len(merge_users))
    # 写入文件
    with open(append_user_list, 'w', encoding='utf-8') as f2:
        for i in merge_users:
            f2.write(i)


if __name__ == '__main__':
    compare_and_merge()
