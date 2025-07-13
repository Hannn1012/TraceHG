# -*- coding: utf-8 -*-
# @Time    : 2024/10/15 19:27
# @Author  : hnn
# @FileName: graph_builder.py 构造图文件
# @Software: PyCharm
import json
import os
import shutil
import warnings
import pprint
import pandas as pd
import numpy as np



def load_data(dir_path):
    """ 加载event.csv和edge.csv """
    edge_csv_path = os.path.join(dir_path, 'edge.csv')
    event_csv_path = os.path.join(dir_path, 'event.csv')
    # 不要使用第1列作为索引列
    edge_csv_data = pd.read_csv(edge_csv_path, index_col=False, on_bad_lines='skip')
    event_csv_data = pd.read_csv(event_csv_path, index_col=False, on_bad_lines='skip')

    # edge_csv_data.iloc[:, -1] = edge_csv_data.iloc[:, -1].str.replace(',', '')
    event_csv_data.iloc[:, -1] = event_csv_data.iloc[:, -1].str.replace(',', '')
    edge_csv_data.iloc[:, -1] = edge_csv_data.iloc[:, -1].str.replace(',', '')
    # 只读取前41行并筛选出第一列为指定值的行
    # filtered_rows = []；
    # for i, row in enumerate(edge_csv_data):
    #
    #     if i >= 41:  # 读取到第41行后停止
    #         break
    #     if row[0] == filter_value:
    #         # 去除最后一列中的所有逗号
    #         row[-1] = row[-1].replace(',', '')
    #         filtered_rows.append(row)
    return edge_csv_data, event_csv_data

def event_loader(group):
    # grouped_eventbyTid = group.groupby('TID', sort=False)
    group_list = group.values.tolist()
    Tid_dict = {}

    for row in group_list:
        key = row[1]  # 取得第二项作为键
        if key not in Tid_dict:
            Tid_dict[key] = []  # 如果键不存在，初始化为空列表
        Tid_dict[key].append(row)  # 将整行数组添加到对应的键下

    for key in Tid_dict:
        Tid_dict[key].sort(key=lambda x: int(x[3]))  # x[3]是第4项

    # 将所有值按顺序存在一个数组里
    all_values = []
    for key in Tid_dict:
        all_values = all_values + Tid_dict[key]  # 将每个列表的值添加到 all_values 中


    return all_values

# def edge_loader(group):


if __name__ == '__main__':
    json_dir = '../result/'
    data_path = '../tracebench/hdfs_v3_json_NM'
    json_path = os.path.join(json_dir, '{}.json'.format(data_path.split('_')[-1]))
    # 操作tracebench下面每个文件夹
    # all_jsons = []
    # 定义文件名
    save_path = '../result/NM.jsons'
    save_message_path = '../result/NM_events.json'
    count = 0
    all_message = {}
    error_count = 0
    for dir_name in os.listdir(data_path):
        dir_path = os.path.join(data_path, dir_name)
        # 加载数据

        edge_csv_data, event_csv_data = load_data(dir_path)
        grouped_event = event_csv_data.groupby('TaskID', sort=False)
        grouped_edge = edge_csv_data.groupby('TaskID', sort=False)

        for task_id, group in grouped_event:
            # 对每一个task添加json
            # try:
            Edge_Matrix = []
            EdgeAttribute = []
            Interval = []
            # Message = []
            # 获取一个task的所有数据
            all_values = event_loader(group)
            edges = grouped_edge.get_group(task_id)
            #  去重
            edges = edges.drop_duplicates(keep='first')
            edges = edges.values.tolist()



            # 构建一个tid的边关系
            # 遍历 all_values 查找相邻元素的第二列是否相同
            for i in range(len(all_values) - 1):
                if all_values[i][1] == all_values[i + 1][1]:  # 检查相邻元素的第二列
                    Edge_Matrix.append([i, i + 1])  # 添加索引
                    EdgeAttribute.append(0)
                    Interval.append(int(all_values[i+1][3]) - int(all_values[i][3]))
            # print(len(Edge_Matrix))
            if len(all_values) == 1:
                print("taskid:{} 仅有一条日志".format(task_id))
                error_count += 1
                continue
            for edge in edges:
                flag = 0
                if edge[2] == '0':
                    # 根结点 直接去掉
                    break
                # for j in range(0, len(all_values)):
                #     if all_values[j][1] == edge[3]:
                #         Edge_Matrix.append([-1, j])
                #         EdgeAttribute.append(1)
                #         break  # 跳出内层循环
                else:

                    for i in range(0, len(all_values)):
                        if all_values[i][3] == edge[2]:  # 开始时间对应
                            for j in range(0, len(all_values)):  # 找child节点
                                if all_values[j][1] == edge[3] and int(all_values[j][3]) > int(all_values[i][3]):
                                    Edge_Matrix.append([i, j])
                                    EdgeAttribute.append(1)
                                    Interval.append(int(all_values[j][3])-int(all_values[i][3]))
                                    flag = 1
                                    break
                # if flag == 0:
                #     print("Edge ERROR!")

            # verify
            max_index_edge = np.array(Edge_Matrix).max() + 1
            max_index_event = len(all_values)
            if max_index_event != max_index_edge :
                # print("ERROR!")
                print(task_id)
                error_count += 1
                continue

            # save op+message
            Message = []
            Operation = []
            Message_dict = {}
            for row in all_values:
                if len(row) == 9:
                    Message.append(row[8])
                else:
                    Message.append(row[8] + ' ' + row[9])
                Operation.append(row[2])

            Message_dict[task_id] = [str(o) + '+' + str(m) for o, m in zip(Operation, Message)]
            all_message.update(Message_dict)



            data_json = {
                "EdgeMatrix": Edge_Matrix,
                "EdgeAttribute": EdgeAttribute,
                "Interval": Interval,
                "TaskID": task_id
                }
            # all_jsons.append(data_json)
            with open(save_path, 'a') as f:
                    # 将每个字典对象转换为 JSON 字符串并写入文件
                json_line = json.dumps(data_json)
                f.write(json_line + '\n')  # 每个 JSON 对象后面加一个换行符

            count+= 1
            # print("已处理 {} 个graph\n".format(count))


        # for row in all_values:
        #     Interval.append(int(row[4]) - int(row[3]))
            # if len(row) == 9:
            #     Message.append(row[8])
            # else:
            #     Message.append(row[8] + ' ' + row[9])


        # 构建json图
        # process_one_dir(edge_csv_data, event_csv_data, json_path, Message_dict)

    with open(save_message_path, 'w') as json_file:
        json.dump(all_message, json_file, indent=4)  # indent参数用于美化输出
        print('process finished!')
    print("处理了{}个数据".format(count))
    print("共有{}个错误".format(error_count))
    # print('=' * 30 + '\n共处理 {} 个Task'.format(count))