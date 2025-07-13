# -*- coding: utf-8 -*-
# @Time    : 2024/9/15 19:27
# @Author  : hnn
# @FileName: graph_builder.py
# @Software: PyCharm
import json
import os
import shutil
import warnings
import pprint
import pandas as pd
import numpy as np



def load_data(dir_path):
    edge_csv_path = os.path.join(dir_path, 'edge.csv')
    event_csv_path = os.path.join(dir_path, 'event.csv')
    edge_csv_data = pd.read_csv(edge_csv_path, index_col=False, on_bad_lines='skip')
    event_csv_data = pd.read_csv(event_csv_path, index_col=False, on_bad_lines='skip')

    event_csv_data.iloc[:, -1] = event_csv_data.iloc[:, -1].str.replace(',', '')
    edge_csv_data.iloc[:, -1] = edge_csv_data.iloc[:, -1].str.replace(',', '')

    return edge_csv_data, event_csv_data

def event_loader(group):
    group_list = group.values.tolist()
    Tid_dict = {}

    for row in group_list:
        key = row[1]
        if key not in Tid_dict:
            Tid_dict[key] = []
        Tid_dict[key].append(row)

    for key in Tid_dict:
        Tid_dict[key].sort(key=lambda x: int(x[3]))

    all_values = []
    for key in Tid_dict:
        all_values = all_values + Tid_dict[key]


    return all_values



if __name__ == '__main__':
    json_dir = '../result/'
    data_path = '../tracebench/hdfs_v3_json_NM'
    json_path = os.path.join(json_dir, '{}.json'.format(data_path.split('_')[-1]))
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
            Edge_Matrix = []
            EdgeAttribute = []
            Interval = []
            all_values = event_loader(group)
            edges = grouped_edge.get_group(task_id)
            edges = edges.drop_duplicates(keep='first')
            edges = edges.values.tolist()


            for i in range(len(all_values) - 1):
                if all_values[i][1] == all_values[i + 1][1]:
                    Edge_Matrix.append([i, i + 1])
                    EdgeAttribute.append(0)
                    Interval.append(int(all_values[i+1][3]) - int(all_values[i][3]))
            if len(all_values) == 1:
                print("taskid:{} 仅有一条日志".format(task_id))
                error_count += 1
                continue
            for edge in edges:
                flag = 0
                if edge[2] == '0':
                    break
                else:

                    for i in range(0, len(all_values)):
                        if all_values[i][3] == edge[2]:
                            for j in range(0, len(all_values)):
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
                json_line = json.dumps(data_json)
                f.write(json_line + '\n')

            count+= 1


        # for row in all_values:
        #     Interval.append(int(row[4]) - int(row[3]))
            # if len(row) == 9:
            #     Message.append(row[8])
            # else:
            #     Message.append(row[8] + ' ' + row[9])


    with open(save_message_path, 'w') as json_file:
        json.dump(all_message, json_file, indent=4)
        print('process finished!')
    # print('=' * 30 + '\n共处理 {} 个Task'.format(count))