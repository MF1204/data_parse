import os
from pathlib import Path
import psycopg2
from datetime import datetime

from drs_parsing import SRC_DATA_M_CRUD, SRC_DATA_D_CRUD, master_dataset, one_dataset


# 데이터 파싱
def test_parse(data, f_name):
    conn = psycopg2.connect(dbname='dutchboy', user='dutchboy', password='dutchboy2022!', host='3.36.61.69', port=5432)
    cur = conn.cursor()

    src_m = SRC_DATA_M_CRUD(cur)
    src_d = SRC_DATA_D_CRUD(cur)

    r = open(data, 'r')
    drs_full = r.readlines()

    # S1 index 추출
    flag_list = []
    for flag_1_index, line in enumerate(drs_full):
        if line[:2] != '$S':
            continue
        else:
            line_list = line.split(',')
            if line_list[2] == '1':
                flag_list.append(flag_1_index)
    # $F, K, D, V, U, A 묶음
    drs_index_0 = drs_full[0:flag_list[0]]
    result_sno, master_prs_cd = master_dataset(src_m, f_name, drs_index_0)
    # S1 - S3 묶음 (1 ~ n-1)번째
    for i in range(len(flag_list) - 1):
        drs_index_mid = drs_full[flag_list[i]:flag_list[i + 1]]
        result_d = one_dataset(src_d, result_sno, drs_index_mid, master_prs_cd)
        if result_d is False:
            break
    # S1 - S3 묶음 n번째
    drs_index_last = drs_full[flag_list[-1]:]
    result_d = one_dataset(src_d, result_sno, drs_index_last, master_prs_cd)

    if result_sno is not False and result_d is not False:
        conn.commit()
    return

#
# if __name__ == "__main__":
#     current_path = os.getcwd()
#     drs_list = Path(current_path) / 'drs'
#     listdir = os.listdir(drs_list)
#
#     flag = 0
#     for dirname in listdir:
#         if flag == 1:
#             break
#         file_path = drs_list / dirname
#         result_check = []
#         work_start = datetime.now()
#         listdrs = os.listdir(file_path)
#         for index, filename in enumerate(listdrs):
#             if flag == 1:
#                 break
#             print(f'{index} :: {filename}')
#             drs_path = file_path / filename
#             one_start = datetime.now()
#             result_final = test_parse(drs_path, filename)
#             flag += 1

if __name__ == "__main__":
    current_path = os.getcwd()
    drs_list = Path(current_path) / 'drs'
    listdir = os.listdir(drs_list)

    for dirname in listdir:
        file_path = drs_list / dirname
        result_check = []
        work_start = datetime.now()
        listdrs = os.listdir(file_path)
        for index, filename in enumerate(listdrs):
            print(f'{index} :: {filename}')
            drs_path = file_path / filename
            one_start = datetime.now()
            result_final = test_parse(drs_path, filename)

            if result_final is False:
                break
            one_end = datetime.now()
            print(f'{filename} 파일 처리 시간 : {one_end - one_start}')
            result_check.append(result_final)
        print(result_check)
        print(f'{len(listdrs) - len(result_check)}건 실패')
        if False not in result_check:
            work_end = datetime.now()
            print(f'{len(result_check)}건 처리 시간 : {work_end - work_start}')
        else:
            print(result_check)
