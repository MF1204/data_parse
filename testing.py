import os
import re
import traceback
from pathlib import Path
import psycopg2
from datetime import datetime

from drs_parsing import SRC_DATA_M_CRUD, SRC_DATA_D_CRUD, sns_separate


# $F, K, D, V, U, A 처리
def master_dataset(src_class, drsfile_name, header_list):
    head_dict = {}
    for line in header_list:
        head_dict[line[1:2]] = line[3:].replace('\n', '')
    # src_data_m =================================================
    k_val = head_dict['K']
    lot_date = str(datetime.now().year)[:2] + drsfile_name.split('_')[3].replace('.drs', '')
    col_01_file_nm = drsfile_name
    col_02_lot_no = k_val.split(',')[3]
    col_03_lot_date = f'{lot_date[:8]} {lot_date[8:10]}:{lot_date[10:]}'
    col_04_prs_cd = '_'.join(k_val.split(',')[:3])
    col_05_f_val = head_dict['F']
    col_06_k_val = k_val
    col_07_d_val = head_dict['D']
    col_08_v_val = head_dict['V']
    col_09_u_val = head_dict['U']
    col_10_whl_sns_nm = head_dict['A']
    col_11_reg_date = 'CURRENT_DATE'
    col_12_regr_id = 'GUGO'
    col_13_upd_date = 'CURRENT_DATE'
    col_14_updr_id = 'GUGO'
    src_m_string = f"""
        '{col_01_file_nm}', '{col_02_lot_no}', '{col_03_lot_date}', '{col_04_prs_cd}',
        '{col_05_f_val}', '{col_06_k_val}', '{col_07_d_val}', '{col_08_v_val}', '{col_09_u_val}',
        '{col_10_whl_sns_nm}', {col_11_reg_date}, '{col_12_regr_id}', {col_13_upd_date}, '{col_14_updr_id}'
    """
    # print(src_m_string)
    result_m = src_class.m_insert(src_m_string)
    if result_m is False:
        return False
    result_s = src_class.m_select(drsfile_name)
    # src_data_mc ================================================
    sns_list = col_10_whl_sns_nm.upper().split(',')
    for num, sns in enumerate(sns_list):
        result_mc = src_class.mc_insert(result_s, num + 1, sns)
        if result_mc is False:
            return False
    return result_s


# s1 ~ s3 처리
def one_dataset(src_class, file_sno, sensor_list):
    col_01_file_sno = file_sno
    col_02_prs_cd = ''
    col_03_wfr_no = ''
    col_04_chm_no = ''
    col_05_idl_dtt = ''
    step_dataset = []
    step_row_start = 0
    step_row_end = 0
    for val in sensor_list:
        val = val.replace('\n', '').split(',')
        if val[0] == '$S':
            if val[2] == '1':
                col_03_wfr_no = int(val[4])
                col_04_chm_no = int(val[5])
                col_05_idl_dtt = val[6]
            elif val[2] == '2':
                for steprow in step_dataset[step_row_start:step_row_end]:
                    steprow['stp_no'] = val[4]
                    step_row_start = step_row_end
            elif val[2] == '3':
                col_02_prs_cd = val[4].replace('"', '')
                pass
            else:
                print('Error')
                return False
        else:
            step_row_end += 1
            onerow = {
                'file_sno': file_sno,
                'occr_date': val[0],
                'prs_cd': col_02_prs_cd,
                'wfr_no': col_03_wfr_no,
                'chm_no': col_04_chm_no,
                'idl_dtt': col_05_idl_dtt,
                'stp_no': 0,
                'whl_sns_val': ','.join(val[1:]),
            }
            step_dataset.append(onerow)
    for row in step_dataset:
        print(row)


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
    # result_sno = master_dataset(src_m, f_name, drs_index_0)
    result_sno = 1
    # S1 - S3 묶음 (1 ~ n-1)번째
    for i in range(len(flag_list) - 1):
        drs_index_mid = drs_full[flag_list[i]:flag_list[i + 1]]
        one_dataset(src_d, result_sno, drs_index_mid)
        return
    # S1 - S3 묶음 n번째
    drs_index_last = drs_full[flag_list[-1]:]
    one_dataset(src_d, result_sno, drs_index_last)

    # src_data_d =================================================
    # src_data_dc ================================================
    if result_sno is not False:
        conn.commit()
    return


if __name__ == "__main__":
    current_path = os.getcwd()
    drs_list = Path(current_path) / 'test'
    listdir = os.listdir(drs_list)

    flag = 0
    for dirname in listdir:
        if flag == 1:
            break
        file_path = drs_list / dirname
        result_check = []
        work_start = datetime.now()
        listdrs = os.listdir(file_path)
        for index, filename in enumerate(listdrs):
            if flag == 1:
                break
            print(f'{index} :: {filename}')
            drs_path = file_path / filename
            one_start = datetime.now()
            result_final = test_parse(drs_path, filename)
            flag += 1
