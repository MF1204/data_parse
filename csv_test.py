import os
from pathlib import Path
import psycopg2
from datetime import datetime
from drs_parsing import SRC_DATA_M_CRUD, SRC_DATA_D_CRUD


class MNT_RSLT_MNG:
    def __init__(self, cursor):
        self.cursor = cursor

    def mnt_select(self):
        cur = self.cursor
        sql = f"""
            SELECT nextval('sq_mnt_01'::regclass)-1 as sq FROM sq_mnt_01;
        """
        try:
            cur.execute(sql)
            result = cur.fetchone()
        except psycopg2.Error as e:
            print(e)
            print('mnt_select error')
            return False
        return result[0]

    def mnt_sns_select(self):
        cur = self.cursor
        sql = f"""
            SELECT nextval('sq_mnt_sns_01'::regclass)-1 as sq FROM sq_mnt_sns_01;
        """
        try:
            cur.execute(sql)
            result = cur.fetchone()
        except psycopg2.Error as e:
            print(e)
            print('mnt_select error')
            return False
        return result[0]

    def m_insert(self, row):
        cur = self.cursor
        sql = f"""
            INSERT INTO public.tb_ai_mnt_rslt_mng_m(
                mnt_sno,
                lrn_sno,
                file_sno,
                wfr_no,
                alert_yn,
                rmk_cont,
                reg_date,
                regr_id,
                upd_date,
                updr_id
            ) VALUES 
                {row}
            ;
        """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            print("m_insert error")
            return False
        return True

    def d_insert(self, row):
        cur = self.cursor
        sql = f"""
                INSERT INTO public.tb_ai_mnt_rslt_mng_d(
                    mnt_sns_sno,
                    mnt_sno,
                    mnt_dtt_cd,
                    mnt_date,
                    add_rslt_val,
                    whl_rslt_val,
                    reg_date,
                    regr_id,
                    upd_date,
                    updr_id
                ) VALUES 
                    {row}
                ;
            """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            print("m_insert error")
            return False
        return True

    def dc_insert(self, row):
        cur = self.cursor
        sql = f"""
                INSERT INTO public.tb_ai_mnt_rslt_mng_dc(
                    mnt_sns_sno,
                    sns_sno,
                    sns_val
                ) VALUES 
                    {row}
                ;
            """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            print("m_insert error")
            return False
        return True


def csv_parse(csv_path, file_list):
    conn = psycopg2.connect(dbname='dutchboy', user='dutchboy', password='dutchboy2022!', host='3.36.61.69', port=5432)
    cur = conn.cursor()
    mnt = MNT_RSLT_MNG(cur)
    src_m = SRC_DATA_M_CRUD(cur)
    src_d = SRC_DATA_D_CRUD(cur)
    # print(csv_path)
    print(file_list)
    print('rslt_mng_m==============================')
    file_name_common = file_list[0]
    print(f'file: {file_name_common}')
    # mnt_sno = mnt.mnt_select()
    name_arr = file_name_common.split('_')
    common_name = []
    print(name_arr)
    common_name.append(name_arr[1])
    common_name.append(name_arr[2])
    common_name.append(name_arr[8])
    yymmddhhmi = name_arr[3][-2:] + ''.join(name_arr[4:8])  # yymmddhhmi
    common_name.append(yymmddhhmi)
    lrn_sno = 0
    file_nm = '_'.join(common_name) + '.drs'
    file_nm = file_nm[:-8]
    file_sno = src_m.file_sno_select(file_nm)
    wfr_no = name_arr[9]
    alert_yn = 'N'
    rmk_cont = 'NULL'
    reg_date = 'CURRENT_DATE'
    regr_id = 'MF'
    upd_date = 'NULL'
    updr_id = 'NULL'
    # rslt_mnt_m_sql = f"""
    #     {mnt_sno}, {lrn_sno}, {file_sno}, {wfr_no}, '{alert_yn}', '{rmk_cont}', {reg_date}, '{regr_id}', {upd_date}, '{updr_id}'
    # """
    # mnt.m_insert(rslt_mnt_m_sql)
    print(file_nm)
    print(file_sno)
    print(wfr_no)
    timestamp = src_d.time_select(file_sno, wfr_no)
    print('========================================')
    print('rslt_mng_d==============================')
    for file in file_list:
        print(f'file이름:{file}')
        # mnt_sns_sno = mnt.mnt_sns_select()
        dtt_cd = file.split('_')[-1].replace('.csv', '')
        if dtt_cd == 'ground':
            dtt_cd = '22'
        elif dtt_cd == 'predicted':
            dtt_cd = '32'
        elif dtt_cd == 'scores':
            dtt_cd = '72'
        r = open(Path(csv_path) / file, 'r')
        csv_full = r.readlines()
        print(len(timestamp))
        print(len(csv_full))
        for time, line in zip(timestamp, csv_full):
            print(f'time::{time[0]}')
            print(f'line::{line}')

    print('========================================')
    return True


if __name__ == "__main__":
    sep = os.sep
    current_path = os.getcwd().split(sep)[:-1]
    current_path.append('50.Data')
    csv_list = Path(sep.join(current_path)) / 'Sample' / 'MNT_TMIL01_CH#A_MIL1.0-DEP'
    listcsv = os.listdir(csv_list)
    listcsv.remove('threshold.npy')
    result_true = []
    result_false = []
    work_start = datetime.now()

    flag = 0
    m_stack = []
    for index, filename in enumerate(listcsv):
        print(f'index: {index}')
        if flag == 3:
            break

        result = os.path.isdir(csv_list / filename)
        if result is True or filename[-3:] != 'csv':  # 디렉토리인 경우, csv가 아닌 경우 스킵
            result_false.append(False)
            continue
        file_path = csv_list / filename
        one_start = datetime.now()
        m_stack.append(filename)

        if index % 3 == 2:
            result_final = csv_parse(csv_list, m_stack)
            m_stack = []
            one_end = datetime.now()
            if result_final is True:
                print(f'{filename} 파일 처리 시간 : {one_end - one_start}')
                result_true.append(result_final)
            else:
                result_false.append(result_final)

        flag += 1
    print(f'{len(result_false)}건 실패')
    work_end = datetime.now()
    print(f'{len(result_true)}건 처리 시간 : {work_end - work_start}')