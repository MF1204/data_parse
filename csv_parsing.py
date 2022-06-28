import os
from pathlib import Path
import psycopg2
from datetime import datetime
import drs_parsing


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

    def file_select(self, filename):
        cur = self.cursor
        sql = f"""
            SELECT file_sno FROM public.tb_ai_src_data_m
            WHERE file_nm='';
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


def csv_parse(csv_path, file_name):
    conn = psycopg2.connect(dbname='dutchboy', user='dutchboy', password='dutchboy2022!', host='3.36.61.69', port=5432)
    cur = conn.cursor()
    mnt = MNT_RSLT_MNG(cur)

    # mnt.file_select(file_name)
    # r = open(data, 'r')
    # csv_full = r.readlines()
    print(f'path: {csv_path}')
    print(f'file: {file_name}')
    return True


if __name__ == "__main__":
    sep = os.sep
    current_path = os.getcwd().split(sep)[:-1]
    current_path.append('50.Data')
    csv_list = Path(sep.join(current_path)) / 'Sample' / 'monitoring_0627'
    listcsv = os.listdir(csv_list)
    result_true = []
    result_false = []
    work_start = datetime.now()
    for index, filename in enumerate(listcsv):
        result = os.path.isdir(csv_list / filename)
        if result is True or filename[-3:] != 'csv':  # 디렉토리인 경우, csv가 아닌 경우 스킵
            result_false.append(False)
            continue
        file_path = csv_list / filename
        one_start = datetime.now()
        result_final = csv_parse(file_path, filename)
        one_end = datetime.now()
        if result_final is True:
            print(f'{filename} 파일 처리 시간 : {one_end - one_start}')
            result_true.append(result_final)
        else:
            result_false.append(result_final)
    print(f'{len(result_false)}건 실패')
    work_end = datetime.now()
    print(f'{len(result_true)}건 처리 시간 : {work_end - work_start}')