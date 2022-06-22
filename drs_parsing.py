import os
import re
import traceback
from pathlib import Path
import psycopg2
from datetime import datetime


# 데이터베이스 쿼리
class SRC_DATA_M_CRUD:
    def __init__(self, cursor):
        self.cursor = cursor

    def m_select(self, drs_file_name):
        cur = self.cursor
        sql = f"""
            SELECT file_sno FROM public.tb_ai_src_data_m WHERE file_nm='{drs_file_name}'
        """
        try:
            cur.execute(sql)
            result = cur.fetchone()
            print(result[0])
        except psycopg2.Error as e:
            print(e)
            return False
        return result[0]

    def m_insert(self, row_data):
        cur = self.cursor
        sql = f"""
            INSERT INTO public.tb_ai_src_data_m(
                file_nm,
	            lot_no,
                lot_date,
	            prs_cd,
	            f_val,
	            k_val,
	            d_val,
	            v_val,
	            u_val,
	            whl_sns_nm,
	            reg_date,
	            regr_id,
	            upd_date,
	            updr_id
	        ) VALUES (
	            {row_data}
	        );
        """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            return False
        return True

    def mc_insert(self, file_sno, sns_no, sns_rm):
        cur = self.cursor
        sql = f"""
            INSERT INTO public.tb_ai_src_data_mc(
                file_sno,
                sns_no,
                sns_nm
	        ) VALUES (
	            {file_sno},
	            {sns_no},
	            '{sns_rm}'
	        );
        """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            return False
        return True


# 데이터베이스 쿼리
class SRC_DATA_D_CRUD:
    def __init__(self, cursor):
        self.cursor = cursor

    def d_select(self, file_sno):
        cur = self.cursor
        sql = f"""
            SELECT file_sns_sno FROM public.tb_ai_src_data_d WHERE file_sno='{file_sno}'
        """
        try:
            cur.execute(sql)
            result = cur.fetchone()
            print(result[0])
        except psycopg2.Error as e:
            print(e)
            return False
        return result[0]

    def d_insert(self, row_data):
        cur = self.cursor
        sql = f"""
            INSERT INTO public.tb_ai_src_data_d(
                file_sno,
				occr_date, 
				prs_cd,
				wfr_no, 
				chm_no, 
				idl_dtt, 
				stp_no, 
				whl_sns_val, 
   	            reg_date,
    	        regr_id,
    	        upd_date,
    	        updr_id
    	        ) VALUES (
    	            {row_data}
    	        );
            """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            return False
        return True

    def dc_insert(self, file_sno, sns_no, sns_val):
        cur = self.cursor
        sql = f"""
                INSERT INTO public.tb_ai_src_data_dc(
                    file_sno,
                    sns_no,
                    sns_val
    	        ) VALUES (
    	            {file_sno},
    	            {sns_no},
    	            '{sns_val}'
    	        );
            """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            return False
        return True


# 전체 센서 코드 분리
def sns_separate(whl_sns):
    sns_sql_string = ''
    sns_list = whl_sns.upper().split(',')
    for sns in sns_list:
        sns_sql_string += f"'{sns}', "
    if len(sns_list) < 200:
        for i in range(200 - len(sns_list)):
            sns_sql_string += 'NULL, '
    sns_sql_string = sns_sql_string.replace('\n', '')
    return sns_sql_string


# 데이터 파싱
def data_parse(data):
    conn = psycopg2.connect(dbname='dutchboy', user='dutchboy', password='dutchboy2022!', host='3.36.61.69', port=5432)
    cur = conn.cursor()

    r = open(data, 'r')
    drs_full = r.readlines()

    # src_data_m ===========================================
    src_data_m = SRC_DATA_M_CRUD(cur)
    f_val, k_val, d_val, v_val, u_val, whl_sns_nm, act_dtt = '', '', '', '', '', '', ''
    for line in drs_full:
        if line[:1] == '$':
            if line[1:2] == 'F':
                f_val = line[3:].replace('\n', '')
            elif line[1:2] == 'K':
                k_val = line[3:].replace('\n', '')
            elif line[1:2] == 'D':
                d_val = line[3:].replace('\n', '')
            elif line[1:2] == 'V':
                v_val = line[3:].replace('\n', '')
            elif line[1:2] == 'U':
                u_val = line[3:].replace('\n', '')
            elif line[1:2] == 'A':
                whl_sns_nm = line[3:].replace('\n', '')
            elif line[1:2] == 'S':
                act_dtt = line[3:].replace('\n', '')
        else:
            if f_val != '' and k_val != '' and d_val != '' and v_val != '' and u_val != '' and whl_sns_nm != '' and act_dtt != '':
                break
            else:
                continue
    lot_ymd = act_dtt[:10].replace('/', '')
    lot_no = k_val.split(',')[3]
    prs_cd_list = k_val.split(',')[:3]
    prs_cd = '_'.join(prs_cd_list)
    sns_sql_string = sns_separate(whl_sns_nm)
    reg_string = "CURRENT_DATE, 'GUGO', "
    upd_string = "CURRENT_DATE, 'GUGO'"

    # ======================================================

    # src_data_d ===========================================
    src_data_d = SRC_DATA_D_CRUD(cur)
    drs_string = ', '.join(e.replace('\n', '') for e in drs_full)
    snv_list = drs_string.split('$S,')
    lot_number = ''
    wafer_number = ''
    chamber_number = ''
    idle_flag = ''
    step_number = ''
    forflag = True
    for index, i in enumerate(snv_list):
        if forflag is not True:
            break
        if index > 0:  # $S가 나오기 전 = 헤더값
            onerow = i.split(', ')
            action_flag = onerow[0].split(',')  # $S줄의 데이터

            if action_flag[1] == '1':  # $S의 플래그 값이 1인 경우
                lot_number = action_flag[2].replace('"', '')
                wafer_number = action_flag[3]
                chamber_number = action_flag[4]
                idle_flag = action_flag[5]
                step_number = 'NULL'
                if len(onerow) == 1:
                    continue
            elif action_flag[1] == '2':  # $S의 플래그 값이 2인 경우
                chamber_number = action_flag[2]
                step_number = action_flag[3]
                idle_flag = action_flag[4]
            elif action_flag[1] == '3':  # $S의 플래그 값이 3인 경우(데이터 없음)
                chamber_number = action_flag[2]
                step_number = action_flag[3].replace('"', '')  # 이 경우 step_number = recipe_name
                idle_flag = action_flag[4]
                prs_cd_list[2] = step_number
                prs_cd = '_'.join(prs_cd_list)
            else:
                pass
            for snv_row in onerow[1:]:
                if snv_row == '':
                    break
                snv_row_list = snv_row.split(',')
                time = snv_row_list[0].split(' ')
                yyyymmdd = time[0].split('/')
                lot_ymd = f'{yyyymmdd[0].zfill(4)}{yyyymmdd[1].zfill(2)}{yyyymmdd[2].zfill(2)}'
                lot_no = lot_number
                occr_date_unready = time[1].split(':')
                occr_date = f'{lot_ymd} {occr_date_unready[0]}:{occr_date_unready[1]}:{occr_date_unready[2]}.{occr_date_unready[3]}'
                act_dtt = action_flag[1]
                wfr_no = wafer_number
                chm_no = chamber_number
                idl_dtt = idle_flag
                stp_no = step_number
                whl_sns_val = ','.join(snv_row_list[1:])
                snv_query_string = sns_separate(whl_sns_val)
                reg_string = "CURRENT_DATE, 'GUGO', "
                upd_string = "CURRENT_DATE, 'GUGO'"
                sql_query = f"'{lot_ymd}', '{lot_no}', '{prs_cd}', '{occr_date}', '{act_dtt}', '{wfr_no}', '{chm_no}', '{idl_dtt}', '{stp_no}', '{whl_sns_val}', "
                sql_query += snv_query_string + reg_string + upd_string

                result_d = src_data_d.insert(sql_query)

                if result_d is True:
                    continue
                else:
                    forflag = False
                    break

    # ======================================================

    sql_query = f"'{lot_ymd}', '{lot_no}', '{prs_cd}', '{f_val}', '{k_val}', '{d_val}', '{v_val}', '{u_val}', '{whl_sns_nm}', "
    sql_query += sns_sql_string + reg_string + upd_string
    result_m = src_data_m.insert(sql_query)
    if result_m is True:
        conn.commit()
        return True
    else:
        return False


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
            result_final = data_parse(drs_path)

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
