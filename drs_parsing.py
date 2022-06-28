import os
from pathlib import Path
import psycopg2
from datetime import datetime


# 데이터베이스 쿼리
class SRC_DATA_M_CRUD:
    def __init__(self, cursor):
        self.cursor = cursor

    def file_sno_select(self, file_nm):
        cur = self.cursor
        sql = f"""
            SELECT file_sno FROM public.tb_ai_src_data_m
            WHERE file_nm LIKE '{file_nm}%';
        """
        try:
            cur.execute(sql)
            result = cur.fetchone()
            if result is None:
                return 0
        except psycopg2.Error as e:
            print(e)
            print('mnt_select error')
            return False
        return result[0]

    def m_select(self):
        cur = self.cursor
        sql = f"""
            SELECT nextval('sq_file_01'::regclass)-1 as sq FROM sq_file_01;
        """
        try:
            cur.execute(sql)
            result = cur.fetchone()
        except psycopg2.Error as e:
            print(e)
            print("m_select error")
            return False
        return result[0]

    def m_insert(self, row_data):
        cur = self.cursor
        sql = f"""
            INSERT INTO public.tb_ai_src_data_m(
                file_sno,
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
            print("m_insert error")
            return False
        return True

    def mc_insert(self, data_row):
        cur = self.cursor
        sql = f"""
            INSERT INTO public.tb_ai_src_data_mc(
                file_sno,
                sns_no,
                sns_nm
	        ) VALUES 
	            {data_row}
	        ;
        """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            print("mc_insert error")
            return False
        return True


# 데이터베이스 쿼리
class SRC_DATA_D_CRUD:
    def __init__(self, cursor):
        self.cursor = cursor

    def d_select(self):
        cur = self.cursor
        sql = f"""
            SELECT nextval('sq_file_sns_01'::regclass)-1 as sq FROM sq_file_sns_01;
        """
        try:
            cur.execute(sql)
            result = cur.fetchone()
        except psycopg2.Error as e:
            print(e)
            print("d_select error")
            return False
        return result[0]

    def d_insert(self, row_data):
        cur = self.cursor
        sql = f"""
            INSERT INTO public.tb_ai_src_data_d(
                file_sns_sno,
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
    	        ) VALUES
    	            {row_data}
    	        ;
            """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            print("d_insert error")
            print(sql)
            return False
        return True

    def dc_insert(self, data_row):
        cur = self.cursor
        sql = f"""
                INSERT INTO public.tb_ai_src_data_dc(
                    file_sns_sno,
                    sns_no,
                    sns_val
        	    ) VALUES 
        	        {data_row}
        	    ;
        """
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print(e)
            print("dc_insert error")
            return False
        return True

    def time_select(self, file_sno, wfr_no):
        cur = self.cursor
        sql = f"""
            SELECT occr_date FROM public.tb_ai_src_data_d
            WHERE file_sno={file_sno}
            AND wfr_no={wfr_no}
            AND idl_dtt='0';
        """
        try:
            cur.execute(sql)
            result = cur.fetchall()
        except psycopg2.Error as e:
            print(e)
            print("d_select error")
            return False
        return result

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

    # pk sequence를 가져오면서 1 증가, 메모리에 저장해서 사용
    # insert, select를 처리하는 시간동안 다른 인스턴스에서 접근할 경우
    # a인스턴스에서 사용하던 시퀀스 값이 트랜잭션 되기 전에
    # b인스턴스에서 먼저 트랜잭션 될 상황을 방지
    result_sno = src_class.m_select()
    src_m_string = f"""
        '{result_sno}', '{col_01_file_nm}', '{col_02_lot_no}', '{col_03_lot_date}', '{col_04_prs_cd}',
        '{col_05_f_val}', '{col_06_k_val}', '{col_07_d_val}', '{col_08_v_val}', '{col_09_u_val}',
        '{col_10_whl_sns_nm}', {col_11_reg_date}, '{col_12_regr_id}', {col_13_upd_date}, '{col_14_updr_id}'
    """
    # print(src_m_string)
    result_m = src_class.m_insert(src_m_string)
    if result_m is False:
        return False
    # src_data_mc ================================================
    # drs파일 1개당 센서 개수만큼 mc테이블 행 추가
    # 문자열로 만들어 multiple rows insert
    sns_list = col_10_whl_sns_nm.upper().split(',')
    sql_row = ''
    for num, sns in enumerate(sns_list):
        sql_row += f"({result_sno}, {num + 1}, '{sns}'),"
    result_mc = src_class.mc_insert(sql_row[:-1])
    return result_sno, col_04_prs_cd


# s1 ~ s3 처리
# $S의 플래그 값 1~3(웨이퍼 1개당 처리 과정) 마다 반복
def one_dataset(src_class, file_sno, sensor_list, master_prs_cd):
    col_01_file_sno = file_sno
    col_02_prs_cd = ''
    col_03_wfr_no = ''
    col_04_chm_no = ''
    col_05_idl_dtt = ''
    col_06_stp_no = 0
    step_dataset = []
    step_row_start = 0
    step_row_end = 0
    for val in sensor_list:
        val = val.replace('\n', '').split(',')
        if val[0] == '$S':
            if val[2] == '1':  # 웨이퍼번호, 챔버번호, 아이들플래그 저장
                col_03_wfr_no = int(val[4])
                col_04_chm_no = int(val[5])
                col_05_idl_dtt = val[6]
            elif val[2] == '2':  # 분기를 만나기 이전까지의 센서데이터들 스텝 번호 채우기
                for steprow in step_dataset[step_row_start:step_row_end]:
                    col_06_stp_no = val[4]  # 날짜가 변경되면서 생기는 out range값에 자동으로 이전 스텝번호 부여
                    steprow['stp_no'] = val[4]
                    step_row_start = step_row_end  # 채우고 나면 start 플래그 이동
            elif val[2] == '3':  #
                col_02_prs_cd = master_prs_cd.split('_')
                col_02_prs_cd[2] = val[4].replace('"', '')
                col_02_prs_cd = '_'.join(col_02_prs_cd)
                for steprow in step_dataset:
                    steprow['prs_cd'] = col_02_prs_cd
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
                'stp_no': col_06_stp_no,
                'whl_sns_val': ','.join(val[1:]),
            }
            step_dataset.append(onerow)
    d_list = []
    dc_list = []
    # 센서데이터가 없는 경우
    if len(step_dataset) == 0:
        return True
    for rindex, row in enumerate(step_dataset):
        print(f'{rindex + 1} : {row}')
        timestamp = row['occr_date'].split(' ')
        temp_yyyymmdd = timestamp[0].split('/')
        yyyymmdd = f'{temp_yyyymmdd[0].zfill(4)}{temp_yyyymmdd[1].zfill(2)}{temp_yyyymmdd[2].zfill(2)}'
        hhmmss = timestamp[1].split(':')
        occr_date = f'{yyyymmdd} {hhmmss[0]}:{hhmmss[1]}:{hhmmss[2]}.{hhmmss[3]}'
        reg_date = 'CURRENT_DATE'
        regr_id = 'GUGO'
        upd_date = 'NULL'
        updr_id = 'NULL'
        result_sno = src_class.d_select()

        sql = f"""
            ({result_sno}, '{row['file_sno']}', '{occr_date}', '{row['prs_cd']}', '{row['wfr_no']}', '{row['chm_no']}',
            '{row['idl_dtt']}', '{row['stp_no']}', '{row['whl_sns_val']}',
            {reg_date}, '{regr_id}', {upd_date}, {updr_id})
        """
        d_list.append(sql)
        # src_data_dc ================================================
        snv_list = row['whl_sns_val'].split(',')
        for num, snv in enumerate(snv_list):
            sql_row = f"({result_sno}, {num + 1}, '{snv}')"
            dc_list.append(sql_row)
    d_sql = ', '.join(d_list)
    dc_sql = ', '.join(dc_list)
    result_d = src_class.d_insert(d_sql)
    result_dc = src_class.dc_insert(dc_sql)
    if result_d is False or result_dc is False:
        print('Error')
        return False
    return True


# 데이터 파싱
def drs_parse(data, f_name):
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
    return True


if __name__ == "__main__":
    sep = os.sep
    current_path = os.getcwd().split(sep)[:-1]
    current_path.append('50.Data')
    drs_list = Path(sep.join(current_path)) / 'Sample'
    listdir = os.listdir(drs_list)
    for name in listdir:
        result = os.path.isdir(drs_list / name)
        if result is not True:  # 디렉토리가 아닌 경우 스킵
            continue
        file_path = drs_list / name
        result_check = []
        work_start = datetime.now()
        listdrs = os.listdir(file_path)
        for index, filename in enumerate(listdrs):
            if filename[-3:] != 'drs':  # drs 파일이 아닌경우 스킵
                continue
            drs_path = file_path / filename
            one_start = datetime.now()
            result_final = drs_parse(drs_path, filename)
            one_end = datetime.now()
            if result_final is True:
                print(f'{filename} 파일 처리 시간 : {one_end - one_start}')
                result_check.append(result_final)
        print(result_check)
        print(f'{len(listdrs) - len(result_check)}건 실패')
        work_end = datetime.now()
        print(f'{len(result_check)}건 처리 시간 : {work_end - work_start}')

