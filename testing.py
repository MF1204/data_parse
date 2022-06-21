import os
import re
import traceback
from pathlib import Path
import psycopg2
from datetime import datetime


# 데이터베이스 쿼리
class SRC_DATA_M_CRUD:
    def __init__(self, cursor):
        self.tablename = 'tb_ai_src_data_m'
        self.cursor = cursor

    def insert(self, row_data):
        cur = self.cursor
        sql = f"""
            INSERT INTO public.{self.tablename}(
                lot_ymd,
	            lot_no,
	            prs_cd,
	            f_val,
	            k_val,
	            d_val,
	            v_val,
	            u_val,
	            whl_sns_nm,
	            snn001,
	            snn002,
	            snn003, 
	            snn004, 
	            snn005, 
	            snn006, 
	            snn007, 
	            snn008, 
	            snn009, 
	            snn010, 
                snn011, 
                snn012, 
                snn013, 
                snn014, 
                snn015, 
                snn016, 
                snn017, 
                snn018, 
                snn019, 
                snn020, 
                snn021, 
                snn022, 
                snn023, 
                snn024, 
                snn025, 
                snn026, 
                snn027, 
                snn028, 
                snn029, 
                snn030, 
                snn031, 
                snn032, 
                snn033, 
                snn034, 
                snn035, 
                snn036, 
                snn037, 
                snn038, 
                snn039, 
                snn040, 
                snn041, 
                snn042, 
                snn043, 
                snn044, 
                snn045, 
                snn046, 
                snn047, 
                snn048, 
                snn049, 
                snn050, 
                snn051, 
                snn052, 
                snn053, 
                snn054, 
                snn055, 
                snn056, 
                snn057, 
                snn058, 
                snn059, 
                snn060, 
                snn061, 
                snn062, 
                snn063, 
                snn064, 
                snn065, 
                snn066, 
                snn067, 
                snn068, 
                snn069, 
                snn070, 
                snn071, 
                snn072, 
                snn073, 
                snn074, 
                snn075, 
                snn076, 
                snn077, 
                snn078, 
                snn079, 
                snn080, 
                snn081, 
                snn082, 
                snn083, 
                snn084, 
                snn085, 
                snn086, 
                snn087, 
                snn088, 
                snn089, 
                snn090, 
                snn091, 
                snn092, 
                snn093, 
                snn094, 
                snn095, 
                snn096, 
                snn097, 
                snn098, 
                snn099, 
                snn100, 
                snn101, 
                snn102, 
                snn103, 
                snn104, 
                snn105, 
                snn106, 
                snn107, 
                snn108, 
                snn109, 
                snn110, 
                snn111, 
                snn112, 
                snn113, 
                snn114, 
                snn115, 
                snn116, 
                snn117, 
                snn118, 
                snn119, 
                snn120, 
                snn121, 
                snn122, 
                snn123, 
                snn124, 
                snn125, 
                snn126, 
                snn127, 
                snn128, 
                snn129, 
                snn130, 
                snn131, 
                snn132, 
                snn133, 
                snn134, 
                snn135, 
                snn136, 
                snn137, 
                snn138, 
                snn139, 
                snn140, 
                snn141, 
                snn142, 
                snn143, 
                snn144, 
                snn145, 
                snn146, 
                snn147, 
                snn148, 
                snn149, 
                snn150, 
                snn151, 
                snn152, 
                snn153, 
                snn154, 
                snn155, 
                snn156, 
                snn157, 
                snn158, 
                snn159, 
                snn160, 
                snn161, 
                snn162, 
                snn163, 
                snn164, 
                snn165, 
                snn166, 
                snn167, 
                snn168, 
                snn169, 
                snn170, 
                snn171, 
                snn172, 
                snn173, 
                snn174, 
                snn175, 
                snn176, 
                snn177, 
                snn178, 
                snn179, 
                snn180, 
                snn181, 
                snn182, 
                snn183, 
                snn184, 
                snn185, 
                snn186, 
                snn187, 
                snn188, 
                snn189, 
                snn190, 
                snn191, 
                snn192, 
                snn193, 
                snn194, 
                snn195, 
                snn196, 
                snn197, 
                snn198, 
                snn199, 
                snn200, 
	            reg_date,
	            regr_id,
	            upd_date,
	            updr_id
	        ) VALUES (
	            {row_data}
	        );;
        """
        try:
            cur.execute(sql)
        except Exception:
            traceback.print_exc()
            return False
        return True


# 데이터베이스 쿼리
class SRC_DATA_D_CRUD:
    def __init__(self, cursor):
        self.tablename = 'tb_ai_src_data_d'
        self.cursor = cursor

    def insert(self, row_data):
        cur = self.cursor
        sql = f"""
            INSERT INTO public.{self.tablename}(
                lot_ymd, 
				lot_no,
				prs_cd,
				occr_date, 
				act_dtt, 
				wfr_no, 
				chm_no, 
				idl_dtt, 
				stp_no, 
				whl_sns_val, 
				snv001, 
				snv002, 
				snv003, 
   	            snv004, 
   	            snv005, 
   	            snv006, 
   	            snv007, 
   	            snv008, 
   	            snv009, 
   	            snv010, 
                snv011, 
                snv012, 
                snv013, 
                snv014, 
                snv015, 
                snv016, 
                snv017, 
                snv018, 
                snv019, 
                snv020, 
                snv021, 
                snv022, 
                snv023, 
                snv024, 
                snv025, 
                snv026, 
                snv027, 
                snv028, 
                snv029, 
                snv030, 
                snv031, 
                snv032, 
                snv033, 
                snv034, 
                snv035, 
                snv036, 
                snv037, 
                snv038, 
                snv039, 
                snv040, 
                snv041, 
                snv042, 
                snv043, 
                snv044, 
                snv045, 
                snv046, 
                snv047, 
                snv048, 
                snv049, 
                snv050, 
                snv051, 
                snv052, 
                snv053, 
                snv054, 
                snv055, 
                snv056, 
                snv057, 
                snv058, 
                snv059, 
                snv060, 
                snv061, 
                snv062, 
                snv063, 
                snv064, 
                snv065, 
                snv066, 
                snv067, 
                snv068, 
                snv069, 
                snv070, 
                snv071, 
                snv072, 
                snv073, 
                snv074, 
                snv075, 
                snv076, 
                snv077, 
                snv078, 
                snv079, 
                snv080, 
                snv081, 
                snv082, 
                snv083, 
                snv084, 
                snv085, 
                snv086, 
                snv087, 
                snv088, 
                snv089, 
                snv090, 
                snv091, 
                snv092, 
                snv093, 
                snv094, 
                snv095, 
                snv096, 
                snv097, 
                snv098, 
                snv099, 
                snv100, 
                snv101, 
                snv102, 
                snv103, 
                snv104, 
                snv105, 
                snv106, 
                snv107, 
                snv108, 
                snv109, 
                snv110, 
                snv111, 
                snv112, 
                snv113, 
                snv114, 
                snv115, 
                snv116, 
                snv117, 
                snv118, 
                snv119, 
                snv120, 
                snv121, 
                snv122, 
                snv123, 
                snv124, 
                snv125, 
                snv126, 
                snv127, 
                snv128, 
                snv129, 
                snv130, 
                snv131, 
                snv132, 
                snv133, 
                snv134, 
                snv135, 
                snv136, 
                snv137, 
                snv138, 
                snv139, 
                snv140, 
                snv141, 
                snv142, 
                snv143, 
                snv144, 
                snv145, 
                snv146, 
                snv147, 
                snv148, 
                snv149, 
                snv150, 
                snv151, 
                snv152, 
                snv153, 
                snv154, 
                snv155, 
                snv156, 
                snv157, 
                snv158, 
                snv159, 
                snv160, 
                snv161, 
                snv162, 
                snv163, 
                snv164, 
                snv165, 
                snv166, 
                snv167, 
                snv168, 
                snv169, 
                snv170, 
                snv171, 
                snv172, 
                snv173, 
                snv174, 
                snv175, 
                snv176, 
                snv177, 
                snv178, 
                snv179, 
                snv180, 
                snv181, 
                snv182, 
                snv183, 
                snv184, 
                snv185, 
                snv186, 
                snv187, 
                snv188, 
                snv189, 
                snv190, 
                snv191, 
                snv192, 
                snv193, 
                snv194, 
                snv195, 
                snv196, 
                snv197, 
                snv198, 
                snv199, 
                snv200, 
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
        except Exception:
            print(row_data)
            traceback.print_exc()
            return False
        return True


# 전체 센서 코드 분리
def sns_separate(whl_sns_nm):
    sns_sql_string = ''
    sns_list = whl_sns_nm.upper().split(',')
    for sns in sns_list:
        sns_sql_string += f"'{sns}', "
    if len(sns_list) < 200:
        for i in range(200 - len(sns_list)):
            sns_sql_string += 'NULL, '
    sns_sql_string = sns_sql_string.replace('\n', '')
    return sns_sql_string


# 데이터 파싱
def test_parse(data):
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
    result_list = []
    lot_number = ''
    wafer_number = ''
    chamber_number = ''
    idle_flag = ''
    step_number = ''
    for index, i in enumerate(snv_list):
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
                step_number = action_flag[3]  # 이 경우 step_number = recipe_name
                idle_flag = action_flag[4]
                prs_cd_list[2] = action_flag[3].replace('"', '')
                prs_cd = '_'.join(prs_cd_list)
                continue
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
                try:
                    result_d = src_data_d.insert(sql_query)
                    # result_list.append(result_d)
                except Exception as e:
                    print(f'src_d : {sql_query}')
                    traceback.print_exc(e)
                    return False

    # ======================================================

    sql_query = f"'{lot_ymd}', '{lot_no}', '{prs_cd}', '{f_val}', '{k_val}', '{d_val}', '{v_val}', '{u_val}', '{whl_sns_nm}', "
    sql_query += sns_sql_string + reg_string + upd_string
    # result_m = src_data_m.insert(sql_query)
    print(f'src_m : {sql_query}')

    # if result_m is True and False not in result_list:
    #     conn.commit()
    #     return True

    return False


if __name__ == "__main__":
    current_path = os.getcwd()
    drs_list = Path(current_path) / 'test'
    listdir = os.listdir(drs_list)
    print(listdir)
    for dirname in listdir:
        file_path = drs_list / dirname
        result_check = []
        work_start = datetime.now()
        listdrs = os.listdir(file_path)
        for index, filename in enumerate(listdrs):
            print(f'{index} :: {filename}')
            drs_path = file_path / filename
            one_start = datetime.now()
            result_final = test_parse(drs_path)

            if result_final is False:
                break
            one_end = datetime.now()
            print(f'{filename} 파일 처리 시간 : {one_end - one_start}')
            result_check.append(result_final)
        if False not in result_check:
            work_end = datetime.now()
            print(f'{len(result_check)}건 처리 시간 : {work_end - work_start}')
        else:
            print(result_check)
