import re
from datetime import datetime
import pandas as pd
import joblib
import random
from pathlib import Path
import os


def get_header(drs_file):
    """원본 파일(*.drs)에서 header 정보 불러오기"""
    regex = re.compile(r'\$A.+\n')
    with open(drs_file, 'r') as f:
        data = f.read()
        header = regex.search(data).group().upper()
    # dataframe 생성시 index 이름을 'TIME'으로 임시 저장
    header = header.replace('$A', 'TIME').replace(' ', '_')

    return header


def get_columns(drs_file):
    """원본 파일(*.drs)의 header 정보를 리스트 형태로 반환"""
    header = get_header(drs_file)
    if header[-1] == '\n':
        header = header[:-1]
    columns = header.split(',')[1:]

    return columns


def data_gen(drs_file, minmax_input):
    start_pt = re.compile(r'\$S,(\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}:\d{3}),(\d+),"(.+)",(\d+),(\d+),(0)')
    # group(0): 전체 text
    # group(1): 년/월/일 시:분:초:밀리초
    # group(3): lot 번호
    # group(4): wafer 번호
    end_pt = re.compile(r'\$S,(\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}:\d{3}),(\d+),(\d+),"(.+)",(0)')
    sensor_data = re.compile(r'^\d+.+\n')

    start_idx = []  # 각 wafer의 센서 데이터 시작점의 인덱스 저장
    end_idx = []  # 각 wafer의 센서 데이터 종료점의 인덱스 저장
    name_lst = []  # csv file 이름 저장

    sep = os.sep
    header = get_header(drs_file)
    recipe_name = drs_file.split(sep)[-3]
    monitoring_path = sep.join(drs_file.split(sep)[:-1])

    with open(drs_file, 'r') as f:
        lines = f.readlines()
        for idx, line in enumerate(lines):

            if start_pt.search(line):
                start_idx.append(idx)

                date, time = start_pt.search(line).group(1).split()
                year, month, day = date.split('/')
                hour, minute, second, milisec = time.split(':')
                lot_no = start_pt.search(line).group(3)
                wafer_no = start_pt.search(line).group(4).zfill(2)
                csv_name = f'{year}_{month}_{day}_{hour}_{minute}_{lot_no}_{wafer_no}.csv'
                csv_path = str(Path(monitoring_path) / csv_name)
                with open(csv_path, 'w') as g:
                    g.write(header)  # csv file의 맨 첫줄에 header 정보 저장

                name_lst.append(csv_name)

            if end_pt.search(line):
                end_idx.append(idx)

        for idx, line in enumerate(lines):
            for i in range(len(start_idx)):
                # start_pt, end_pt 구간 내에 있는 sensor_data만 csv file에 저장
                if (start_idx[i] < idx < end_idx[i]) and (sensor_data.search(line)):
                    data = sensor_data.search(line).group()
                    with open(f'{monitoring_path}{sep}{name_lst[i]}', 'a') as g:
                        g.write(data)

    dt_parser = lambda x: datetime.strptime(x, '%Y/%m/%d %H:%M:%S:%f')
    for csv_file in name_lst:
        df = pd.read_csv(f'{monitoring_path}{sep}{csv_file}', index_col=0, parse_dates=['TIME'],
                         date_parser=dt_parser)
        df = df.resample(rule='S').last()
        df = df.interpolate('zero')
        df = df.reset_index(drop=True)
        df.columns = ['s1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's9', 's10', 's11', 's12', 's13']

        scaler = joblib.load(minmax_input)
        test_norm = scaler.transform(df)
        test_data = pd.DataFrame(test_norm, columns=df.columns, index=df.index)

        # test_data.columns = ['s1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's9', 's10', 's11', 's12', 's13']

        test_data['attack'] = random.choices([0, 1], weights=[3, 1], k=len(test_data))
        test_data.index.name = 'timestamp'
        test_data.to_csv(f'{monitoring_path}{sep}{csv_file}')

