from monitoring import main_test
from monitoring import monitoring_data_gen
import os
from pathlib import Path
import torch
import numpy as np
import random


def inference_run(train_config, env_config):
    random_seed = 0
    torch.manual_seed(random_seed)
    torch.cuda.manual_seed(random_seed)
    torch.cuda.manual_seed_all(random_seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    np.random.seed(random_seed)
    random.seed(random_seed)

    os.environ['PYTHONHASHSEED'] = str(random_seed)

    drs_input = env_config['drs_input']
    minmax_input = env_config['minmax_input']

    monitoring_data_gen.data_gen(drs_input, minmax_input)
    file_ext = r'.csv'
    raw_list = [file for file in os.listdir(get_monitoring_path(drs_input)) if file.endswith(file_ext)]
    print(raw_list)
    anomaly_list = []
    for file in raw_list:
        main = main_test.Main(file, train_config, env_config, debug=False)
        anomaly_decision = main.run(file)
        lot_info = file.split(".")
        if anomaly_decision[0]:
            anomaly_list.append(lot_info[0])
        csv_file = Path(env_config['monitoring_path']) / str(file)
        # os.remove(csv_file)

    print('===========Error Report==========')
    if not anomaly_list:
        print('There is no anomaly')
    else:
        print(anomaly_list)
    print(f'Total Number of anomaly samples: {len(anomaly_list)}')


def get_monitoring_path(drs_file_path):

    recipe_path = get_recipe_path(drs_file_path)
    monitoring_front = Path('static') / 'data' / 'sensor'
    monitoring_back = 'monitoring'

    score_path = os.path.join(BASE_DIR, monitoring_front, recipe_path, monitoring_back)
    return score_path