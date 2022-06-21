# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import random
import os
from monitoring.monitoring_data_gen import data_gen
import torch
from torch.utils.data import DataLoader, Subset

from util.env import get_device
from util.preprocess import build_loc_net, construct_data
from util.net_struct import get_feature_map, get_fc_graph_struc

from datasets.TimeDataset import TimeDataset
from models.GDN import GDN

from test import test
from evaluate import get_test_full_err_scores, get_test

from pathlib import Path

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)  # FutureWarning 제거
warnings.simplefilter("ignore", UserWarning)


class Main:
    def __init__(self, test_data, train_config, env_config, debug=False):

        self.threshhold = np.load(env_config['threshhold_input'])
        self.threshhold = self.threshhold[0]
        self.env_config = env_config
        self.train_config = train_config
        self.datestr = None

        feature = self.env_config['feature_input']
        monitoring_path = env_config['monitoring_path']
        test_data_path = Path(monitoring_path) / test_data
        test_orig = pd.read_csv(f'{test_data_path}', sep=',', index_col=0)
        test = test_orig

        feature_map = get_feature_map(feature)
        fc_struc = get_fc_graph_struc(feature)

        self.device = get_device()

        fc_edge_index = build_loc_net(fc_struc, list(test.columns), feature_map=feature_map)
        fc_edge_index = torch.tensor(fc_edge_index, dtype=torch.long)

        self.feature_map = feature_map

        test_dataset_indata = construct_data(test, feature_map, labels=test.attack.tolist())

        cfg = {
            'slide_win': train_config['slide_win'],
            'slide_stride': train_config['slide_stride'],
        }

        test_dataset = TimeDataset(test_dataset_indata, fc_edge_index, mode='test', config=cfg)

        self.test_dataset = test_dataset
        self.test_dataloader = DataLoader(test_dataset, batch_size=train_config['batch'],
                                          shuffle=False)

        edge_index_sets = []
        edge_index_sets.append(fc_edge_index)

        self.model = GDN(edge_index_sets, len(feature_map),
                         dim=train_config['dim'],
                         input_dim=train_config['slide_win'],
                         out_layer_num=train_config['out_layer_num'],
                         out_layer_inter_dim=train_config['out_layer_inter_dim'],
                         topk=train_config['topk']
                         ).to(self.device)

    def run(self, wafer_no):
        use_model = self.env_config['model_input']

        self.model.load_state_dict(torch.load(use_model))
        best_model = self.model.to(self.device)

        _, self.test_result = test(best_model, self.test_dataloader)

        '''
         test 5_results
         '''
        predicted = np.array(self.test_result[0])
        ground = np.array(self.test_result[1])
        pre_label = np.array(self.test_result[2])

        predicted_pd = pd.DataFrame(predicted)
        ground_pd = pd.DataFrame(ground)
        pre_label_pd = pd.DataFrame(pre_label)

        save_path = self.env_config['score_output']
        os.makedirs(Path(save_path) / wafer_no, exist_ok=True)
        predicted_csv = Path(save_path) / wafer_no / f'test_predicted.csv'
        ground_csv = Path(save_path) / wafer_no / f'test_ground.csv'
        pre_label_csv = Path(save_path) / wafer_no / f'test_predicted_labels.csv'
        test_score_csv = Path(save_path) / wafer_no / f'test_scores.csv'

        predicted_pd.to_csv(predicted_csv, header=False, index=False)
        ground_pd.to_csv(ground_csv, header=False, index=False)
        pre_label_pd.to_csv(pre_label_csv, header=False, index=False)
        test_scores = self.get_score(self.test_result)
        test_scores = np.array(test_scores).transpose()
        test_scores_pd = pd.DataFrame(test_scores)
        test_scores_pd.to_csv(test_score_csv, header=False, index=False)

        r, c = np.shape(ground)
        k = 1
        self.anomaly_predict = get_test(test_score_csv, int(r), self.threshhold, self.train_config['slide_stride'], 1)

        return self.anomaly_predict

    def seed_worker(_worker_id):
        worker_seed = torch.initial_seed() % 2 ** 32
        np.random.seed(worker_seed)
        random.seed(worker_seed)

    def get_loaders(self, train_dataset, seed, batch, val_ratio=0.1):
        dataset_len = int(len(train_dataset))
        train_use_len = int(dataset_len * (1 - val_ratio))
        val_use_len = int(dataset_len * val_ratio)
        val_start_index = random.randrange(train_use_len)
        indices = torch.arange(dataset_len)

        train_sub_indices = torch.cat([indices[:val_start_index], indices[val_start_index + val_use_len:]])
        train_subset = Subset(train_dataset, train_sub_indices)

        val_sub_indices = indices[val_start_index:val_start_index + val_use_len]
        val_subset = Subset(train_dataset, val_sub_indices)

        train_dataloader = DataLoader(train_subset, batch_size=batch, shuffle=False,
                                      worker_init_fn=lambda id: np.random.seed(id))

        val_dataloader = DataLoader(val_subset, batch_size=batch, shuffle=False,
                                    worker_init_fn=lambda id: np.random.seed(id))

        return train_dataloader, val_dataloader

    def get_score(self, test_result):

        feature_num = len(test_result[0][0])
        np_test_result = np.array(test_result)
        test_labels = np_test_result[2, :, 0].tolist()
        test_scores = get_test_full_err_scores(test_result)

        return test_scores
