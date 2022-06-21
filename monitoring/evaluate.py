from anomaly_detection.util.data import *
import numpy as np
from sklearn.metrics import precision_score, recall_score, roc_auc_score, f1_score
import pandas as pd


def get_threshhold(input, wafer_length):
    normal_score = pd.read_csv(input, header=None)
    sensor_count = normal_score.shape[1]
    threshhold_arr = []
    histogram_arr = []
    for i in range(0, sensor_count):
        grouped_score = normal_score[[i]].sum(axis=1)
        r = np.shape(grouped_score)
        values = []
        for j in range(0, r[0]-wafer_length, wafer_length):
            values.append(grouped_score.iloc[j:j+wafer_length].sum()/wafer_length)
        thresh = []
        hist, bins = np.histogram(values, 30)
        hist = np.append(hist, np.array([0]))

        histogram = [hist, bins]
        # print(f'{i} histogram : {histogram}')

        for j in range(0, len(hist)):
            if hist[j] < 10:
                thresh.append(bins[j])
            if max(hist) == hist[j]:
                peak_loc = j

        peak = bins[peak_loc]
        thresh2 = []

        for j in range(0, len(thresh)):
            if thresh[j] > peak:
                thresh2.append(thresh[j])
        # threshold = min(thresh2)
        threshhold = max(thresh2)
        # print(f'{i} threshhold : {threshhold}')

        threshhold_arr.append(threshhold)
        histogram_arr.append(histogram)
    # plt.hist(values, bins=30)
    # plt.title("Trained Data - Predicted Data")
    # plt.xlabel("Train MAE loss")
    # plt.ylabel("No of samples")
    # plt.show()
    return threshhold_arr, histogram_arr


def get_test(input, wafer_length, threshhold, slide_window, monitor):
    normal_score = pd.read_csv(input, header=None)
    grouped_score = normal_score[[2, 3, 5]].sum(axis=1)
    r = np.shape(grouped_score)
    values = []
    if monitor == 0:
        values.append(grouped_score.iloc[0:wafer_length - slide_window].sum() / (wafer_length - slide_window))
        for i in range(wafer_length - slide_window + 1, r[0], wafer_length):
            values.append(grouped_score.iloc[i:i + wafer_length].sum() / wafer_length)
    else:
        values.append(grouped_score.iloc[0:-1].sum() / (wafer_length))

    anomalies = values > threshhold
    print("=========================** Result **============================")
    print("Threshhold", threshhold)
    print("Anomaly Score", values)
    print("Number of anomaly samples: ", np.sum(anomalies))
    print("=================================================================")
    print(" ")
    return anomalies


def get_full_err_scores(test_result, val_result):
    np_test_result = np.array(test_result)
    np_val_result = np.array(val_result)

    all_scores = None
    all_normals = None
    feature_num = np_test_result.shape[-1]

    labels = np_test_result[2, :, 0].tolist()

    for i in range(feature_num):
        test_re_list = np_test_result[:2, :, i]
        val_re_list = np_val_result[:2, :, i]

        scores = get_err_scores(test_re_list, val_re_list)
        normal_dist = get_err_scores(val_re_list, val_re_list)

        if all_scores is None:
            all_scores = scores
            all_normals = normal_dist
        else:
            all_scores = np.vstack((
                all_scores,
                scores
            ))
            all_normals = np.vstack((
                all_normals,
                normal_dist
            ))

    return all_scores, all_normals


def get_test_full_err_scores(test_result):
    np_test_result = np.array(test_result)

    all_scores = None
    all_normals = None
    feature_num = np_test_result.shape[-1]

    labels = np_test_result[2, :, 0].tolist()

    for i in range(feature_num):
        test_re_list = np_test_result[:2, :, i]

        scores = get_test_err_scores(test_re_list)

        if all_scores is None:
            all_scores = scores
        else:
            all_scores = np.vstack((
                all_scores,
                scores
            ))
            all_normals = np.vstack((
                all_normals,
            ))
    return all_scores


def get_final_err_scores(test_result, val_result):
    full_scores, all_normals = get_full_err_scores(test_result, val_result, return_normal_scores=True)

    all_scores = np.max(full_scores, axis=0)

    return all_scores


def get_err_scores(test_res, val_res):
    test_predict, test_gt = test_res
    val_predict, val_gt = val_res

    n_err_mid, n_err_iqr = get_err_median_and_iqr(test_predict, test_gt)

    test_delta = np.abs(np.subtract(
        np.array(test_predict).astype(np.float64),
        np.array(test_gt).astype(np.float64)
    ))
    epsilon = 1e-2

    err_scores = (test_delta - n_err_mid) / (np.abs(n_err_iqr) + epsilon)

    smoothed_err_scores = np.zeros(err_scores.shape)
    before_num = 3
    for i in range(before_num, len(err_scores)):
        smoothed_err_scores[i] = np.mean(err_scores[i - before_num:i + 1])

    return test_delta


def get_test_err_scores(test_res):
    test_predict, test_gt = test_res
    n_err_mid, n_err_iqr = get_err_median_and_iqr(test_predict, test_gt)

    test_delta = np.abs(np.subtract(
        np.array(test_predict).astype(np.float64),
        np.array(test_gt).astype(np.float64)
    ))
    epsilon = 1e-2

    err_scores = (test_delta - n_err_mid) / (np.abs(n_err_iqr) + epsilon)

    smoothed_err_scores = np.zeros(err_scores.shape)
    before_num = 3
    for i in range(before_num, len(err_scores)):
        smoothed_err_scores[i] = np.mean(err_scores[i - before_num:i + 1])

    return test_delta


def get_loss(predict, gt):
    return eval_mseloss(predict, gt)


def get_f1_scores(total_err_scores, gt_labels, topk=1):
    print('total_err_scores', total_err_scores.shape)
    # remove the highest and lowest score at each timestep
    total_features = total_err_scores.shape[0]

    # topk_indices = np.argpartition(total_err_scores, range(total_features-1-topk, total_features-1), axis=0)[-topk-1:-1]
    topk_indices = np.argpartition(total_err_scores, range(total_features - topk - 1, total_features), axis=0)[-topk:]

    topk_indices = np.transpose(topk_indices)

    total_topk_err_scores = []
    topk_err_score_map = []
    # topk_anomaly_sensors = []

    for i, indexs in enumerate(topk_indices):
        sum_score = sum(
            score for k, score in enumerate(sorted([total_err_scores[index, i] for j, index in enumerate(indexs)])))

        total_topk_err_scores.append(sum_score)

    final_topk_fmeas = eval_scores(total_topk_err_scores, gt_labels, 400)

    return final_topk_fmeas


def get_val_performance_data(total_err_scores, normal_scores, gt_labels, topk=1):
    total_features = total_err_scores.shape[0]

    topk_indices = np.argpartition(total_err_scores, range(total_features - topk - 1, total_features), axis=0)[-topk:]

    total_topk_err_scores = []
    topk_err_score_map = []

    total_topk_err_scores = np.sum(np.take_along_axis(total_err_scores, topk_indices, axis=0), axis=0)

    thresold = np.max(normal_scores)

    pred_labels = np.zeros(len(total_topk_err_scores))
    pred_labels[total_topk_err_scores > thresold] = 1

    for i in range(len(pred_labels)):
        pred_labels[i] = int(pred_labels[i])
        gt_labels[i] = int(gt_labels[i])

    pre = precision_score(gt_labels, pred_labels)
    rec = recall_score(gt_labels, pred_labels)

    f1 = f1_score(gt_labels, pred_labels)

    auc_score = roc_auc_score(gt_labels, total_topk_err_scores)

    return f1, pre, rec, auc_score, thresold


def get_best_performance_data(total_err_scores, gt_labels, topk=1):
    total_features = total_err_scores.shape[0]

    # topk_indices = np.argpartition(total_err_scores, range(total_features-1-topk, total_features-1), axis=0)[-topk-1:-1]
    topk_indices = np.argpartition(total_err_scores, range(total_features - topk - 1, total_features), axis=0)[-topk:]

    total_topk_err_scores = []
    topk_err_score_map = []

    total_topk_err_scores = np.sum(np.take_along_axis(total_err_scores, topk_indices, axis=0), axis=0)

    final_topk_fmeas, thresolds = eval_scores(total_topk_err_scores, gt_labels, 400, return_thresold=True)

    th_i = final_topk_fmeas.index(max(final_topk_fmeas))
    thresold = thresolds[th_i]

    pred_labels = np.zeros(len(total_topk_err_scores))
    pred_labels[total_topk_err_scores > thresold] = 1

    for i in range(len(pred_labels)):
        pred_labels[i] = int(pred_labels[i])
        gt_labels[i] = int(gt_labels[i])

    pre = precision_score(gt_labels, pred_labels)
    rec = recall_score(gt_labels, pred_labels)

    auc_score = roc_auc_score(gt_labels, total_topk_err_scores)

    return max(final_topk_fmeas), pre, rec, auc_score, thresold

