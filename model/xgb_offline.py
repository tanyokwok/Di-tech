#! /usr/bin/python

from sklearn import ensemble
from sklearn.datasets import load_svmlight_file
import xgboost as xgb
import numpy as np  
import sys
import json
import math
import time

def t_now():
	return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))


def customed_obj_1(preds, dtrain):
    labels = dtrain.get_label()
    grad = 1 / ( labels + 1e-6)

    grad[ preds < labels ] = grad[ preds < labels ] * -1
    grad[ abs( preds - labels ) < 1e-6 ] = 0
    hess = np.zeros( len(preds)) 
    return grad, hess

def customed_obj_2(preds, dtrain):
	labels = dtrain.get_label()

	grad = 2*preds/(labels*labels + 1e-6) - 2/(labels + 1e-6)
	hess = 2/(labels*labels + 1e-6)
	return grad, hess

def load(fp, lines):
	f = open(fp)
	for line in f :
		lines.append(line.strip())

def run(train_fp, test_fp, pred_fp, key_fp):

	keys = []
	load(key_fp, keys)

	dtrain = xgb.DMatrix(train_fp)
	dtest = xgb.DMatrix(test_fp)

	params = {}
	with open("xgb_reg.params", 'r') as f:
		params = json.load(f)
	print "[%s] [INFO] params: %s\n" % (t_now(), str(params))

	# model = xgb.train( params, dtrain, params['n_round'], obj= customed_obj_2)
	model = xgb.train( params, dtrain, params['n_round'])
	#pred = model.predict(dtest, ntree_limit=params['n_round'])
	pred = model.predict(dtest)

	f = open(pred_fp, 'w')
	for i in range(len(keys)):
		f.write(keys[i] + "," + str(pred[i]) + "\n")
	f.close()

	return 0

if __name__ == "__main__":
	print "[%s] [INFO] gradient boosting for regression from xgboost ..." % t_now()

	# if (3 != len(sys.argv)):
	# 	print "[%s] [ERROR]: check parameters!" % t_now()
	# 	sys.exit(1)

	# run for offline
	data_fp = "../data/raw/season_1/training_data"
	train_fp = data_fp + "/train_libsvm"
	test_fp = data_fp + "/test_libsvm"
	pred_fp = data_fp + "/ans/ans.csv"
	key_fp = data_fp + "/test_key"

	run(train_fp, test_fp, pred_fp, key_fp)

	#run for online
	# data_fp = "E:/Di-tech/data/raw/season_1/test_set_1"
	# train_fp = data_fp + "/train_libsvm"
	# test_fp = data_fp + "/test_libsvm"
	# pred_fp = data_fp + "/ans/ans.csv"
	# key_fp = data_fp + "/test_key"

	# run(train_fp, test_fp, pred_fp, key_fp)

	print "[%s] [INFO] gradient boosting for regression from xgboost done\n" % t_now()
