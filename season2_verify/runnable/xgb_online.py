#! /usr/bin/python

from sklearn import ensemble
from sklearn.datasets import load_svmlight_file
import xgboost as xgb
import numpy as np  
import sys
import json
import math
import time
import random

def t_now():
	return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))

def evalerror(preds, dtrain):
    labels = dtrain.get_label()
    # return a pair metric_name, result
    # since preds are margin(before logistic transformation, cutoff at 0)
    v = ( preds*preds/(labels*labels + 1e-6) - 2*preds/(labels+1e-6) + 1 )
    ind = ( labels > 0 )
    v = v * ind

    return 'error', float( sum( v )) / sum(ind)

def customed_obj_1(preds, dtrain):
    labels = dtrain.get_label()
    grad = 1 / ( labels + 1e-6)

    grad[ preds < labels ] = grad[ preds < labels ] * -1
    grad[ abs( preds - labels ) < 1e-6 ] = 0
    grad[ labels == 0 ] = 0
    hess = 1 / (labels + 1e-6) 
    hess[labels == 0 ] = 0

    # print "grad: " + str( grad )
    # print "hess: " + str( hess )
    return grad, hess


def customed_obj_2(preds, dtrain):
	labels = dtrain.get_label()

	grad = 2*preds/(labels*labels + 1e-6) - 2/(labels + 1e-6)
	# grad = grad / 4
	hess = 2/(labels*labels + 1e-6)
	# hess = hess /4
	return grad, hess


def customed_obj_3(preds, dtrain):
	labels = dtrain.get_label()
	grad = np.zeros( len(labels))
	hess = np.zeros( len(labels))
	k = 2.0
	neg = preds < labels
	grad = (( k + 1 )/(labels*k)) * pow( abs(preds/(labels) - 1), 1/k)
	grad[neg] = - grad[neg]
	hess = (k+1)/(labels*labels*k*k)*pow( abs(preds/(labels) -1) + 1e-6, 1/k -1)
	return grad, hess

def load(fp, lines):
	f = open(fp)
	for line in f :
		lines.append(line.strip())

def run(train_fp):

	dtrain = xgb.DMatrix(train_fp)

	params = {}
	with open("xgb_reg.params", 'r') as f:
		params = json.load(f)

	params['max_depth'] = params['max_depth'] + random.randint(-1, 1)
	params['eta'] = params['eta'] +  (random.random() - 0.5) / 50
	params['subsample'] = params['subsample'] +  (random.random() - 0.5) / 5
	params['colsample_bytree'] = params['colsample_bytree'] +  (random.random() - 0.5) / 5
	params['seed'] = int(time.time())

	print "[%s] [INFO] params: %s\n" % (t_now(), str(params))

	watchlist={(dtrain,'eval')}
	
	label = dtrain.get_label() + 1
	dtrain.set_label( label )
	# model = xgb.train( params, dtrain, params['n_round'], watchlist, obj= customed_obj_2, feval = evalerror)
	model = xgb.train( params, dtrain, params['n_round'],obj= customed_obj_3)
	# model = xgb.train( params, dtrain, params['n_round'])
	#pred = model.predict(dtest, ntree_limit=params['n_round'])

	return model

def predict( model,test_fp, pred_fp, key_fp):
	keys = []
	load(key_fp, keys)
	dtest = xgb.DMatrix(test_fp)
	pred = model.predict(dtest) -1
	pred[ pred < 1 ] = 1	
	f = open(pred_fp, 'w')
	for i in range(len(keys)):
		f.write(keys[i] + "," + str(pred[i]) + "\n")
	f.close()

if __name__ == "__main__":
	print "[%s] [INFO] gradient boosting for regression from xgboost ..." % t_now()

	# if (3 != len(sys.argv)):
	# 	print "[%s] [ERROR]: check parameters!" % t_now()
	# 	sys.exit(1)

	# run for online
	fs_fp = sys.argv[1] 
	train_fp = fs_fp + "/train_libsvm"
	test_fp = fs_fp + "/test_libsvm"
	# test_pred_fp = fs_fp + "/ans.csv"
	test_pred_fp = fs_fp + "/ans/ans" + sys.argv[2] + ".csv"
	test_key_fp = fs_fp + "/test_key"

	model = run(train_fp)
	predict(model,test_fp, test_pred_fp,test_key_fp)


	print "[%s] [INFO] gradient boosting for regression from xgboost done\n" % t_now()
