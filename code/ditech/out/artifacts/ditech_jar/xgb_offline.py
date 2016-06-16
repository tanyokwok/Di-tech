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

def evalerror(preds, dtrain):
    labels = dtrain.get_label()
    # return a pair metric_name, result
    # since preds are margin(before logistic transformation, cutoff at 0)
    v = abs( preds/(labels + 1e-6) - 1 )
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
	k = 14.0
	neg = preds < labels
	grad = (( k + 1 )/(labels*k + 1e-6)) * pow( abs(preds/(labels + 1e-5) - 1) + 1e-6, 1/k)
	grad[neg] = - grad[neg]
	grad[ labels == 0 ] = 0
	hess = (k+1)/(labels*labels*k*k + 1e-6)*pow( abs(preds/(labels + 1e-5) -1) + 1e-6, 1/k -1)
	hess[ labels == 0 ] = 0
	return grad, hess

def customed_obj_4(preds, dtrain):
	labels = dtrain.get_label()
	grad = np.zeros( len(labels))
	hess = np.zeros( len(labels))
	k = 14.0
	neg = preds < labels
	grad = (( k + 1 )/k) * pow( abs(preds - labels) + 1e-6, 1/k)
	grad[neg] = - grad[neg]
	grad[ labels == 0 ] = 0
	hess = (k+1)/(k*k)*pow( abs(preds - labels ) + 1e-6, 1/k -1)
	hess[ labels == 0 ] = 0
	return grad, hess

def load(fp, lines):
	f = open(fp)
	for line in f :
		lines.append(line.strip())

def neg_sampling( dtrain, negsample_rate ):
	label = dtrain.get_label()
	l = len(label)
	prob = np.zeros( l )
	ind = ( label == 0 )

	prob[ind] = np.random.random( sum( ind ) )
	indices = np.where( prob <= negsample_rate)
	return dtrain.slice(indices[0].ravel())

def preprocess( dtrain ):
	label = dtrain.get_label()
	lad_label = label
	lad_label[ (label > 18)*(label <75 ) ] = 18
	lad_label[ (label > 75)] = 75 
	dtrain.set_label( lad_label)
	return dtrain

def run(train_fp):

	dtrain = xgb.DMatrix(train_fp)
	label = dtrain.get_label() + 1
	dtrain.set_label( label )	

	params = {}
	with open("xgb_reg.params", 'r') as f:
		params = json.load(f)
	print "[%s] [INFO] params: %s\n" % (t_now(), str(params))

	watchlist={(dtrain,'eval')}
	#model = xgb.train( params, dtrain, params['n_round'], watchlist, obj= customed_obj_2, feval = evalerror)
	model = xgb.train( params, dtrain, params['n_round'],obj= customed_obj_2)
	# model = xgb.train( params, dtrain, params['n_round'])
	#pred = model.predict(dtest, ntree_limit=params['n_round'])

	return model

def predict( model,test_fp, pred_fp, key_fp):
	keys = []
	load(key_fp, keys)
	dtest = xgb.DMatrix(test_fp)
	pred = model.predict(dtest) - 1
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

	# run for offline
	data_fp = "../data/raw/season_1/training_data"
	fs_fp = "./" + sys.argv[1]
	train_fp = fs_fp + "/train_libsvm"
	test_fp = fs_fp + "/test_libsvm"
	val_fp1 = fs_fp + "/val_libsvm1"
	val_fp2 = fs_fp + "/val_libsvm2"
	test_pred_fp = data_fp + "/ans/ans_test.csv"
	val_pred_fp1 = data_fp + "/ans/ans_val1.csv"
	val_pred_fp2 = data_fp + "/ans/ans_val2.csv"
	test_key_fp = fs_fp + "/test_key"
	val_key_fp1 = fs_fp + "/val_key1"
	val_key_fp2 = fs_fp + "/val_key2"

	model = run(train_fp)
	predict(model,test_fp, test_pred_fp,test_key_fp)
	predict(model,val_fp1, val_pred_fp1,val_key_fp1)
	predict(model,val_fp2, val_pred_fp2,val_key_fp2)


	#run for online
	# data_fp = "E:/Di-tech/data/raw/season_1/test_set_1"
	# train_fp = data_fp + "/train_libsvm"
	# test_fp = data_fp + "/test_libsvm"
	# pred_fp = data_fp + "/ans/ans.csv"
	# key_fp = data_fp + "/test_key"

	# run(train_fp, test_fp, pred_fp, key_fp)

	print "[%s] [INFO] gradient boosting for regression from xgboost done\n" % t_now()
