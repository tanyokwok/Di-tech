# encoding=UTF-8
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
    print grad
    hess = np.zeros( len(preds))  + 0.1
    return grad, hess

def customed_obj_2(preds, dtrain):
	labels = dtrain.get_label()

	grad = 2*preds/(labels*labels + 1e-6) - 2/(labels + 1e-6)
	grad = grad / 4
	hess = 2/(labels*labels + 1e-6)
	hess = hess/4
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
	indices = np.where( prob < negsample_rate)
	return dtrain.slice(indices[0].ravel())

def copy_sampling( dtrain, cp ):
	label = dtrain.get_label()

	l = len(label)

	print "label length: " + str( l )	
	ind_list = range(0,l)

	# 分段拷贝样本	
	# ind_arr = []
	# ind_arr.append( np.where( label == 2 )[0].ravel() )
	# ind_arr.append(np.where( (label <= 10) * (label > 2) )[0].ravel() )
	# ind_arr.append(np.where( (label <= 30) *(label > 10) )[0].ravel() )
	# ind_arr.append(np.where( (label <= 100) * (label > 30) )[0].ravel() )
	# for j in range(0, len(cp) ):
		# for i in range(0,cp[j]):
			# ind_list.extend( ind_arr[ j ] )


	ind_arr = np.where( label >= 1 )[0].ravel()
	for i in range(0,cp):
		ind_list.extend( ind_arr )

	indices = np.array( ind_list )
	print "indices length:" + str( len( indices ) )
	return dtrain.slice( indices )

def get_binary( dtrain ):
	label = dtrain.get_label()
	l = len( label)

	dtrain_pos = dtrain.slice( np.array( range(0,l) ))	
	# dtrain_pos = dtrain.slice( np.where( label > 0 )[0].ravel() )
	label[ label > 0 ] = 1
	print "binary labels:" + str( label  )
	dtrain.set_label( label )
	return dtrain,dtrain_pos

def run(train_fp, test_fp, pred_fp, key_fp):

	params_reg = {}
	with open("xgb_reg.params", 'r') as f:
		params_reg = json.load(f)
	print "[%s] [INFO] params: %s\n" % (t_now(), str(params_reg))

	params_binary = {}
	with open("xgb_class.params", 'r') as f:
		params_binary = json.load(f)
	print "[%s] [INFO] params: %s\n" % (t_now(), str(params_binary))


	keys = []
	load(key_fp, keys)

	dtrain = xgb.DMatrix(train_fp)
	dtrain_binary,dtrain = get_binary( dtrain )
	# dtrain = neg_sampling( dtrain, params['negsample_rate'] )
	#dtrain_binary = copy_sampling( dtrain_binary, 20 )
	print "label length:" + str( len( dtrain.get_label() ) )
	dtest = xgb.DMatrix(test_fp)
	
	model = xgb.train( params_reg, dtrain, params_reg['n_round'], obj= customed_obj_3)
	model_binary = xgb.train( params_binary, dtrain_binary, params_binary['n_round'] )
	# model = xgb.train( params_reg, dtrain, params_reg['n_round'])
	#pred = model.predict(dtest, ntree_limit=params['n_round'])
	pred = model.predict(dtest)
	pred_binary = model_binary.predict( dtest )
#	print "predict binary:" + str( pred_binary )

	pred[ pred_binary < 0.8 ] = 1
	pred[ pred < 1 ] = 1
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

	# run for online
	data_fp = "../data/raw/season_1/test_set_1"
	train_fp = data_fp + "/train_libsvm"
	test_fp = data_fp + "/test_libsvm"
	pred_fp = data_fp + "/ans/ans.csv"
	key_fp = data_fp + "/test_key"

	run(train_fp, test_fp, pred_fp, key_fp)


	print "[%s] [INFO] gradient boosting for regression from xgboost done\n" % t_now()
