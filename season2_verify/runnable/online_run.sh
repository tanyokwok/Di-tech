mkdir $1/ans
for((i=0;i <24; i=i+1));
do
python xgb_online.py $1 $i
done

java -cp ditech.jar ditech.time_slices.WeightedSum --path $1 --count 24
