id_exp=7
N=100
python3 setupMySQL.py
echo setup
python3 storeSeed.py seed.csv $id_exp
echo storeSeed
python3 storeExpertTypes.py ChessExpertType.csv $id_exp
echo store exp
python3 twitter.py -x $id_exp -n $N seeds
echo twitter
python3 myDandelion.py $id_exp
echo dand seed
python3 createFeatureVector.py $id_exp seeds
echo fv seed
python3 createInstanceVector.py $id_exp seeds
echo f seeds
python3 createCentroid.py $id_exp
echo centroid
python3 createCentroidInstance.py $id_exp
echo centroid inst
python3 listCandidate.py $id_exp
echo list
python3 twitter.py -x $id_exp -n $N candidates
echo twitter cand
python3 myDandelion.py $id_exp
echo dand cand
python3 createFeatureVector.py $id_exp candidates
echo fv cand
python3 createInstanceVector.py $id_exp candidates
echo f cand
python3 evaluateInstances.py $id_exp
echo evaluate inst
python3 evaluateTypes.py $id_exp
echo evaluate type
python3 evaluateCandidate.py $id_exp
echo evaluate
python3 rankCandidates.py $id_exp
echo fineee bye yeeee

