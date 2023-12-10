spark-submit --deploy-mode cluster \
  --class etl.TaxiYellow tlc-based-transport_2.12-0.6.0.jar \
  --source /user/cg4177_nyu_edu/project/data/source/tlc/yellow \
  --clean-output /user/cg4177_nyu_edu/project/data/clean/tlc/yellow \