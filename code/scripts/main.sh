#!/bin/bash
# Owner: Wan-Yu Lin

#sbt clean
#sbt compile
#sbt package

hadoop fs -rm -r /user/wl2484_nyu_edu/project/data/profile/tlc/taxi_trips
hadoop fs -rm -r /user/wl2484_nyu_edu/project/data/intermediate/taxi_trip_path_frequency

spark-submit --deploy-mode cluster \
  --class Main tlc-based-transport_2.12-1.1.1.jar \
  --ns-dis-input /user/wl2484_nyu_edu/project/data/intermediate/location_neighbors_distance \
  --tlc-input /user/wl2484_nyu_edu/project/data/clean/tlc/taxi_trips \
  --path-freq-output /user/wl2484_nyu_edu/project/data/intermediate/taxi_trip_path_frequency \
  --path-coverage-output /user/wl2484_nyu_edu/project/data/intermediate/trip_paths_coverage_count \
  --path-HRcoverage-output /user/wl2484_nyu_edu/project/data/result/rec_routes_coverage_count \
  --path-HRcoveragepercent-output /user/wl2484_nyu_edu/project/data/result/rec_routes_coverage_percent
