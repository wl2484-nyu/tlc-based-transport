#!/bin/bash
# Owner: Wan-Yu Lin

#sbt clean
#sbt compile
#sbt package

hadoop fs -rm -r /user/wl2484_nyu_edu/project/data/profile/tlc/taxi_trips
hadoop fs -rm -r /user/wl2484_nyu_edu/project/data/intermediate/taxi_trip_path_frequency

spark-submit --deploy-mode cluster \
  --class Main tlc-based-transport_2.12-1.0.2.jar \
  --ns-dis-input /user/wl2484_nyu_edu/project/data/intermediate/location_neighbors_distance \
  --tlc-input /user/wl2484_nyu_edu/project/data/clean/tlc/taxi_trips \
  --profile-output /user/wl2484_nyu_edu/project/data/profile/tlc/taxi_trips \
  --path-freq-output /user/wl2484_nyu_edu/project/data/intermediate/taxi_trip_path_frequency
