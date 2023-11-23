# Taxi Trip Based Transport Recommendation
> Optional items are marked with *

Recommend public transport route(s) based on [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Members
1. Wan-Yu Lin (wl2484)
2. Priyanka Narain (pn2182)
3. Charvi Gupta (cg4177)

## Data Sources
|     | Main Dataset                                                                                                                                          | Subordinate Dataset                                                                                                              | Cleaner & Profiler | Format    | Size         | Time Span         | HDFS Location                                               | Notes                                                                                                    |
|-----|:------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------|:-------------------|:----------|:-------------|:------------------|:------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------|
| 1   | [NYC Taxi Zones](https://catalog.data.gov/dataset/nyc-taxi-zones)                                                                                     | -                                                                                                                                | Wan-Yu Lin         | CSV-ish   | ~4MB         | -                 | /user/wl2484_nyu_edu/project/data/source/tlc/zones          | Each zone has a unique LocationID and corresponds to the same *LocationID value used in dataset-{2,3,4}. |
| 2   | [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)                                                                  | [2.1 Yellow Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)         | Charvi Gupta       | PARQUET   | ~50MB/Month  | 2020.10 - 2023.09 | /user/wl2484_nyu_edu/project/data/source/tlc/yellow         |                                                                                                          |
|     |                                                                                                                                                       | [2.2 Green Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)           | Charvi Gupta       | PARQUET   | ~2MB/Month   | 2020.10 - 2023.09 | /user/wl2484_nyu_edu/project/data/source/tlc/green          |                                                                                                          |
|     |                                                                                                                                                       | [2.3 FHV Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_fhv.pdf)               | Priyanka Narain    | PARQUET   | ~10MB/Month  | 2020.10 - 2023.09 | /user/wl2484_nyu_edu/project/data/source/tlc/fhv            |                                                                                                          |
|     |                                                                                                                                                       | [2.4 High Volume FHV Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf) | Priyanka Narain    | PARQUET   | ~500MB/Month | 2020.10 - 2023.09 | /user/wl2484_nyu_edu/project/data/source/tlc/fhvhv          |                                                                                                          |
| 3   | [Taxi Zone Lookup Table](https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv)                                                            | -                                                                                                                                | Wan-Yu Lin         | CSV       | ~12KB        | -                 | /user/wl2484_nyu_edu/project/data/source/tlc/zone_lookup    |                                                                                                          |
| 4*  | [MTA General Transit Feed Specification (GTFS) Static Data](https://catalog.data.gov/dataset/mta-general-transit-feed-specification-gtfs-static-data) | [4.1 NYCT Subway](http://web.mta.info/developers/data/nyct/subway/google_transit.zip)                                            | Wan-Yu Lin         | CSV       | ~41MB        | -                 | /user/wl2484_nyu_edu/project/data/source/subway/nyct        |                                                                                                          |
|     |                                                                                                                                                       | [4.2 MTA Bus Company](http://web.mta.info/developers/data/busco/google_transit.zip)                                              | Wan-Yu Lin         | CSV       | ~94MB        | -                 | /user/wl2484_nyu_edu/project/data/source/bus/mta_private    |                                                                                                          |
|     |                                                                                                                                                       | [4.3 NYCT Bus (Bronx)](http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip)                                    | Wan-Yu Lin         | CSV       | ~79MB        | -                 | /user/wl2484_nyu_edu/project/data/source/bus/nyct/bronx     |                                                                                                          |
|     |                                                                                                                                                       | [4.4 NYCT Bus (Brooklyn)](http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip)                              | Wan-Yu Lin         | CSV       | ~165MB       | -                 | /user/wl2484_nyu_edu/project/data/source/bus/nyct/brooklyn  |                                                                                                          |
|     |                                                                                                                                                       | [4.5 NYCT Bus (Manhattan)](http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip)                            | Wan-Yu Lin         | CSV       | ~79MB        | -                 | /user/wl2484_nyu_edu/project/data/source/bus/nyct/manhattan |                                                                                                          |
|     |                                                                                                                                                       | [4.6 NYCT Bus (Queens)](http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip)                                  | Wan-Yu Lin         | CSV       | ~86MB        | -                 | /user/wl2484_nyu_edu/project/data/source/bus/nyct/queens    |                                                                                                          |

## Data Ingestion
> Owner: Wan-Yu Lin

Upload datasets to HDFS with `wget` and `hadoop fs` commands.

```shell
NET_ID="your-net-id"
hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project
hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data
hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source
hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/tlc

# upload dataset-1
hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/tlc/zones

wget -O taxi_zones.csv https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv
hadoop fs -put taxi_zones.csv /user/${NET_ID}_nyu_edu/project/data/source/tlc/zones/taxi_zones.csv
rm taxi_zones.csv

# upload dataset-2
for t in yellow green fhv fhvhv; do 
  hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/tlc/$t; 
  for y in 2023 2022 2021; do 
    hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/tlc/$t/$y; 
    for m in {9..1}; do 
      hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/tlc/$t/$y/0$m; 
      wget https://d37ci6vzurychx.cloudfront.net/trip-data/${t}_tripdata_${y}-0${m}.parquet; 
      hadoop fs -put ${t}_tripdata_${y}-0${m}.parquet /user/${NET_ID}_nyu_edu/project/data/source/tlc/${t}/${y}/0${m}/${t}_tripdata_${y}-0${m}.parquet; 
      rm ${t}_tripdata_${y}-0${m}.parquet; 
    done; 
  done; 
  for y in 2022 2021 2020; do 
    hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/tlc/$t/$y; 
    for m in {12..10}; do 
      hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/tlc/$t/$y/$m; 
      wget https://d37ci6vzurychx.cloudfront.net/trip-data/${t}_tripdata_${y}-${m}.parquet; 
      hadoop fs -put ${t}_tripdata_${y}-${m}.parquet /user/${NET_ID}_nyu_edu/project/data/source/tlc/${t}/${y}/${m}/${t}_tripdata_${y}-${m}.parquet; 
      rm ${t}_tripdata_${y}-${m}.parquet; 
    done; 
  done; 
done;

# upload dataset-3
hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/tlc/zone_lookup

wget -O taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv
hadoop fs -put taxi_zone_lookup.csv /user/${NET_ID}_nyu_edu/project/data/source/tlc/zone_lookup/taxi_zone_lookup.csv
rm taxi_zone_lookup.csv

# upload dataset-4
## dataset-4.1
hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/subway

wget -O nyct.zip http://web.mta.info/developers/data/nyct/subway/google_transit.zip
unzip -d nyct nyct.zip
hadoop fs -copyFromLocal nyct /user/${NET_ID}_nyu_edu/project/data/source/subway/nyct
rm nyct.zip
rm -r nyct

## dataset-4.{2,3,4,5,6}
hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/bus
hadoop fs -mkdir /user/${NET_ID}_nyu_edu/project/data/source/bus/nyct

wget -O mta_private.zip http://web.mta.info/developers/data/busco/google_transit.zip
unzip -d mta_private mta_private.zip
hadoop fs -copyFromLocal mta_private /user/${NET_ID}_nyu_edu/project/data/source/bus/mta_private
rm mta_private.zip
rm -r mta_private

for br in bronx brooklyn manhattan queens; do 
  wget -O ${br}.zip http://web.mta.info/developers/data/nyct/bus/google_transit_${br}.zip; 
  unzip -d ${br} ${br}.zip; 
  hadoop fs -copyFromLocal ${br} /user/${NET_ID}_nyu_edu/project/data/source/bus/nyct/${br}; 
  rm ${br}.zip; 
  rm -r ${br}; 
done;
```


## Methodology
When there exist frequent taxi trips going from location-X to location-Y, both location-X and location-Y could be 
considered good candidate starting and ending stops, or 2 intermediate stops, of a public transport route.

With the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), we can compute the 
frequency of each taxi trip (pick-up, drop-off) pair, and identify the top K pairs that appear most frequently. 

Then, we need to figure out an applicable route from the pick-up location to the drop-off location for each of the top K 
pairs. As the explicit travel way of each taxi trip is not given in the dataset, we propose a simple approach to 
approximate a coarse-grained route using the neighboring relation between taxi zones (i.e. locations).

There exist multiple possible paths going from location-X to location-Y. For each possible path of length N-1, e.g. 
location-L_{1 -> ... -> i -> ... -> N}, where N >= 2 and 1 <= i <= N, L_1 corresponds to X, L_N corresponds to Y, and 
for all i each edge connecting L_i and L_(i+1) implies a neighboring relation and its weight represents the distance 
between L_i and L_(i+1). By adopting this setting, we can then apply the Dijkstra's Algorithm to find the shortest
(distance) path, i.e. an applicable route, going from location-X to location-Y.

The followings are the assumptions we made:
1. Every taxi zone is a small enough area where no public transport route recommendation is needed, i.e. taxi trips 
   where the starting and ending locations are the same are excluded from the datasets.
2. Every taxi zone has >= 1 neighbor, i.e. isolated locations like islands are excluded from the datasets.
3. For all locations, there exist at least one viable way going directly from itself to each of its neighbor.
4. Each taxi trip going from location-X to location-Y took the shortest distance path between the two locations.
5. Given the latitude and longitude coordinates of the taxi zone boundary provided in dataset-1, the coordinate of each 
   location is represented by the average of the geological coordinates of its borderline, and the distance between two 
   locations is the great-circle distance (unit: km) between the two average geological coordinates.

After that, the implementation could be broken down into the following 7 steps:

### Step-1: Build up the neighbor zone graph
> Owner: Wan-Yu Lin

Transform **dataset-1** into a neighbor zone graph represented by an adjacency list with distance as weight.

#### Expected Output 1: Spark scripts to convert dataset-1 into graph stored in a broadcast variable
For each borough in dataset-1, load and transform locations in the borough into a graph, i.e. an adjacency list with 
distance as weight, using the neighboring relation between locations, and store it as Map in a broadcast variable, e.g. 
`Broadcast[Map[Location, List[WeightedEdge[Location]]]]`.
1. When there are >= 2 identical geological coordinates on the borderline between two locations, they are considered 
neighbors.
   * **If this assumption is rejected due to the dataset's setting**, e.g. 2 neighbor locations sharing the same 
     borderline have < 2 common geological coordinate points, **the neighboring relation between locations would be 
     manually determined** according to the Taxi Zone Map - 
     {[Bronx](https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_bronx.jpg), 
     [Brooklyn](https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_brooklyn.jpg),
     [Manhattan](https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_manhattan.jpg),
     [Queens](https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_queens.jpg)} 
     provided in the dataset.
2. The coordinate of each location is represented by the average of its boundary geological coordinates.
3. The great-circle distance between two locations is calculated using the haversine formula [2].

#### Expected Output 2: Isolated locations per borough
Output a TSV file with two columns to `/user/wl2484_nyu_edu/project/data/intermediate/isolated_locations`, where each 
line in the file provides comma-separated isolated locations within a borough. Below is an example:

```tsv
Borough\tLocationIDs
Bronx\t46,199
Brooklyn\t
Manhattan\t103,104,105,153,194,202
Queens\t2,27,30,86,117,201
```


## References
1. [Graph algorithms in Scala](https://github.com/Arminea/scala-graphs)
2. [Scala: Calculating the Distance Between Two Locations](https://dzone.com/articles/scala-calculating-distance-between-two-locations)
