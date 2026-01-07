using TulipaClustering
using DataFrames
using Debugger
using Distances

# Example clustering data
clustering_data = DataFrame(
    rep_period = [1,1,2,2],
    timestep   = [1,2,1,2],
    profile_name = ["demand","wind","demand","wind"],
    value = [10.0, 0.0, 12.0, 1.0]
)

nrp = 2
layout = ProfilesTableLayout()
initial_representatives = DataFrame()

# Step into the function
temp = TulipaClustering.find_representative_periods(
    clustering_data,
    nrp;
    drop_incomplete_last_period = false,
    method = :kmeans,
    distance = SqEuclidean(),
    initial_representatives = initial_representatives,
    layout = layout,
)
