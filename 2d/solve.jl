import TulipaIO as TIO
import TulipaEnergyModel as TEM
using DuckDB
using DataFrames
using Plots
using Distances
import TulipaClustering as TC 

using Random
Random.seed!(123)

connection = DBInterface.connect(DuckDB.DB)
input_dir = "2d"
output_dir = "2d/results"

TIO.read_csv_folder(connection, input_dir)
nice_query(str) = DuckDB.query(connection, str) |> DataFrame


nice_query("CREATE OR REPLACE TABLE profiles AS 
SELECT 'demand' AS profile_name, period, timestep, scenario AS year, location, demand AS value FROM demand
UNION ALL 
SELECT technology AS profile_name, period, timestep, scenario AS year, location, availability AS value FROM generation_availability")

nice_query("CREATE OR REPLACE TABLE max_demand AS
    SELECT location, MAX(demand) AS max_demand
    FROM demand
    GROUP BY location
") # we get max demand from real data demand (not artificial rp_demand)
TIO.get_table(connection,"max_demand")

nice_query("UPDATE profiles p
SET value = p.value / m.max_demand
FROM max_demand m
WHERE p.profile_name = 'demand'
  AND p.location = m.location")
TIO.get_table(connection,"profiles")



TIO.show_tables(connection)
TIO.get_table(connection,"profiles")

period_duration = 1
num_rps = 3

# let's get the initial representative periods
initial_rp = [1,67,133]
case_stmt = join(
    ["WHEN $p THEN $i" for (i, p) in enumerate(initial_rp)],
    "\n"
)
period_list = join(initial_rp, ", ") # string
#initial_demand = filter(row -> row.period in initial_rp, TIO.get_table(connection,"rp_demand")) # the three that we decided to start with 
#initial_availability = filter(row -> row.period in initial_rp, TIO.get_table(connection,"rp_generation_availability"))
#period_mapping = Dict(p => i for (i, p) in enumerate(initial_rp)) # dictionary that maps the initial rp
#initial_demand.period = getindex.(Ref(period_mapping), initial_demand.period) # to have the correct name for the initial rp periods
#initial_availability.period = getindex.(Ref(period_mapping), initial_availability.period)

nice_query("
CREATE OR REPLACE TABLE initial_demand AS
SELECT
    CASE period
        $case_stmt
    END AS period,
    timestep,
    'demand' AS profile_name,
    scenario AS year,
    location,
    demand AS value
FROM rp_demand
WHERE period IN ($period_list)")

nice_query("
    CREATE OR REPLACE TABLE initial_availability AS
    SELECT
    CASE period
        $case_stmt
    END AS period,
    timestep,
    technology AS profile_name,
    scenario AS year,
    location,
    availability AS value
FROM rp_generation_availability
WHERE period IN ($period_list);
")

nice_query("
CREATE OR REPLACE TABLE initial_rp AS
SELECT * FROM initial_demand
UNION ALL
SELECT * FROM initial_availability;
")

nice_query("UPDATE initial_rp p
SET value = p.value / m.max_demand
FROM max_demand m
WHERE p.profile_name = 'demand'
  AND p.location = m.location")
TIO.get_table(connection,"initial_rp")

#given that the data comes from Lotte, we multiply by the annualization factor
AF = 365*24/100
nice_query("UPDATE flow_milestone
SET operational_cost = operational_cost * $AF;
")

TIO.get_table(connection,"flow_milestone")

# use tulipa clustering
init = TIO.get_table(connection,"initial_rp")
using TulipaClustering
clusters = cluster!(connection, 
                    period_duration,
                    num_rps; 
                    method =:convex_hull, 
                    weight_type = :convex, 
                    #initial_representatives=init,
                    distance=Distances.Euclidean(),
                    weight_fitting_kwargs = Dict(
                      :learning_rate => 0.5) # added by me after problem with weights
                    )




nice_query("SHOW tables")
TIO.get_table(connection,"rep_periods_data")
TIO.get_table(connection,"profiles_rep_periods")
TIO.get_table(connection,"rep_periods_mapping")
TIO.get_table(connection,"timeframe_data")
TEM.populate_with_defaults!(connection)
energy_problem =
    TEM.run_scenario(connection;
                     output_folder=output_dir, 
                     model_file_name = "model.lp",
                     )


# Plot the results in the original periods using the representative period results
flows = TIO.get_table(connection, "var_flow")
select!(
    flows,
    :from_asset,
    :to_asset,
    :year,
    :rep_period,
    :time_block_start => :timestep,
    :solution
)
from_asset = "WindOn"
to_asset = "Demand_GER"
year = 1900
filtered_flow = filter(
    row ->
        row.from_asset == from_asset &&
            row.to_asset == to_asset &&
            row.year == year,
    flows,
)


rep_periods_mapping = TIO.get_table(connection, "rep_periods_mapping")
df = innerjoin(filtered_flow, rep_periods_mapping, on=[:year, :rep_period])
gdf = groupby(df, [:from_asset, :to_asset, :year, :period, :timestep])
result_df = combine(gdf, [:weight, :solution] => ((w, s) -> sum(w .* s)) => :solution)
TC.combine_periods!(result_df)
sort!(result_df, :timestep)
plot(
    result_df.timestep,
    result_df.solution;
    label=string(from_asset, " -> ", to_asset),
    xlabel="Period",
    ylabel="[MWh]",
    marker=:circle,
    markersize=2,
    xlims=(1, 100),
    dpi=600,
)

# now let's do it with all periods
connection2 = DBInterface.connect(DuckDB.DB)
TIO.read_csv_folder(connection2, input_dir)
DuckDB.query(connection2, "CREATE TABLE profiles AS 
SELECT 'demand' AS profile_name, period, timestep, scenario AS year, location, demand AS value FROM demand
UNION ALL 
SELECT technology AS profile_name, period, timestep, scenario AS year, location, availability AS value FROM generation_availability"
)
DuckDB.query(connection2,"CREATE OR REPLACE TABLE max_demand AS
    SELECT location, MAX(demand) AS max_demand
    FROM demand
    GROUP BY location
")
TIO.get_table(connection, "max_demand")
DuckDB.query(connection2,"UPDATE profiles p
SET value = p.value / m.max_demand
FROM max_demand m
WHERE p.profile_name = 'demand'
  AND p.location = m.location")
DuckDB.query(connection2,"UPDATE flow_milestone
SET operational_cost = operational_cost * $AF;
")
TIO.get_table(connection2, "profiles")
output_dir2 = "2d/results2"
# rename 'timestep' to a temporary name
DuckDB.query(connection2, "ALTER TABLE profiles RENAME COLUMN timestep TO tmp")

# rename 'period' to 'timestep'
DuckDB.query(connection2, "ALTER TABLE profiles RENAME COLUMN period TO timestep")

# rename temporary column to 'period'
DuckDB.query(connection2, "ALTER TABLE profiles RENAME COLUMN tmp TO period")

TIO.get_table(connection2,"profiles")
TC.dummy_cluster!(connection2)
TEM.populate_with_defaults!(connection2)
energy_problem2 =
    TEM.run_scenario(connection2; output_folder=output_dir2)


flows2 = TIO.get_table(connection2, "var_flow")

select!(
    flows2,
    :from_asset,
    :to_asset,
    :year,
    :rep_period,
    :time_block_start => :timestep,
    :solution
)

# Filter directly on from/to and year
filtered_flow = filter(
    row -> row.from_asset == from_asset &&
           row.to_asset == to_asset &&
           row.year == year,
    flows2,
)

# No need to join with rep_periods_mapping
# No need to combine weighted solutions
sort!(filtered_flow, :timestep)

# Plot directly
plot!(
    filtered_flow.timestep,
    filtered_flow.solution;
    label = string(from_asset, " -> ", to_asset),
    xlabel = "Period",
    ylabel = "[MWh]",
    marker = :circle,
    markersize = 2,
    xlims = (1, 100),
    dpi = 600,
)

# plot periods
using DataFrames, Tullio, Plots

profiles = TIO.get_table(connection, "profiles")

demand_df = filter(row -> row.profile_name == "demand", profiles)
generation_df = filter(row -> row.profile_name != "demand", profiles)

joined = innerjoin(demand_df, generation_df,
                   on=[:period, :timestep, :year, :location],
                   makeunique=true)

rename!(joined, Dict(:value => :demand, :value_1 => :availability, :profile_name_1 => :technology))

scatter(joined.demand, joined.availability,
        xlabel="Demand",
        ylabel="Availability",
        label="Base Periods"
        #legend=:topright
        )


rp = TIO.get_table(connection, "profiles_rep_periods")

rp_demand = filter(row -> row.profile_name == "demand", rp)
rp_generation = filter(row -> row.profile_name != "demand", rp)

rp_joined = innerjoin(rp_demand, rp_generation,
                      on=[:rep_period, :timestep, :year, :location],
                      makeunique=true)

rename!(rp_joined, Dict(:value => :demand, :value_1 => :availability, :profile_name_1 => :technology))
scatter!(rp_joined.demand, rp_joined.availability,
         color=:red,
         marker=:circle,
         label="Representative Periods"
         )

weights = TIO.get_table(connection,"rep_periods_mapping")
show(weights,allrows=true, allcols=true)
show(profiles,allrows=true, allcols=true)