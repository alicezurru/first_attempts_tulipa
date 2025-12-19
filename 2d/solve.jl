import TulipaIO as TIO
import TulipaEnergyModel as TEM
using DuckDB
using DataFrames
using Plots
using Distances
import TulipaClustering as TC 

using Random
Random.seed!(123)

using DataFrames, DuckDB

connection = DBInterface.connect(DuckDB.DB)
input_dir = "2d"
output_dir = "2d/results"

TIO.read_csv_folder(connection, input_dir)
nice_query(str) = DuckDB.query(connection, str) |> DataFrame



nice_query("CREATE TABLE profiles AS 
SELECT 'demand' AS profile_name, period, timestep, scenario AS year, location, (demand/66113.79524366604) AS value FROM demand
UNION ALL 
SELECT technology AS profile_name, period, timestep, scenario AS year, location, availability AS value FROM generation_availability")

nice_query("CREATE TABLE max_demand AS
    SELECT location, MAX(demand) AS max_demand
    FROM demand_data
    GROUP BY location
")
TIO.get_table(connection,"max_demand")
nice_query("
scaled_demand AS (
    SELECT
        d.period,
        d.timestep,
        d.scenario,
        d.location,
        d.demand / m.max_demand AS demand  -- scaled between 0 and 1
    FROM demand_data d
    JOIN max_demand m ON d.location = m.location
),
scaled_availability AS (
    SELECT
        g.period,
        g.timestep,
        g.scenario,
        g.location,
        g.availability / s.demand AS availability  -- divide by scaled demand
    FROM generation_availability_data g
    JOIN scaled_demand s
      ON g.location = s.location
     AND g.period = s.period
     AND g.timestep = s.timestep
     AND g.scenario = s.scenario
)
SELECT * 
FROM scaled_demand;
-- or
SELECT * 
FROM scaled_availability; ")

TIO.show_tables(connection)
TIO.get_table(connection,"profiles")

period_duration = 1
num_rps = 3

# let's get the initial representative periods
initial_rp = [1, 67, 133]
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
    (demand/66113.79524366604) AS value
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
#given that the data comes from Lotte, we multiply by the annualization factor
AF = 365*24/100
nice_query("UPDATE flow_milestone
SET operational_cost = operational_cost * $AF;
")

TIO.get_table(connection,"initial_rp")

# use tulipa clustering
using TulipaClustering
clusters = cluster!(connection, 
                    period_duration,
                    num_rps; 
                    method =:convex_hull, 
                    weight_type = :convex, 
                    initial_representatives=TIO.get_table(connection,"initial_rp"),
                    distance=Distances.Euclidean()
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
from_asset = "Gas"
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

