import TulipaIO as TIO
import TulipaEnergyModel as TEM
using DuckDB
using DataFrames
using Plots
using Distances
import TulipaClustering as TC 

using Random
Random.seed!(123)

using DuckDB: DBInterface, DuckDB

# We are staring from a fresh `obz.db` file
connection = DBInterface.connect(DuckDB.DB, "obz.db")

nice_query(str) = DataFrame(DuckDB.query(connection, str))

using TulipaIO
TulipaIO.read_csv_folder(
    connection,
    "tutorial7/obz",
    replace_if_exists = true,
)

# The first 5 tables
nice_query("SELECT table_name FROM duckdb_tables() LIMIT 5")

# now process data for tulipa
# we need a profiles table with 4 cols: profile_name, year, timestep, value

nice_query("SELECT year, timestep, * LIKE 'NL_%' FROM profiles LIMIT 5")
nice_query("SELECT year, MAX(timestep) FROM profiles GROUP BY year")

TulipaClustering.transform_wide_to_long!(connection, "profiles", "pivot_profiles") # the right input for tulipa, putting together

DuckDB.query(
    connection,
    "CREATE OR REPLACE TABLE profiles AS
    FROM pivot_profiles
    ORDER BY profile_name, year, timestep
    "
)

nice_query("SELECT COUNT(*) FROM profiles")
nice_query("SELECT profile_name FROM profiles")
nice_query("SELECT * FROM profiles")

using Plots
# plot the profiles 
subtable = DuckDB.query(
    connection,
    "SELECT
        timestep,
        value,
        profile_name,
    FROM profiles
    WHERE
        profile_name LIKE 'NL_%'
        AND year=2050
        AND timestep <= 72 -- Just 72 hours
    ORDER BY timestep
    ",
)
df = DataFrame(subtable)
plot(df.timestep, df.value, group=df.profile_name)

using Distances: SqEuclidean

## Data for clustering
clustering_params = (
    num_rep_periods = 3,    # number of representative periods
    period_duration = 24,   # hours of the representative period
    method = :k_means,
    distance = SqEuclidean(),
    ## Data for weight fitting
    weight_type = :convex,
    tol = 1e-2,
)
TulipaClustering.cluster!(
    connection,
    clustering_params.period_duration,  # Required
    clustering_params.num_rep_periods;  # Required
    clustering_params.method,           # Optional
    clustering_params.distance,         # Optional
    clustering_params.weight_type,      # Optional
    clustering_params.tol,              # Optional
)

TIO.get_table(connection,"rep_periods_data")
TIO.get_table(connection,"rep_periods_mapping")
TIO.get_table(connection,"profiles_rep_periods")
TIO.get_table(connection,"timeframe_data")

# table asset 
DuckDB.query(
    connection,
    "CREATE TABLE asset AS
    SELECT
        name AS asset,
        type,
        capacity,
        capacity_storage_energy,
        is_seasonal,
    FROM (
        FROM assets_consumer_basic_data
        UNION BY NAME
        FROM assets_conversion_basic_data
        UNION BY NAME
        FROM assets_hub_basic_data
        UNION BY NAME
        FROM assets_producer_basic_data
        UNION BY NAME
        FROM assets_storage_basic_data
    ) -- to put them all together we use UNION
    ORDER BY asset
    ",
)

nice_query("FROM asset ORDER BY random() LIMIT 5")

# now we join the assets' yearly data used after to create the three other asset tables that Tulipa requires
DuckDB.query(
    connection,
    "CREATE TABLE t_asset_yearly AS
    FROM (
        FROM assets_consumer_yearly_data
        UNION BY NAME
        FROM assets_conversion_yearly_data
        UNION BY NAME
        FROM assets_hub_yearly_data
        UNION BY NAME
        FROM assets_producer_yearly_data
        UNION BY NAME
        FROM assets_storage_yearly_data
    )
    ",
)

# asset_commission
DuckDB.query(
    connection,
    "CREATE TABLE asset_commission AS
    SELECT
        name AS asset,
        year AS commission_year,
    FROM t_asset_yearly
    ORDER by asset
    "
)
# asset_milestone
DuckDB.query(
    connection,
    "CREATE TABLE asset_milestone AS
    SELECT
        name AS asset,
        year AS milestone_year,
        peak_demand,
        initial_storage_level,
        storage_inflows,
    FROM t_asset_yearly
    ORDER by asset
    "
)
# asset_both
DuckDB.query(
    connection,
    "CREATE TABLE asset_both AS
    SELECT
        name AS asset,
        year AS milestone_year,
        year AS commission_year, -- Yes, it is the same year twice with different names because it's not a multi-year problem
        initial_units,
        initial_storage_units,
    FROM t_asset_yearly
    ORDER by asset
    "
)

# example: 
nice_query("FROM asset_both WHERE initial_storage_units > 0 LIMIT 5")


# same for flows
DuckDB.query(
    connection,
    "CREATE TABLE flow AS
    SELECT
        from_asset,
        to_asset,
        carrier,
        capacity,
        is_transport,
    FROM (
        FROM flows_assets_connections_basic_data
        UNION BY NAME
        FROM flows_transport_assets_basic_data
    )
    ORDER BY from_asset, to_asset
    ",
)

DuckDB.query(
    connection,
    "CREATE TABLE t_flow_yearly AS
    FROM (
        FROM flows_assets_connections_yearly_data
        UNION BY NAME
        FROM flows_transport_assets_yearly_data
    )
    ",
)

DuckDB.query(
    connection,
    "CREATE TABLE flow_commission AS
    SELECT
        from_asset,
        to_asset,
        year AS commission_year,
        efficiency AS producer_efficiency,
    FROM t_flow_yearly
    ORDER by from_asset, to_asset
    "
)

DuckDB.query(
    connection,
    "CREATE TABLE flow_milestone AS
    SELECT
        from_asset,
        to_asset,
        year AS milestone_year,
        variable_cost AS operational_cost,
    FROM t_flow_yearly
    ORDER by from_asset, to_asset
    "
)

DuckDB.query(
    connection,
    "CREATE TABLE flow_both AS
    SELECT
        t_flow_yearly.from_asset,
        t_flow_yearly.to_asset,
        t_flow_yearly.year AS milestone_year,
        t_flow_yearly.year AS commission_year,
        t_flow_yearly.initial_export_units,
        t_flow_yearly.initial_import_units,
    FROM t_flow_yearly
    LEFT JOIN flow -- left join keeps all rows from t_flow_yearly even if there is no matching in flow
      ON flow.from_asset = t_flow_yearly.from_asset
      AND flow.to_asset = t_flow_yearly.to_asset
    WHERE flow.is_transport = TRUE -- flow_both must only contain transport flows (that can be positive or negative)
    ORDER by t_flow_yearly.from_asset, t_flow_yearly.to_asset
    "
)

# assets_profiles already exists, so we only need assets_timeframe_profiles
DuckDB.query(
    connection,
      "CREATE TABLE assets_timeframe_profiles AS
      SELECT
        asset,
        commission_year AS year,
        profile_type,
        profile_name
      FROM assets_storage_min_max_reservoir_level_profiles
      ORDER BY asset, year, profile_name
      ",
)

# partitions
DuckDB.query(
    connection,
    "CREATE TABLE assets_rep_periods_partitions AS
    SELECT
        t.name AS asset,
        t.year,
        t.partition AS partition,
        rep_periods_data.rep_period,
        'uniform' AS specification,
    FROM t_asset_yearly AS t
    LEFT JOIN rep_periods_data
        ON t.year = rep_periods_data.year
    ORDER BY asset, t.year, rep_period
    ",
)
# Given a flow (from_asset, to_asset), we look at the partition of both from_asset and to_asset. 
# If the flow is a transport flow, we use the maximum between the partitions of from_asset and to_asset. Otherwise, we use the minimum between these two.
DuckDB.query(
    connection,
    "CREATE TABLE flows_rep_periods_partitions AS
    SELECT
        flow.from_asset,
        flow.to_asset,
        t_from.year,
        t_from.rep_period,
        'uniform' AS specification,
        IF(
            flow.is_transport,
            greatest(t_from.partition::int, t_to.partition::int),
            least(t_from.partition::int, t_to.partition::int)
        ) AS partition,
    FROM flow
    LEFT JOIN assets_rep_periods_partitions AS t_from
        ON flow.from_asset = t_from.asset
    LEFT JOIN assets_rep_periods_partitions AS t_to
        ON flow.to_asset = t_to.asset
        AND t_from.year = t_to.year
        AND t_from.rep_period = t_to.rep_period
    ",
)

# timeframe profiles 


TulipaClustering.transform_wide_to_long!(
    connection,
    "min_max_reservoir_levels",
    "pivot_min_max_reservoir_levels",
)
# we do not cluster these profiles, since the representative periods are already computed. 
# Instead, we will create a temporary table (cte_split_profiles) that converts the timestep that goes from 1 to 8760 into two columns: period, from to 1 to 365 (days) and timestep, from 1 to 24 (hours).
period_duration = clustering_params.period_duration
# the timeframe profiles are computed with the average over period, i.e., each value of a given timeframe profile in a period is the average of 24 hours of the original profile.
DuckDB.query(
    connection,
    "
    CREATE TABLE profiles_timeframe AS
    WITH cte_split_profiles AS (
        SELECT
            profile_name,
            year,
            1 + (timestep - 1) // $period_duration  AS period,
            1 + (timestep - 1)  % $period_duration AS timestep,
            value,
        FROM pivot_min_max_reservoir_levels
    )
    SELECT
        cte_split_profiles.profile_name,
        cte_split_profiles.year,
        cte_split_profiles.period,
        AVG(cte_split_profiles.value) AS value, -- Computing the average aggregation
    FROM cte_split_profiles
    GROUP BY
        cte_split_profiles.profile_name,
        cte_split_profiles.year,
        cte_split_profiles.period
    ORDER BY
        cte_split_profiles.profile_name,
        cte_split_profiles.year,
        cte_split_profiles.period
    ",
)

using TulipaEnergyModel: TulipaEnergyModel as TEM

TEM.populate_with_defaults!(connection)
energy_problem = TEM.EnergyProblem(connection)
optimizer_parameters = Dict(
    "output_flag" => true,
    "mip_rel_gap" => 0.0,
    "mip_feasibility_tolerance" => 1e-5,
)
TEM.create_model!(energy_problem; optimizer_parameters)
TEM.solve_model!(energy_problem)
TEM.save_solution!(energy_problem; compute_duals = true) # to save the solutions and compute_duals
# example:
nice_query("SELECT *
    FROM var_storage_level_rep_period
    WHERE solution > 0
    LIMIT 5
")
nice_query("SELECT *
    FROM cons_balance_storage_rep_period
    WHERE dual_max_storage_level_rep_period_limit = 0
        AND dual_min_storage_level_rep_period_limit = 0
    LIMIT 5
")

# example of processing solutions:
nice_query("
CREATE TEMP TABLE analysis_inter_storage_levels AS
SELECT
    var.id,
    var.asset,
    var.period_block_start as period,
    asset.capacity_storage_energy,
    var.solution / (
        IF(asset.capacity_storage_energy > 0, asset.capacity_storage_energy, 1)
    ) AS SoC,
FROM var_storage_level_over_clustered_year AS var
LEFT JOIN asset
    ON var.asset = asset.asset
")
nice_query("FROM analysis_inter_storage_levels LIMIT 5")

# create plots 
using Plots

p = plot()
assets = ["ES_Hydro_Reservoir", "NO_Hydro_Reservoir", "FR_Hydro_Reservoir"]

df = nice_query("SELECT asset, period, SoC
    FROM analysis_inter_storage_levels
    WHERE asset in ('ES_Hydro_Reservoir', 'NO_Hydro_Reservoir', 'FR_Hydro_Reservoir')
")

plot!(
    df.period,          # x-axis
    df.SoC,             # y-axis
    group = df.asset,   # each asset is a different plot
    xlabel = "Period",
    ylabel = "Storage level [p.u.]",
    linewidth = 3,
    dpi = 600,
)

# now finally export solutions
mkdir("obz-outputs")
TEM.export_solution_to_csv_files("obz-outputs", energy_problem)
readdir("obz-outputs")

close(connection)