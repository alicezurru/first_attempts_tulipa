minimize
obj: 13500 assets_investment_1900,Coal_ + 5833.333332499999 assets_investment_1900,Gas_ + 15500 assets_investment_1900,Lignite_ + 2416.666667 assets_investment_1900,Oil_ + 4840 assets_investment_1900,WindOn_ + 499.31999999999994 flow_(Coal,Demand_GER),1900,1,1_1_ + 166.44 flow_(Gas,Demand_GER),1900,1,1_1_ + 332.88 flow_(Lignite,Demand_GER),1900,1,1_1_ + 665.76 flow_(Oil,Demand_GER),1900,1,1_1_ + 8.322 flow_(WindOn,Demand_GER),1900,1,1_1_ + 33288 flow_(Loss_of_load,Demand_GER),1900,1,1_1_ + 512.4599999999999 flow_(Coal,Demand_GER),1900,2,1_1_ + 170.82 flow_(Gas,Demand_GER),1900,2,1_1_ + 341.64 flow_(Lignite,Demand_GER),1900,2,1_1_ + 683.28 flow_(Oil,Demand_GER),1900,2,1_1_ + 8.541 flow_(WindOn,Demand_GER),1900,2,1_1_ + 34164 flow_(Loss_of_load,Demand_GER),1900,2,1_1_ + 302.21999999999997 flow_(Coal,Demand_GER),1900,3,1_1_ + 100.74 flow_(Gas,Demand_GER),1900,3,1_1_ + 201.48 flow_(Lignite,Demand_GER),1900,3,1_1_ + 402.96 flow_(Oil,Demand_GER),1900,3,1_1_ + 5.037 flow_(WindOn,Demand_GER),1900,3,1_1_ + 20148 flow_(Loss_of_load,Demand_GER),1900,3,1_1_
subject to
max_output_flows_limit_simple_method_Coal,1900,1,1_1_: 1 flow_(Coal,Demand_GER),1900,1,1_1_ - 400 assets_investment_1900,Coal_ <= 0
max_output_flows_limit_simple_method_Coal,1900,2,1_1_: 1 flow_(Coal,Demand_GER),1900,2,1_1_ - 400 assets_investment_1900,Coal_ <= 0
max_output_flows_limit_simple_method_Coal,1900,3,1_1_: 1 flow_(Coal,Demand_GER),1900,3,1_1_ - 400 assets_investment_1900,Coal_ <= 0
max_output_flows_limit_simple_method_Gas,1900,1,1_1_: 1 flow_(Gas,Demand_GER),1900,1,1_1_ - 250 assets_investment_1900,Gas_ <= 0
max_output_flows_limit_simple_method_Gas,1900,2,1_1_: 1 flow_(Gas,Demand_GER),1900,2,1_1_ - 250 assets_investment_1900,Gas_ <= 0
max_output_flows_limit_simple_method_Gas,1900,3,1_1_: 1 flow_(Gas,Demand_GER),1900,3,1_1_ - 250 assets_investment_1900,Gas_ <= 0
max_output_flows_limit_simple_method_Lignite,1900,1,1_1_: 1 flow_(Lignite,Demand_GER),1900,1,1_1_ - 400 assets_investment_1900,Lignite_ <= 0
max_output_flows_limit_simple_method_Lignite,1900,2,1_1_: 1 flow_(Lignite,Demand_GER),1900,2,1_1_ - 400 assets_investment_1900,Lignite_ <= 0
max_output_flows_limit_simple_method_Lignite,1900,3,1_1_: 1 flow_(Lignite,Demand_GER),1900,3,1_1_ - 400 assets_investment_1900,Lignite_ <= 0
max_output_flows_limit_simple_method_Loss_of_load,1900,1,1_1_: 1 flow_(Loss_of_load,Demand_GER),1900,1,1_1_ <= 100000
max_output_flows_limit_simple_method_Loss_of_load,1900,2,1_1_: 1 flow_(Loss_of_load,Demand_GER),1900,2,1_1_ <= 100000
max_output_flows_limit_simple_method_Loss_of_load,1900,3,1_1_: 1 flow_(Loss_of_load,Demand_GER),1900,3,1_1_ <= 100000
max_output_flows_limit_simple_method_Oil,1900,1,1_1_: 1 flow_(Oil,Demand_GER),1900,1,1_1_ - 100 assets_investment_1900,Oil_ <= 0
max_output_flows_limit_simple_method_Oil,1900,2,1_1_: 1 flow_(Oil,Demand_GER),1900,2,1_1_ - 100 assets_investment_1900,Oil_ <= 0
max_output_flows_limit_simple_method_Oil,1900,3,1_1_: 1 flow_(Oil,Demand_GER),1900,3,1_1_ - 100 assets_investment_1900,Oil_ <= 0
max_output_flows_limit_simple_method_WindOn,1900,1,1_1_: 1 flow_(WindOn,Demand_GER),1900,1,1_1_ - 24.987070396363134 assets_investment_1900,WindOn_ <= 0
max_output_flows_limit_simple_method_WindOn,1900,2,1_1_: 1 flow_(WindOn,Demand_GER),1900,2,1_1_ - 72.925 assets_investment_1900,WindOn_ <= 0
max_output_flows_limit_simple_method_WindOn,1900,3,1_1_: 1 flow_(WindOn,Demand_GER),1900,3,1_1_ - 48.56961351706048 assets_investment_1900,WindOn_ <= 0
consumer_balance_Demand_GER,1900,1,1_1_: 1 flow_(Coal,Demand_GER),1900,1,1_1_ + 1 flow_(Gas,Demand_GER),1900,1,1_1_ + 1 flow_(Lignite,Demand_GER),1900,1,1_1_ + 1 flow_(Oil,Demand_GER),1900,1,1_1_ + 1 flow_(WindOn,Demand_GER),1900,1,1_1_ + 1 flow_(Loss_of_load,Demand_GER),1900,1,1_1_ = 49500.759808196424
consumer_balance_Demand_GER,1900,2,1_1_: 1 flow_(Coal,Demand_GER),1900,2,1_1_ + 1 flow_(Gas,Demand_GER),1900,2,1_1_ + 1 flow_(Lignite,Demand_GER),1900,2,1_1_ + 1 flow_(Oil,Demand_GER),1900,2,1_1_ + 1 flow_(WindOn,Demand_GER),1900,2,1_1_ + 1 flow_(Loss_of_load,Demand_GER),1900,2,1_1_ = 50030.20316431932
consumer_balance_Demand_GER,1900,3,1_1_: 1 flow_(Coal,Demand_GER),1900,3,1_1_ + 1 flow_(Gas,Demand_GER),1900,3,1_1_ + 1 flow_(Lignite,Demand_GER),1900,3,1_1_ + 1 flow_(Oil,Demand_GER),1900,3,1_1_ + 1 flow_(WindOn,Demand_GER),1900,3,1_1_ + 1 flow_(Loss_of_load,Demand_GER),1900,3,1_1_ = 66715.08161341047
Bounds
flow_(Coal,Demand_GER),1900,1,1_1_ >= 0
flow_(Gas,Demand_GER),1900,1,1_1_ >= 0
flow_(Lignite,Demand_GER),1900,1,1_1_ >= 0
flow_(Oil,Demand_GER),1900,1,1_1_ >= 0
flow_(WindOn,Demand_GER),1900,1,1_1_ >= 0
flow_(Loss_of_load,Demand_GER),1900,1,1_1_ >= 0
flow_(Coal,Demand_GER),1900,2,1_1_ >= 0
flow_(Gas,Demand_GER),1900,2,1_1_ >= 0
flow_(Lignite,Demand_GER),1900,2,1_1_ >= 0
flow_(Oil,Demand_GER),1900,2,1_1_ >= 0
flow_(WindOn,Demand_GER),1900,2,1_1_ >= 0
flow_(Loss_of_load,Demand_GER),1900,2,1_1_ >= 0
flow_(Coal,Demand_GER),1900,3,1_1_ >= 0
flow_(Gas,Demand_GER),1900,3,1_1_ >= 0
flow_(Lignite,Demand_GER),1900,3,1_1_ >= 0
flow_(Oil,Demand_GER),1900,3,1_1_ >= 0
flow_(WindOn,Demand_GER),1900,3,1_1_ >= 0
flow_(Loss_of_load,Demand_GER),1900,3,1_1_ >= 0
assets_investment_1900,Coal_ >= 0
assets_investment_1900,Gas_ >= 0
assets_investment_1900,Lignite_ >= 0
assets_investment_1900,Oil_ >= 0
assets_investment_1900,WindOn_ >= 0
End
