minimize
obj: 13500 assets_investment_1900,Coal_ + 5833.333332499999 assets_investment_1900,Gas_ + 15500 assets_investment_1900,Lignite_ + 2416.666667 assets_investment_1900,Oil_ + 4840 assets_investment_1900,WindOn_ + 485.8231418470126 flow_(Coal,Demand_GER),1900,1,1_1_ + 161.94104728233754 flow_(Gas,Demand_GER),1900,1,1_1_ + 323.88209456467507 flow_(Lignite,Demand_GER),1900,1,1_1_ + 647.7641891293501 flow_(Oil,Demand_GER),1900,1,1_1_ + 8.097052364116877 flow_(WindOn,Demand_GER),1900,1,1_1_ + 32388.20945646751 flow_(Loss_of_load,Demand_GER),1900,1,1_1_ + 490.9541832998838 flow_(Coal,Demand_GER),1900,2,1_1_ + 163.65139443329463 flow_(Gas,Demand_GER),1900,2,1_1_ + 327.30278886658925 flow_(Lignite,Demand_GER),1900,2,1_1_ + 654.6055777331785 flow_(Oil,Demand_GER),1900,2,1_1_ + 8.18256972166473 flow_(WindOn,Demand_GER),1900,2,1_1_ + 32730.278886658925 flow_(Loss_of_load,Demand_GER),1900,2,1_1_ + 337.22267485310357 flow_(Coal,Demand_GER),1900,3,1_1_ + 112.40755828436787 flow_(Gas,Demand_GER),1900,3,1_1_ + 224.81511656873573 flow_(Lignite,Demand_GER),1900,3,1_1_ + 449.63023313747146 flow_(Oil,Demand_GER),1900,3,1_1_ + 5.620377914218394 flow_(WindOn,Demand_GER),1900,3,1_1_ + 22481.511656873576 flow_(Loss_of_load,Demand_GER),1900,3,1_1_
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
max_output_flows_limit_simple_method_WindOn,1900,1,1_1_: 1 flow_(WindOn,Demand_GER),1900,1,1_1_ - 45.538001752397996 assets_investment_1900,WindOn_ <= 0
max_output_flows_limit_simple_method_WindOn,1900,2,1_1_: 1 flow_(WindOn,Demand_GER),1900,2,1_1_ - 51.60734584262674 assets_investment_1900,WindOn_ <= 0
max_output_flows_limit_simple_method_WindOn,1900,3,1_1_: 1 flow_(WindOn,Demand_GER),1900,3,1_1_ - 72.73211663761832 assets_investment_1900,WindOn_ <= 0
consumer_balance_Demand_GER,1900,1,1_1_: 1 flow_(Coal,Demand_GER),1900,1,1_1_ + 1 flow_(Gas,Demand_GER),1900,1,1_1_ + 1 flow_(Lignite,Demand_GER),1900,1,1_1_ + 1 flow_(Oil,Demand_GER),1900,1,1_1_ + 1 flow_(WindOn,Demand_GER),1900,1,1_1_ + 1 flow_(Loss_of_load,Demand_GER),1900,1,1_1_ = 83193.3905524274
consumer_balance_Demand_GER,1900,2,1_1_: 1 flow_(Coal,Demand_GER),1900,2,1_1_ + 1 flow_(Gas,Demand_GER),1900,2,1_1_ + 1 flow_(Lignite,Demand_GER),1900,2,1_1_ + 1 flow_(Oil,Demand_GER),1900,2,1_1_ + 1 flow_(WindOn,Demand_GER),1900,2,1_1_ + 1 flow_(Loss_of_load,Demand_GER),1900,2,1_1_ = 41812.188384245244
consumer_balance_Demand_GER,1900,3,1_1_: 1 flow_(Coal,Demand_GER),1900,3,1_1_ + 1 flow_(Gas,Demand_GER),1900,3,1_1_ + 1 flow_(Lignite,Demand_GER),1900,3,1_1_ + 1 flow_(Oil,Demand_GER),1900,3,1_1_ + 1 flow_(WindOn,Demand_GER),1900,3,1_1_ + 1 flow_(Loss_of_load,Demand_GER),1900,3,1_1_ = 65185.53892001514
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
