[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fact_budget_demand_current = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_budget_demand_current", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Budget remaining _Cash out Total_", Currency.Type}, {"Net purchase order value _PO+PR new_", Currency.Type}, {"Net purchase order value _PO_", Currency.Type}, {"Net purchase order value _PR_ - New without PO", Currency.Type}, {"Net purchase order value _PR_ - All", Currency.Type}, {"Cash Out _Plan Year __Gross_", Currency.Type}, {"Cash Out _Planning Year __Net_", Currency.Type}, {"Demandbook _Cost effective BudgetYear _inc. VAT", Currency.Type}, {"Demandbook_Cash Out BudgetYear _inc. VAT", Currency.Type}, {"Demandbook_Cash Out Total _inc. VAT", Currency.Type}, {"Budget _Available", Currency.Type}, {"Actual _Alloted", Currency.Type}, {"Commitment _P.Order", Currency.Type}, {"Commitment _P.Requisition", Currency.Type}, {"Commitment_Total", Currency.Type}, {"Actual _CO Order", Currency.Type}, {"Budget _CO Order", Currency.Type}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Changed column type",{{"Fiscal year", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed Type1", {{"Budget _CO Order", "Budget Approved"}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Renamed columns 1", null, 0, Replacer.ReplaceValue, {"Budget remaining _Cash out Total_", "Net purchase order value _PO+PR new_", "Net purchase order value _PO_", "Net purchase order value _PR_ - New without PO", "Net purchase order value _PR_ - All", "Cash Out _Plan Year __Gross_", "Cash Out _Planning Year __Net_", "Demandbook _Cost effective BudgetYear _inc. VAT", "Demandbook_Cash Out BudgetYear _inc. VAT", "Demandbook_Cash Out Total _inc. VAT", "Budget _Available", "Actual _Alloted", "Commitment _P.Order", "Commitment _P.Requisition", "Commitment_Total", "Actual _CO Order", "Budget Approved"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value 1", {{"Refresh Date", type datetime}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 1", {{"Purchasing order number _PO_", "Purchasing order number (PO)"}}),
    #"Added Custom" = Table.AddColumn(#"Renamed columns", "KeyDemandPO", each [Demand ID]&"-"&[#"Purchasing order number (PO)"]),
  #"Renamed columns 2" = Table.RenameColumns(#"Added Custom", {{"Budget remaining _Cash out Total_", "Budget remaining Cash out Total"}, {"Net purchase order value _PO+PR new_", "Net purchase order value PO+PR new"}, {"Net purchase order value _PO_", "Net purchase order value PO"}, {"Net purchase order value _PR_ - New without PO", "Net purchase order value PR - New without PO"}, {"Net purchase order value _PR_ - All", "Net purchase order value PR - All"}, {"Cash Out _Plan Year __Gross_", "Cash Out Plan Year Gross"}, {"Cash Out _Planning Year __Net_", "Cash Out Planning Year Net"}, {"Demandbook _Cost effective BudgetYear _inc. VAT", "Demandbook Cost effective BudgetYear inc. VAT"}, {"Demandbook_Cash Out BudgetYear _inc. VAT", "Demandbook Cash Out BudgetYear inc. VAT"}, {"Demandbook_Cash Out Total _inc. VAT", "Demandbook Cash Out Total inc. VAT"}, {"Budget _Available", "Budget Available"}, {"Actual _Alloted", "Actual Alloted"}, {"Commitment _P.Order", "Commitment P.Order"}, {"Commitment _P.Requisition", "Commitment P.Requisition"}, {"Commitment_Total", "Commitment Total"}, {"Actual _CO Order", "Actual CO Order"}, {"Plan ID _MM PO_", "Plan ID MM PO"}, {"WBS _Acc._", "WBS Acc."}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 2", {{"KeyDemandPO", type text}}),
  #"Renamed columns 3" = Table.RenameColumns(#"Changed column type 2", {{"Purchasing order number (PO)", "Purchasing order number PO"}}),
  #"Added custom 1" = Table.AddColumn(#"Renamed columns 3", "Budget Spent", each [Budget Approved] - [Budget Available]),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Added custom 1", {{"Budget Spent", Currency.Type}, {"Budget Available", Currency.Type}, {"Budget Approved", Currency.Type}}),
  #"Added custom 2" = Table.AddColumn(#"Changed column type 3", "Profit Center and Name", each [Profit Center] & " - " & [Profit Center Name]),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "Vendor Number and Name", each [Vendor] & " - " & [Vendor Name]),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "WBS Number and Name", each [#"WBS Acc."] & " - " & [WBS Name]),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Added custom 4", {{"WBS Number and Name", type text}, {"Vendor Number and Name", type text}, {"Profit Center and Name", type text}})
in
  #"Changed column type 4";
shared fact_budget_demand_archive = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_budget_demand_archive", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"Budget remaining _Cash out Total_", Currency.Type}, {"Net purchase order value _PO+PR new_", Currency.Type}, {"Net purchase order value _PO_", Currency.Type}, {"Net purchase order value _PR_ - New without PO", Currency.Type}, {"Net purchase order value _PR_ - All", Currency.Type}, {"Cash Out _Plan Year __Gross_", Currency.Type}, {"Cash Out _Planning Year __Net_", Currency.Type}, {"Demandbook _Cost effective BudgetYear _inc. VAT", Currency.Type}, {"Demandbook_Cash Out BudgetYear _inc. VAT", Currency.Type}, {"Demandbook_Cash Out Total _inc. VAT", Currency.Type}, {"Budget _Available", Currency.Type}, {"Actual _Alloted", Currency.Type}, {"Commitment _P.Order", Currency.Type}, {"Commitment _P.Requisition", Currency.Type}, {"Commitment_Total", Currency.Type}, {"Actual _CO Order", Currency.Type}, {"Budget _CO Order", Currency.Type}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Changed column type",{{"Fiscal year", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed Type1", {{"Budget _CO Order", "Budget Approved"}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Renamed columns 1", null, 0, Replacer.ReplaceValue, {"Budget remaining _Cash out Total_", "Net purchase order value _PO+PR new_", "Net purchase order value _PO_", "Net purchase order value _PR_ - New without PO", "Net purchase order value _PR_ - All", "Cash Out _Plan Year __Gross_", "Cash Out _Planning Year __Net_", "Demandbook _Cost effective BudgetYear _inc. VAT", "Demandbook_Cash Out BudgetYear _inc. VAT", "Demandbook_Cash Out Total _inc. VAT", "Budget _Available", "Actual _Alloted", "Commitment _P.Order", "Commitment _P.Requisition", "Commitment_Total", "Actual _CO Order", "Budget Approved"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value 1", {{"Refresh Date", type datetime}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 1", {{"Purchasing order number _PO_", "Purchasing order number (PO)"}}),
    #"Added Custom" = Table.AddColumn(#"Renamed columns", "KeyDemandPO", each [Demand ID]&"-"&[#"Purchasing order number (PO)"]),
  #"Renamed columns 2" = Table.RenameColumns(#"Added Custom", {{"Budget remaining _Cash out Total_", "Budget remaining Cash out Total"}, {"Net purchase order value _PO+PR new_", "Net purchase order value PO+PR new"}, {"Net purchase order value _PO_", "Net purchase order value PO"}, {"Net purchase order value _PR_ - New without PO", "Net purchase order value PR - New without PO"}, {"Net purchase order value _PR_ - All", "Net purchase order value PR - All"}, {"Cash Out _Plan Year __Gross_", "Cash Out Plan Year Gross"}, {"Cash Out _Planning Year __Net_", "Cash Out Planning Year Net"}, {"Demandbook _Cost effective BudgetYear _inc. VAT", "Demandbook Cost effective BudgetYear inc. VAT"}, {"Demandbook_Cash Out BudgetYear _inc. VAT", "Demandbook Cash Out BudgetYear inc. VAT"}, {"Demandbook_Cash Out Total _inc. VAT", "Demandbook Cash Out Total inc. VAT"}, {"Budget _Available", "Budget Available"}, {"Actual _Alloted", "Actual Alloted"}, {"Commitment _P.Order", "Commitment P.Order"}, {"Commitment _P.Requisition", "Commitment P.Requisition"}, {"Commitment_Total", "Commitment Total"}, {"Actual _CO Order", "Actual CO Order"}, {"Plan ID _MM PO_", "Plan ID MM PO"}, {"WBS _Acc._", "WBS Acc."}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 2", {{"KeyDemandPO", type text}}),
  #"Renamed columns 3" = Table.RenameColumns(#"Changed column type 2", {{"Purchasing order number (PO)", "Purchasing order number PO"}}),
  #"Added custom 1" = Table.AddColumn(#"Renamed columns 3", "Budget Spent", each [Budget Approved] - [Budget Available]),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Added custom 1", {{"Budget Spent", Currency.Type}, {"Budget Available", Currency.Type}, {"Budget Approved", Currency.Type}}),
  #"Added custom 2" = Table.AddColumn(#"Changed column type 3", "Profit Center and Name", each [Profit Center] & " - " & [Profit Center Name]),
  #"Added custom 3" = Table.AddColumn(#"Added custom 2", "Vendor Number and Name", each [Vendor] & " - " & [Vendor Name]),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "WBS Number and Name", each [#"WBS Acc."] & " - " & [WBS Name]),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Added custom 4", {{"WBS Number and Name", type text}, {"Vendor Number and Name", type text}, {"Profit Center and Name", type text}})
in
  #"Changed column type 4";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_budget_demand_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_budget_demand = let
  Source = Table.Combine({fact_budget_demand_archive, fact_budget_demand_current})
in
  Source;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_budget_demand_per_demand_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_budget_demand_per_demand = let
  Source = fact_budget_demand,
   #"Filtered Rows31" = Table.SelectRows(Source, each Text.StartsWith([Demand ID], "GBUY") or Text.StartsWith([Demand ID], "RITM")),
    #"Grouped Rows" = Table.Group(#"Filtered Rows31", {"Company Code", "KeyDemandPO", "Demand ID", "Purchasing order number PO", "Profit Center", "Profit Center Name", "Profit Center and Name", "Vendor Number and Name"}, {{"PO Approved", each List.Max([#"Net purchase order value PO+PR new"]), type nullable number}, {"PO Spent", each List.Sum([Actual CO Order]), type nullable number}}),
    #"Merged Queries" = Table.NestedJoin(#"Grouped Rows", {"Demand ID"}, fact_budget_demand_per_vendors, {"Demand ID"}, "fact_budget_demand_per_vendors", JoinKind.LeftOuter),
  #"Expanded fact_budget_demand_per_vendors 1" = Table.ExpandTableColumn(#"Merged Queries", "fact_budget_demand_per_vendors", {"Vendor", "Vendor Name"}, {"Vendor", "Vendor Name"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Expanded fact_budget_demand_per_vendors 1",null,"#",Replacer.ReplaceValue,{"Vendor"}),
    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1",null,"Not assigned",Replacer.ReplaceValue,{"Vendor Name"}),
    #"Added Custom 1" = Table.AddColumn(#"Replaced Value2", "PO Approved inc. Tax", each [PO Approved] * 1.076),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom 1",{{"PO Approved inc. Tax", Currency.Type}, {"PO Spent", Currency.Type}, {"PO Approved", Currency.Type}}),
    #"Removed Duplicates" = Table.Distinct(#"Changed Type3", {"KeyDemandPO"}),
  #"Added custom" = Table.AddColumn(#"Removed Duplicates", "PO Remainging", each [PO Approved inc. Tax] - [PO Spent]),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"PO Remainging", Currency.Type}})
in
  #"Changed column type";
shared fact_budget_demand_per_vendors = let
  Source = fact_budget_demand,
   #"Filtered Rows3" = Table.SelectRows(Source, each Text.StartsWith([Demand ID], "GBUY") or Text.StartsWith([Demand ID], "RITM")),
    #"Filtered Rows5" = Table.SelectRows(#"Filtered Rows3", each ([Vendor] <> "#")),
    #"Removed Duplicates" = Table.Distinct(#"Filtered Rows5", {"Demand ID"}),
    #"Filtered Rows41" = Table.SelectRows(#"Removed Duplicates", each ([Profit Center] <> "#"))
in
  #"Filtered Rows41";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_budget_demand_per_planbook_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_budget_demand_per_planbook = let
  Source = fact_budget_demand,
   #"Added Custom2" = Table.AddColumn(Source, "Plan ID Consolidated", each if Text.StartsWith([Demand ID],"GBUY") or Text.StartsWith([Demand ID],"RITM") then [#"Plan ID MM PO"] else [Demand ID]),
    #"Added Custom3" = Table.AddColumn(#"Added Custom2", "Plan ID", each if Text.Contains([Plan ID Consolidated],"_") then
Text.AfterDelimiter([Plan ID Consolidated], "_") else [Plan ID Consolidated]),
    #"Filtered Rows31" = Table.SelectRows(#"Added Custom3", each ([Plan ID] <> "#")),
    #"Removed Columns1" = Table.RemoveColumns(#"Filtered Rows31",{"Budget Approved"}),
    #"Renamed Columns2" = Table.RenameColumns(#"Removed Columns1",{{"Demandbook Cash Out BudgetYear inc. VAT", "Budget Approved"}}),
    #"Added Custom4" = Table.AddColumn(#"Renamed Columns2", "Budget Spent Corrected", each if [Budget Spent] > 0 then [Budget Spent] * 1 else [Budget Spent] * -1),
    #"Removed Columns3" = Table.RemoveColumns(#"Added Custom4",{"Budget Spent"}),
    #"Renamed Columns3" = Table.RenameColumns(#"Removed Columns3",{{"Budget Spent Corrected", "Budget Spent"}}),
    #"Changed Type3" = Table.TransformColumnTypes(#"Renamed Columns3",{{"Budget Spent", type number}}),
  #"Removed other columns" = Table.SelectColumns(#"Changed Type3", {"Company Code", "Pillar", "Profit Center", "Profit Center Name", "Order Type", "Order Type Name", "Order", "Demand ID", "Purchasing order number PO", "Plan ID MM PO", "CO Doc Line Item Txt", "Vendor", "Vendor Name", "WBS Acc.", "WBS Name", "Fiscal year", "Budget Available", "Budget Approved", "Refresh Date", "Profit Center and Name", "Plan ID", "Budget Spent"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed other columns", {{"Plan ID", type text}, {"Profit Center and Name", type text}})
in
  #"Changed column type";
shared fact_budget_demand_per_demand_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_budget_demand_per_demand", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_budget_demand_per_planbook_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_budget_demand_per_planbook", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared fact_budget_demand_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/GoldLakehouseID)",  "6c07b77b-2af9-48eb-95e5-87b904a26384")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_budget_demand", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
